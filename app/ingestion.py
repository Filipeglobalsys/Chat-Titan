"""
Extract → Parquet → OneLake ingestion pipeline.

Flow:
  1. Connect to source DB (SQL Server / PostgreSQL / MySQL) via SQLAlchemy
  2. Run user-supplied SELECT query → pandas DataFrame
  3. Serialize to Parquet (snappy, in-memory)
  4. Upload to OneLake Files section via ADLS Gen2 REST API
  5. Register as Delta table via Fabric Load Table API
  6. Poll operation until Fabric confirms the table is ready
"""
import asyncio
import io
import time
import httpx

_ONELAKE_BASE = "https://onelake.dfs.fabric.microsoft.com"
_FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
_TIMEOUT = 30.0
_UPLOAD_TIMEOUT = 300.0
_CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB


# ── Authentication ────────────────────────────────────────────────────────────

async def _get_token(tenant_id: str, client_id: str, client_secret: str, scope: str) -> str:
    url = f"https://login.microsoftonline.com/{tenant_id.strip()}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id.strip(),
        "client_secret": client_secret.strip(),
        "scope": scope,
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(url, data=data, timeout=_TIMEOUT)
        if r.status_code in (400, 401):
            msg = r.json().get("error_description", r.text)[:300]
            raise ValueError(f"Autenticação falhou: {msg}")
        r.raise_for_status()
        return r.json()["access_token"]


async def get_onelake_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    return await _get_token(tenant_id, client_id, client_secret, "https://storage.azure.com/.default")


async def get_fabric_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    return await _get_token(tenant_id, client_id, client_secret, "https://api.fabric.microsoft.com/.default")


# ── Extract ───────────────────────────────────────────────────────────────────

def _extract_sync(connection_string: str, sql: str) -> tuple[bytes, int, list[str]]:
    """Synchronous: run SQL and return (parquet_bytes, row_count, columns)."""
    import pandas as pd
    from sqlalchemy import create_engine

    engine = create_engine(connection_string, connect_args={"timeout": 60})
    try:
        df = pd.read_sql(sql, engine)
    finally:
        engine.dispose()

    # Coerce object columns with mixed types to string to avoid Arrow schema errors
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].where(df[col].isna(), df[col].astype(str))

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    return buf.getvalue(), len(df), list(df.columns)


async def extract_data(connection_string: str, sql: str) -> tuple[bytes, int, list[str]]:
    """Async wrapper — runs SQLAlchemy in a thread pool."""
    return await asyncio.to_thread(_extract_sync, connection_string, sql)


# ── OneLake Upload (ADLS Gen2) ────────────────────────────────────────────────

async def upload_to_onelake(
    token: str,
    workspace_id: str,
    lakehouse_id: str,
    file_path: str,
    data: bytes,
) -> None:
    """
    Upload bytes to OneLake using the ADLS Gen2 REST protocol.
    file_path: relative path inside the lakehouse, e.g. "Files/ingestion/sales/data.parquet"
    """
    base = f"{_ONELAKE_BASE}/{workspace_id}/{lakehouse_id}"
    auth = {"Authorization": f"Bearer {token}"}
    size = len(data)

    async with httpx.AsyncClient() as client:
        # 1. Create (or overwrite) the file
        r = await client.put(
            f"{base}/{file_path}",
            params={"resource": "file", "overwrite": "true"},
            headers=auth,
            timeout=_TIMEOUT,
        )
        r.raise_for_status()

        # 2. Append data in chunks
        position = 0
        while position < size:
            chunk = data[position: position + _CHUNK_SIZE]
            r = await client.patch(
                f"{base}/{file_path}",
                params={"action": "append", "position": position},
                headers={**auth, "Content-Length": str(len(chunk))},
                content=chunk,
                timeout=_UPLOAD_TIMEOUT,
            )
            r.raise_for_status()
            position += len(chunk)

        # 3. Flush (commit)
        r = await client.patch(
            f"{base}/{file_path}",
            params={"action": "flush", "position": size},
            headers=auth,
            timeout=_TIMEOUT,
        )
        r.raise_for_status()


# ── Fabric Load Table ─────────────────────────────────────────────────────────

async def load_table(
    fabric_token: str,
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    relative_path: str,
) -> str:
    """
    Call Fabric Load Table API to register the Parquet file as a Delta table.
    Returns the operation_id (empty string if operation completed synchronously).
    relative_path: path relative to the lakehouse root, e.g. "Files/ingestion/sales/data.parquet"
    """
    url = (
        f"{_FABRIC_BASE}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
        f"/tables/{table_name}/load"
    )
    body = {
        "relativePath": relative_path,
        "pathType": "File",
        "mode": "Overwrite",
        "formatOptions": {"format": "Parquet", "header": False},
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(
            url,
            json=body,
            headers={
                "Authorization": f"Bearer {fabric_token}",
                "Content-Type": "application/json",
            },
            timeout=_TIMEOUT,
        )
        if r.status_code not in (200, 202):
            raise ValueError(
                f"Fabric Load Table API retornou {r.status_code}: {r.text[:400]}"
            )
        if r.status_code == 202:
            op_id = (
                r.headers.get("x-ms-operation-id")
                or r.headers.get("Location", "").rstrip("/").split("/")[-1]
            )
            return op_id or ""
        return ""


async def poll_operation(
    fabric_token: str,
    operation_id: str,
    timeout_seconds: int = 180,
) -> str:
    """
    Poll a Fabric long-running operation until it reaches a terminal state.
    Returns: "Succeeded" | "Failed" | "Cancelled" | "Timeout"
    """
    if not operation_id:
        return "Succeeded"

    url = f"{_FABRIC_BASE}/operations/{operation_id}"
    headers = {"Authorization": f"Bearer {fabric_token}"}
    deadline = time.monotonic() + timeout_seconds

    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            r = await client.get(url, headers=headers, timeout=_TIMEOUT)
            if r.status_code == 404:
                return "Succeeded"
            r.raise_for_status()
            status = r.json().get("status", "Running")
            if status in ("Succeeded", "Failed", "Cancelled"):
                return status
            await asyncio.sleep(4)

    return "Timeout"
