"""
CSV → cloud upload pipeline.

Fabric:  CSV → Parquet (pyarrow/snappy) → OneLake ADLS Gen2 → Delta table via Load Table API
Databricks: CSV raw bytes → DBFS chunked upload (create/add-block/close)
"""
import asyncio
import base64
import io
import httpx

_TIMEOUT = 30.0
_DBFS_BLOCK = 750 * 1024   # 750 KB raw → ~1 MB base64 (DBFS block limit)


def _csv_to_parquet_sync(file_bytes: bytes) -> tuple[bytes, int, list[str]]:
    import pandas as pd
    df = pd.read_csv(io.BytesIO(file_bytes))
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].where(df[col].isna(), df[col].astype(str))
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    return buf.getvalue(), len(df), list(df.columns)


async def csv_to_parquet(file_bytes: bytes) -> tuple[bytes, int, list[str]]:
    """Async wrapper: parse CSV and return (parquet_bytes, row_count, columns)."""
    return await asyncio.to_thread(_csv_to_parquet_sync, file_bytes)


async def upload_csv_to_databricks(
    host: str, token: str, data: bytes, table_name: str
) -> str:
    """
    Upload raw CSV bytes to Databricks DBFS using the chunked API.
    Returns the dbfs:/ path for use in notebooks.
    """
    from urllib.parse import urlparse as _urlparse
    _p = _urlparse(host.strip() if host.strip().startswith("http") else f"https://{host.strip()}")
    base_url = f"{_p.scheme}://{_p.netloc}"
    auth = {"Authorization": f"Bearer {token}"}
    safe = "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)
    dbfs_path = f"/FileStore/tables/uploads/{safe}.csv"

    async with httpx.AsyncClient() as client:
        # 1. Create (get handle)
        r = await client.post(
            f"{base_url}/api/2.0/dbfs/create",
            json={"path": dbfs_path, "overwrite": True},
            headers=auth,
            timeout=_TIMEOUT,
        )
        if r.status_code == 403:
            raise ValueError("Token sem permissão para escrita no DBFS. Verifique as permissões do PAT.")
        r.raise_for_status()
        handle = r.json()["handle"]

        # 2. Add blocks
        offset = 0
        while offset < len(data):
            chunk = data[offset : offset + _DBFS_BLOCK]
            b64 = base64.b64encode(chunk).decode("ascii")
            r = await client.post(
                f"{base_url}/api/2.0/dbfs/add-block",
                json={"handle": handle, "data": b64},
                headers=auth,
                timeout=_TIMEOUT,
            )
            r.raise_for_status()
            offset += len(chunk)

        # 3. Close
        r = await client.post(
            f"{base_url}/api/2.0/dbfs/close",
            json={"handle": handle},
            headers=auth,
            timeout=_TIMEOUT,
        )
        r.raise_for_status()

    return f"dbfs:{dbfs_path}"
