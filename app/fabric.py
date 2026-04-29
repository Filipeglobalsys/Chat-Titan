import asyncio
import httpx
from typing import Any

_FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_TIMEOUT = 15.0

# Item types that matter for data engineering analysis
_DE_ITEM_TYPES = {
    "Lakehouse", "Warehouse", "DataPipeline", "Notebook",
    "SparkJobDefinition", "Dataflow", "DataflowGen2",
    "MLExperiment", "MLModel", "SemanticModel", "Report",
}


async def get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    url = f"https://login.microsoftonline.com/{tenant_id.strip()}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id.strip(),
        "client_secret": client_secret.strip(),
        "scope": _FABRIC_SCOPE,
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(url, data=data, timeout=_TIMEOUT)
        if r.status_code == 400:
            detail = r.json().get("error_description", r.text)
            raise ValueError(f"Credenciais inválidas: {detail}")
        if r.status_code == 401:
            raise ValueError("Tenant ID, Client ID ou Client Secret incorretos.")
        r.raise_for_status()
        return r.json()["access_token"]


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _get(client: httpx.AsyncClient, url: str, token: str, params: dict | None = None) -> dict:
    r = await client.get(url, headers=_headers(token), params=params, timeout=_TIMEOUT)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()


async def get_workspaces(token: str) -> list[dict]:
    async with httpx.AsyncClient() as client:
        data = await _get(client, f"{_FABRIC_BASE}/workspaces", token)
    result = []
    for w in data.get("value", []):
        result.append({
            "id": w.get("id"),
            "name": w.get("displayName"),
            "type": w.get("type"),
            "capacity_id": w.get("capacityId"),
            "capacity_assignment_progress": w.get("capacityAssignmentProgress"),
        })
    return result


async def get_workspace_items(token: str, workspace_id: str) -> list[dict]:
    async with httpx.AsyncClient() as client:
        data = await _get(client, f"{_FABRIC_BASE}/workspaces/{workspace_id}/items", token)
    result = []
    for item in data.get("value", []):
        result.append({
            "id": item.get("id"),
            "name": item.get("displayName"),
            "type": item.get("type"),
            "description": item.get("description", ""),
        })
    return result


async def get_lakehouses(token: str, workspace_id: str) -> list[dict]:
    async with httpx.AsyncClient() as client:
        data = await _get(client, f"{_FABRIC_BASE}/workspaces/{workspace_id}/lakehouses", token)
    result = []
    for lh in data.get("value", []):
        props = lh.get("properties", {})
        result.append({
            "id": lh.get("id"),
            "name": lh.get("displayName"),
            "workspace_id": workspace_id,
            "sql_endpoint": props.get("sqlEndpointProperties", {}).get("connectionString"),
            "default_schema": props.get("defaultSchema"),
        })
    return result


async def get_lakehouse_tables(token: str, workspace_id: str, lakehouse_id: str) -> list[dict]:
    async with httpx.AsyncClient() as client:
        try:
            data = await _get(
                client,
                f"{_FABRIC_BASE}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
                token,
            )
        except Exception:
            return []
    result = []
    for t in data.get("data", []):
        result.append({
            "name": t.get("name"),
            "type": t.get("type"),
            "format": t.get("format"),
            "location": t.get("location"),
        })
    return result


async def get_warehouses(token: str, workspace_id: str) -> list[dict]:
    async with httpx.AsyncClient() as client:
        data = await _get(client, f"{_FABRIC_BASE}/workspaces/{workspace_id}/warehouses", token)
    result = []
    for w in data.get("value", []):
        result.append({
            "id": w.get("id"),
            "name": w.get("displayName"),
            "workspace_id": workspace_id,
            "connection_string": w.get("properties", {}).get("connectionString"),
        })
    return result


async def get_pipelines(token: str, workspace_id: str) -> list[dict]:
    async with httpx.AsyncClient() as client:
        data = await _get(client, f"{_FABRIC_BASE}/workspaces/{workspace_id}/dataPipelines", token)
    result = []
    for p in data.get("value", []):
        result.append({
            "id": p.get("id"),
            "name": p.get("displayName"),
            "workspace_id": workspace_id,
        })
    return result


async def get_notebooks(token: str, workspace_id: str) -> list[dict]:
    async with httpx.AsyncClient() as client:
        data = await _get(client, f"{_FABRIC_BASE}/workspaces/{workspace_id}/notebooks", token)
    return [{"id": n.get("id"), "name": n.get("displayName")} for n in data.get("value", [])]


async def collect_environment(tenant_id: str, client_id: str, client_secret: str) -> dict[str, Any]:
    """Collect full Fabric environment data."""
    token = await get_token(tenant_id, client_id, client_secret)

    workspaces = await get_workspaces(token)

    # Skip personal workspaces and system workspaces; cap at 10 user workspaces
    user_workspaces = [
        w for w in workspaces
        if w.get("type") not in ("PersonalGroup",) and w.get("name") not in ("My workspace",)
    ][:10]

    # Collect items, lakehouses, warehouses, pipelines, notebooks in parallel per workspace
    async def enrich_workspace(ws: dict) -> dict:
        wid = ws["id"]
        items, lakehouses, warehouses, pipelines, notebooks = await asyncio.gather(
            get_workspace_items(token, wid),
            get_lakehouses(token, wid),
            get_warehouses(token, wid),
            get_pipelines(token, wid),
            get_notebooks(token, wid),
            return_exceptions=True,
        )

        items = items if not isinstance(items, Exception) else []
        lakehouses = lakehouses if not isinstance(lakehouses, Exception) else []
        warehouses = warehouses if not isinstance(warehouses, Exception) else []
        pipelines = pipelines if not isinstance(pipelines, Exception) else []
        notebooks = notebooks if not isinstance(notebooks, Exception) else []

        # Count items by type
        item_counts: dict[str, int] = {}
        for item in items:
            t = item.get("type", "Unknown")
            item_counts[t] = item_counts.get(t, 0) + 1

        # Fetch tables for first 3 lakehouses
        lakehouses_with_tables = []
        for lh in lakehouses[:3]:
            tables = await get_lakehouse_tables(token, wid, lh["id"])
            lakehouses_with_tables.append({**lh, "tables": tables, "table_count": len(tables)})
        for lh in lakehouses[3:]:
            lakehouses_with_tables.append({**lh, "tables": [], "table_count": None})

        return {
            **ws,
            "item_counts": item_counts,
            "lakehouses": lakehouses_with_tables,
            "warehouses": warehouses if not isinstance(warehouses, Exception) else [],
            "pipelines": pipelines if not isinstance(pipelines, Exception) else [],
            "notebooks": notebooks if not isinstance(notebooks, Exception) else [],
        }

    enriched = await asyncio.gather(*[enrich_workspace(ws) for ws in user_workspaces], return_exceptions=True)
    enriched_workspaces = [
        ws if not isinstance(ws, Exception) else user_workspaces[i]
        for i, ws in enumerate(enriched)
    ]

    return {
        "tenant_id": tenant_id,
        "total_workspaces": len(workspaces),
        "workspaces": enriched_workspaces,
    }
