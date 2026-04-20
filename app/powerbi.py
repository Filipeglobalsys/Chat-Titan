import httpx
from auth import get_access_token

BASE = "https://api.powerbi.com/v1.0/myorg"


async def pbi_get(path: str) -> dict:
    token = await get_access_token()
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{BASE}{path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


async def pbi_post(path: str, body: dict) -> dict:
    token = await get_access_token()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{BASE}{path}",
            json=body,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


async def get_workspaces() -> list:
    data = await pbi_get("/groups")
    return data.get("value", [])


async def get_datasets(workspace_id: str) -> list:
    data = await pbi_get(f"/groups/{workspace_id}/datasets")
    return data.get("value", [])


async def get_tables(dataset_id: str) -> list:
    data = await pbi_get(f"/datasets/{dataset_id}/tables")
    return data.get("value", [])


async def get_reports(workspace_id: str) -> list:
    data = await pbi_get(f"/groups/{workspace_id}/reports")
    return data.get("value", [])


async def generate_embed_token(workspace_id: str, report_id: str) -> dict:
    return await pbi_post(
        f"/groups/{workspace_id}/reports/{report_id}/GenerateToken",
        {"accessLevel": "View"},
    )


async def execute_query(dataset_id: str, dax: str) -> list:
    result = await pbi_post(
        f"/datasets/{dataset_id}/executeQueries",
        {
            "queries": [{"query": dax}],
            "serializerSettings": {"includeNulls": True},
        },
    )
    return result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
