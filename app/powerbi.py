import re
import httpx
from auth import get_access_token, invalidate_token

BASE = "https://api.powerbi.com/v1.0/myorg"


async def pbi_get(path: str) -> dict:
    token = await get_access_token()
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{BASE}{path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if resp.status_code == 401:
            # Force token refresh and retry once
            await invalidate_token()
            token = await get_access_token()
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
        if resp.status_code == 401:
            print(f"[PBI 401] POST {path} — {resp.text}")
            await invalidate_token()
            token = await get_access_token()
            resp = await client.post(
                f"{BASE}{path}",
                json=body,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
        if not resp.is_success:
            print(f"[PBI ERR] POST {path} {resp.status_code} — {resp.text}")
            raise Exception(f"Power BI {resp.status_code}: {resp.text}")
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


async def generate_embed_token(
    workspace_id: str,
    report_id: str,
    dataset_id: str | None = None,
    username: str | None = None,
    roles: list | None = None,
) -> dict:
    path = f"/groups/{workspace_id}/reports/{report_id}/GenerateToken"
    body: dict = {"accessLevel": "View"}

    if username and dataset_id:
        identity: dict = {"username": username, "datasets": [dataset_id]}
        if roles:
            identity["roles"] = roles
        body["identities"] = [identity]

    try:
        return await pbi_post(path, body)
    except Exception as e:
        err_str = str(e)
        # Power BI returns 400 when effectiveIdentity is required for a specific dataset.
        # Extract the required dataset ID from the error and retry with it.
        if "effective identity" in err_str.lower():
            match = re.search(r'dataset\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', err_str, re.IGNORECASE)
            if match and username:
                required_ds = match.group(1)
                print(f"[EMBED] effectiveIdentity required for dataset {required_ds} — retrying")
                identity = {"username": username, "datasets": [required_ds]}
                body["identities"] = [identity]
                return await pbi_post(path, body)
            elif match and not username:
                raise Exception(
                    f"effectiveIdentity required for dataset {match.group(1)}. "
                    "Configure o e-mail do usuário Power BI nas configurações do dataset."
                )
        raise


async def execute_query(
    dataset_id: str,
    dax: str,
    workspace_id: str | None = None,
    effective_username: str | None = None,
    effective_role: str | None = None,
) -> list:
    path = (
        f"/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        if workspace_id
        else f"/datasets/{dataset_id}/executeQueries"
    )
    body: dict = {
        "queries": [{"query": dax}],
        "serializerSettings": {"includeNulls": True},
    }
    if effective_username:
        identity: dict = {"username": effective_username, "datasets": [dataset_id]}
        if effective_role:
            identity["roles"] = [effective_role]
        body["effectiveIdentities"] = [identity]
    result = await pbi_post(path, body)
    return result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
