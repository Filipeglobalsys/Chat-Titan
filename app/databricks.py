import asyncio
import httpx
from typing import Any

_TIMEOUT = 15.0


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _clean_host(host: str) -> str:
    from urllib.parse import urlparse
    host = host.strip().rstrip("/")
    if not host.startswith("http"):
        host = f"https://{host}"
    parsed = urlparse(host)
    return f"{parsed.scheme}://{parsed.netloc}"


async def test_connection(host: str, token: str) -> None:
    url = f"{_clean_host(host)}/api/2.0/clusters/list"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=_headers(token), timeout=_TIMEOUT)
        if r.status_code == 401:
            raise ValueError("Token inválido ou expirado. Verifique o Personal Access Token.")
        if r.status_code == 403:
            raise ValueError("Acesso negado. O token não tem permissão suficiente.")
        if r.status_code == 404:
            raise ValueError("Host não encontrado. Verifique a URL do workspace.")
        r.raise_for_status()


async def _get(client: httpx.AsyncClient, url: str, token: str, params: dict | None = None) -> dict:
    r = await client.get(url, headers=_headers(token), params=params, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


async def get_clusters(host: str, token: str) -> list[dict]:
    base = _clean_host(host)
    async with httpx.AsyncClient() as client:
        data = await _get(client, f"{base}/api/2.0/clusters/list", token)
    result = []
    for c in data.get("clusters", []):
        autoscale = c.get("autoscale")
        result.append({
            "id": c.get("cluster_id"),
            "name": c.get("cluster_name"),
            "state": c.get("state"),
            "runtime": c.get("spark_version", "").split("-")[0],
            "node_type": c.get("node_type_id"),
            "num_workers": autoscale.get("max_workers") if autoscale else c.get("num_workers"),
            "autoscale": autoscale is not None,
            "creator": c.get("creator_user_name"),
            "cluster_source": c.get("cluster_source"),
        })
    return result


async def get_jobs(host: str, token: str, limit: int = 25) -> list[dict]:
    base = _clean_host(host)
    async with httpx.AsyncClient() as client:
        data = await _get(client, f"{base}/api/2.1/jobs/list", token, {"limit": limit, "expand_tasks": "false"})
    result = []
    for j in data.get("jobs", []):
        settings = j.get("settings", {})
        schedule = settings.get("schedule", {})
        result.append({
            "id": j.get("job_id"),
            "name": settings.get("name", ""),
            "creator": j.get("creator_user_name", ""),
            "schedule": schedule.get("quartz_cron_expression"),
            "pause_status": schedule.get("pause_status"),
        })
    return result


async def get_job_runs(host: str, token: str, job_id: int, limit: int = 10) -> list[dict]:
    base = _clean_host(host)
    async with httpx.AsyncClient() as client:
        try:
            data = await _get(client, f"{base}/api/2.1/jobs/runs/list", token, {"job_id": job_id, "limit": limit})
        except Exception:
            return []
    result = []
    for run in data.get("runs", []):
        state = run.get("state", {})
        start = run.get("start_time", 0)
        end = run.get("end_time", 0)
        result.append({
            "run_id": run.get("run_id"),
            "state": state.get("result_state") or state.get("life_cycle_state"),
            "duration_ms": (end - start) if end and start else None,
        })
    return result


async def get_catalogs(host: str, token: str) -> list[dict]:
    base = _clean_host(host)
    async with httpx.AsyncClient() as client:
        try:
            data = await _get(client, f"{base}/api/2.1/unity-catalog/catalogs", token)
        except (httpx.HTTPStatusError, Exception):
            return []
    return [
        {"name": c.get("name"), "type": c.get("catalog_type"), "comment": c.get("comment", "")}
        for c in data.get("catalogs", [])
    ]


async def get_schemas(host: str, token: str, catalog: str) -> list[dict]:
    base = _clean_host(host)
    async with httpx.AsyncClient() as client:
        try:
            data = await _get(client, f"{base}/api/2.1/unity-catalog/schemas", token, {"catalog_name": catalog})
        except Exception:
            return []
    return [
        {"name": s.get("name"), "catalog": catalog, "comment": s.get("comment", "")}
        for s in data.get("schemas", [])
    ]


async def get_tables(host: str, token: str, catalog: str, schema: str, max_results: int = 25) -> list[dict]:
    base = _clean_host(host)
    async with httpx.AsyncClient() as client:
        try:
            data = await _get(
                client, f"{base}/api/2.1/unity-catalog/tables", token,
                {"catalog_name": catalog, "schema_name": schema, "max_results": max_results},
            )
        except Exception:
            return []
    result = []
    for t in data.get("tables", []):
        cols = t.get("columns", [])
        result.append({
            "name": t.get("name"),
            "full_name": t.get("full_name"),
            "table_type": t.get("table_type"),
            "data_source_format": t.get("data_source_format"),
            "owner": t.get("owner"),
            "comment": t.get("comment", ""),
            "column_count": len(cols),
            "columns": [
                {"name": c.get("name"), "type": c.get("type_text"), "nullable": c.get("nullable")}
                for c in cols[:8]
            ],
        })
    return result


async def collect_environment(host: str, token: str) -> dict[str, Any]:
    """Collect all relevant data from Databricks workspace concurrently."""
    clusters, jobs, catalogs = await asyncio.gather(
        get_clusters(host, token),
        get_jobs(host, token),
        get_catalogs(host, token),
        return_exceptions=True,
    )

    clusters = clusters if not isinstance(clusters, Exception) else []
    jobs = jobs if not isinstance(jobs, Exception) else []
    catalogs = catalogs if not isinstance(catalogs, Exception) else []

    # Enrich first 5 jobs with run history concurrently
    async def enrich_job(job: dict) -> dict:
        runs = await get_job_runs(host, token, job["id"])
        return {**job, "recent_runs": runs}

    enriched_jobs = await asyncio.gather(*[enrich_job(j) for j in jobs[:5]], return_exceptions=True)
    jobs_with_runs = [j if not isinstance(j, Exception) else jobs[i] for i, j in enumerate(enriched_jobs)]
    jobs_with_runs += [{**j, "recent_runs": []} for j in jobs[5:]]

    # Unity Catalog: schemas for user catalogs (skip system)
    user_catalogs = [c for c in catalogs if c["name"] not in ("system", "__databricks_internal")][:4]
    schemas_by_catalog: dict[str, list] = {}
    for cat in user_catalogs:
        schemas = await get_schemas(host, token, cat["name"])
        schemas_by_catalog[cat["name"]] = schemas

    # Sample tables from first 2 schemas of each catalog
    tables_sample: list[dict] = []
    for cat_name, schemas in schemas_by_catalog.items():
        user_schemas = [s for s in schemas if s["name"] != "information_schema"][:2]
        for schema in user_schemas:
            tables = await get_tables(host, token, cat_name, schema["name"], max_results=20)
            tables_sample.extend(tables)

    return {
        "workspace_host": _clean_host(host),
        "clusters": clusters,
        "jobs": jobs_with_runs,
        "catalogs": catalogs,
        "schemas_by_catalog": schemas_by_catalog,
        "tables_sample": tables_sample[:60],
    }
