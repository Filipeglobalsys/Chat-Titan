import asyncio
import os
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client

_client: Client | None = None
_SEMAPHORE = asyncio.Semaphore(5)
_INCREMENTAL_HOURS = 6


def get_db() -> Client:
    global _client
    if _client is None:
        _client = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
        )
    return _client


async def _get_schema_via_dmv(dataset_id: str) -> tuple[list, str | None]:
    """Returns (tables, error_message). Tables is empty list on failure."""
    from powerbi import execute_query

    tables: dict[str, dict] = {}

    cols_dax = (
        "EVALUATE FILTER("
        "  SELECTCOLUMNS(INFO.VIEW.COLUMNS(),"
        "    \"T\", [Table], \"C\", [Name], \"D\", [DataType], \"CT\", [Type]),"
        "  NOT ISBLANK([T]) && NOT CONTAINSSTRING([C], \"RowNumber-\")"
        ")"
    )
    meas_dax = (
        "EVALUATE SELECTCOLUMNS(INFO.VIEW.MEASURES(),"
        "  \"T\", [Table], \"M\", [Name], \"E\", [Expression], \"Desc\", [Description])"
    )

    async with _SEMAPHORE:
        try:
            cols_rows = await execute_query(dataset_id, cols_dax)
        except Exception as e:
            return [], f"{type(e).__name__}: {e}"

        try:
            meas_rows = await execute_query(dataset_id, meas_dax)
        except Exception:
            meas_rows = []

    for r in cols_rows:
        tname = r.get("[T]", "")
        cname = r.get("[C]", "")
        if tname and cname:
            if tname not in tables:
                tables[tname] = {"columns": [], "measures": []}
            tables[tname]["columns"].append({
                "name": cname,
                "dataType": r.get("[D]", ""),
                "columnType": r.get("[CT]", ""),
            })

    for r in meas_rows:
        tname = r.get("[T]", "")
        mname = r.get("[M]", "")
        if tname and mname:
            if tname not in tables:
                tables[tname] = {"columns": [], "measures": []}
            tables[tname]["measures"].append({
                "name": mname,
                "expression": r.get("[E]", ""),
                "description": r.get("[Desc]", ""),
            })

    result = [{"name": tname, **data} for tname, data in tables.items()]
    return result, None


def _save_tables(db: Client, dataset_id: str, tables: list):
    for table in tables:
        res = (
            db.table("tables")
            .upsert({"dataset_id": dataset_id, "name": table["name"]}, on_conflict="dataset_id,name")
            .execute()
        )
        table_id = res.data[0]["id"] if res.data else None
        if not table_id:
            row = (
                db.table("tables")
                .select("id")
                .eq("dataset_id", dataset_id)
                .eq("name", table["name"])
                .single()
                .execute()
            )
            table_id = row.data["id"]

        if table.get("columns"):
            db.table("columns").upsert(
                [{"table_id": table_id, "name": c["name"], "data_type": c.get("dataType"), "column_type": c.get("columnType")} for c in table["columns"]],
                on_conflict="table_id,name",
            ).execute()

        if table.get("measures"):
            db.table("measures").upsert(
                [{"table_id": table_id, "name": m["name"], "expression": m.get("expression"), "description": m.get("description")} for m in table["measures"]],
                on_conflict="table_id,name",
            ).execute()


async def _sync_dataset(db: Client, ws_id: str, ds: dict, skip_ids: set) -> dict:
    ds_id = ds["id"]
    ds_name = ds.get("name", ds_id)

    if ds_name in ("Report Usage Metrics Model",):
        db.table("datasets").upsert(
            {"id": ds_id, "workspace_id": ws_id, "name": ds_name,
             "configured_by": ds.get("configuredBy"), "is_refreshable": False},
            on_conflict="id",
        ).execute()
        return {"name": ds_name, "status": "skipped"}

    if ds_id in skip_ids:
        return {"name": ds_name, "status": "skipped (recent)"}

    db.table("datasets").upsert(
        {
            "id": ds_id,
            "workspace_id": ws_id,
            "name": ds_name,
            "configured_by": ds.get("configuredBy"),
            "is_refreshable": ds.get("isRefreshable", False),
        },
        on_conflict="id",
    ).execute()

    tables, error = await _get_schema_via_dmv(ds_id)

    if tables:
        _save_tables(db, ds_id, tables)
        db.table("datasets").update(
            {"synced_at": datetime.now(timezone.utc).isoformat()}
        ).eq("id", ds_id).execute()
        print(f"[SYNC OK] {ds_name}: {len(tables)} tabelas")
        return {"name": ds_name, "status": "ok", "tables": len(tables)}
    else:
        print(f"[SYNC FAIL] {ds_name}: {error}")
        return {"name": ds_name, "status": "error", "error": error}


async def sync_metadata(force: bool = False) -> dict:
    from powerbi import get_workspaces, get_datasets

    db = get_db()
    workspaces = await get_workspaces()

    for ws in workspaces:
        db.table("workspaces").upsert(
            {"id": ws["id"], "name": ws["name"], "type": ws.get("type"), "is_read_only": ws.get("isReadOnly", False)},
            on_conflict="id",
        ).execute()

    skip_ids: set = set()
    if not force:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=_INCREMENTAL_HOURS)).isoformat()
        recent = db.table("datasets").select("id").gt("synced_at", cutoff).execute()
        skip_ids = {row["id"] for row in (recent.data or [])}

    all_datasets_lists = await asyncio.gather(*[get_datasets(ws["id"]) for ws in workspaces])

    tasks = [
        _sync_dataset(db, ws["id"], ds, skip_ids)
        for ws, datasets in zip(workspaces, all_datasets_lists)
        for ds in datasets
    ]
    results = await asyncio.gather(*tasks)

    ok = [r for r in results if r.get("status") == "ok"]
    errors = [r for r in results if r.get("status") == "error"]
    skipped = [r for r in results if "skipped" in r.get("status", "")]

    return {
        "workspaces": len(workspaces),
        "synced": len(ok),
        "failed": len(errors),
        "skipped": len(skipped),
        "errors": errors,
    }


def get_dataset_schema(dataset_id: str) -> list:
    db = get_db()
    res = (
        db.table("tables")
        .select("id, name, columns(name, data_type, column_type), measures(name, expression, description)")
        .eq("dataset_id", dataset_id)
        .execute()
    )
    return res.data or []
