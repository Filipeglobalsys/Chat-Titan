import asyncio
import os
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client

_client: Client | None = None
_SEMAPHORE = asyncio.Semaphore(5)
_INCREMENTAL_HOURS = 6
_sync_flags: dict[str, str] = {}  # dataset_id -> "gateway" | "rls_required" | "ok" | "no_permission"


def get_db() -> Client:
    global _client
    if _client is None:
        _client = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
        )
    return _client


async def _get_schema_via_dmv(
    dataset_id: str,
    workspace_id: str,
    effective_username: str | None = None,
    effective_role: str | None = None,
) -> tuple[list, str | None]:
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
            cols_rows = await execute_query(dataset_id, cols_dax, workspace_id, effective_username, effective_role)
        except Exception as e:
            return [], f"{type(e).__name__}: {e}"

        try:
            meas_rows = await execute_query(dataset_id, meas_dax, workspace_id, effective_username, effective_role)
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


def _upsert_with_status(db: Client, ds_id: str, ws_id: str, ds_name: str, ds: dict, status: str) -> None:
    payload = {
        "id": ds_id, "workspace_id": ws_id, "name": ds_name,
        "configured_by": ds.get("configuredBy"), "is_refreshable": ds.get("isRefreshable", False),
    }
    try:
        payload["sync_status"] = status
        db.table("datasets").upsert(payload, on_conflict="id").execute()
    except Exception:
        payload.pop("sync_status", None)
        db.table("datasets").upsert(payload, on_conflict="id").execute()


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

    # Datasets com gateway on-premises: tenta DMV primeiro — muitos ainda suportam executeQueries
    if ds.get("isOnPremGatewayRequired"):
        db.table("datasets").upsert(
            {"id": ds_id, "workspace_id": ws_id, "name": ds_name,
             "configured_by": ds.get("configuredBy"), "is_refreshable": ds.get("isRefreshable", False)},
            on_conflict="id",
        ).execute()
        tables, error = await _get_schema_via_dmv(ds_id, ws_id)
        if tables:
            _save_tables(db, ds_id, tables)
            _sync_flags[ds_id] = "ok"
            _upsert_with_status(db, ds_id, ws_id, ds_name, ds, "ok")
            db.table("datasets").update({"synced_at": datetime.now(timezone.utc).isoformat()}).eq("id", ds_id).execute()
            print(f"[SYNC OK] {ds_name}: gateway mas executeQueries funcionou — {len(tables)} tabelas")
            return {"name": ds_name, "status": "ok", "tables": len(tables)}
        # executeQueries falhou — precisa de conexão direta
        _sync_flags[ds_id] = "gateway"
        _upsert_with_status(db, ds_id, ws_id, ds_name, ds, "gateway")
        print(f"[SYNC SKIP] {ds_name}: gateway on-premises — executeQueries não suportado ({error})")
        return {"name": ds_name, "status": "gateway"}

    # executeQueries exige effectiveIdentity com roles para datasets com RLS dinâmica
    if ds.get("isEffectiveIdentityRequired") and ds.get("isEffectiveIdentityRolesRequired"):
        _sync_flags[ds_id] = "rls_required"
        _upsert_with_status(db, ds_id, ws_id, ds_name, ds, "rls_required")
        print(f"[SYNC SKIP] {ds_name}: RLS dinâmica com roles obrigatórias — executeQueries não suportado sem effectiveIdentity")
        return {"name": ds_name, "status": "rls_required"}

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

    tables, error = await _get_schema_via_dmv(ds_id, ws_id)

    if tables:
        _save_tables(db, ds_id, tables)
        _sync_flags[ds_id] = "ok"
        db.table("datasets").update(
            {"synced_at": datetime.now(timezone.utc).isoformat()}
        ).eq("id", ds_id).execute()
        print(f"[SYNC OK] {ds_name}: {len(tables)} tabelas")
        return {"name": ds_name, "status": "ok", "tables": len(tables)}

    # 401 = sem permissão Build no dataset — marcar como sincronizado para não repetir
    if error and "401" in error:
        _sync_flags[ds_id] = "no_permission"
        db.table("datasets").update(
            {"synced_at": datetime.now(timezone.utc).isoformat()}
        ).eq("id", ds_id).execute()
        print(f"[SYNC SKIP] {ds_name}: sem permissão Build (401)")
        return {"name": ds_name, "status": "no_permission"}

    print(f"[SYNC FAIL] {ds_name}: {error}")
    return {"name": ds_name, "status": "error", "error": error}


async def sync_metadata(force: bool = False) -> dict:
    from powerbi import get_workspaces, get_datasets

    db = get_db()
    workspaces = await get_workspaces()

    for ws in workspaces:
        cap = ws.get("capacityId", "none")
        print(f"[WS] {ws['name']} | capacityId={cap} | type={ws.get('type')}")
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


def get_dataset_sync_status(dataset_id: str) -> str | None:
    return _sync_flags.get(dataset_id)


def restore_sync_flags_from_db() -> None:
    """Load sync_status column from DB into _sync_flags on startup."""
    try:
        db = get_db()
        res = db.table("datasets").select("id, sync_status").execute()
        for row in (res.data or []):
            if row.get("sync_status") and row["id"] not in _sync_flags:
                _sync_flags[row["id"]] = row["sync_status"]
        print(f"[STARTUP] Sync flags restaurados do DB: {len(_sync_flags)}")
    except Exception as e:
        print(f"[STARTUP] Erro ao restaurar sync_flags: {e}")


async def sync_gateway_dataset(dataset_id: str, connection_string: str) -> dict:
    from gateway import get_gateway_schema
    db = get_db()
    try:
        tables = await get_gateway_schema(connection_string)
    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}
    if not tables:
        return {"status": "error", "error": "Nenhuma tabela encontrada"}
    _save_tables(db, dataset_id, tables)
    _sync_flags[dataset_id] = "ok"
    db.table("datasets").update(
        {"synced_at": datetime.now(timezone.utc).isoformat()}
    ).eq("id", dataset_id).execute()
    print(f"[SYNC GATEWAY OK] {dataset_id}: {len(tables)} tabelas via SQL direto")
    return {"status": "ok", "tables": len(tables)}


async def sync_rls_dataset(dataset_id: str, workspace_id: str, username: str, role: str) -> dict:
    db = get_db()
    tables, error = await _get_schema_via_dmv(dataset_id, workspace_id, username, role)
    if tables:
        _save_tables(db, dataset_id, tables)
        _sync_flags[dataset_id] = "ok"
        db.table("datasets").update(
            {"synced_at": datetime.now(timezone.utc).isoformat()}
        ).eq("id", dataset_id).execute()
        print(f"[SYNC RLS OK] {dataset_id}: {len(tables)} tabelas com effectiveIdentity")
        return {"status": "ok", "tables": len(tables)}
    # Keep flag as rls_required so the dataset stays usable with saved credentials
    if dataset_id not in _sync_flags or _sync_flags[dataset_id] not in ("ok", "gateway"):
        _sync_flags[dataset_id] = "rls_required"
    return {"status": "error", "error": error or "Sem tabelas retornadas"}


def get_dataset_schema(dataset_id: str) -> list:
    db = get_db()
    res = (
        db.table("tables")
        .select("id, name, columns(name, data_type, column_type), measures(name, expression, description)")
        .eq("dataset_id", dataset_id)
        .execute()
    )
    return res.data or []
