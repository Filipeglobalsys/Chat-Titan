import os
from contextlib import asynccontextmanager
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=True)

from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel

from database import get_db, sync_metadata, get_dataset_schema, get_dataset_sync_status, sync_rls_dataset, sync_gateway_dataset, restore_sync_flags_from_db, _sync_flags
from ai import generate_dax, fix_dax, format_answer, is_followup_question, answer_from_context, generate_sql, fix_sql
from powerbi import execute_query, get_reports, generate_embed_token
import gateway as _gw
from gateway import set_gateway_config, get_gateway_config, execute_sql, detect_dialect, build_connection_string
import config_store


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Restore sync_status flags from DB (gateway, rls_required, etc.)
    restore_sync_flags_from_db()

    # Restore saved gateway connection strings + fill flags from config_store
    for dataset_id, cfg in config_store.get_all().items():
        if cfg.get("db_host") and cfg.get("db_password") is not None:
            try:
                conn_str = build_connection_string(
                    cfg.get("db_dialect", "mssql"),
                    cfg["db_host"],
                    cfg.get("db_port"),
                    cfg.get("db_name", ""),
                    cfg.get("db_user", ""),
                    cfg.get("db_password", ""),
                )
                set_gateway_config(dataset_id, conn_str)
                _sync_flags[dataset_id] = "gateway"
                print(f"[STARTUP] Gateway restaurado para dataset {dataset_id}")
            except Exception as e:
                print(f"[STARTUP] Erro ao restaurar gateway {dataset_id}: {e}")
        elif cfg.get("rls_username") and cfg.get("rls_role"):
            if dataset_id not in _sync_flags:
                _sync_flags[dataset_id] = "rls_required"
                print(f"[STARTUP] RLS flag restaurado para dataset {dataset_id}")
    yield


app = FastAPI(title="Power BI Copilot", lifespan=lifespan)

# ── Auth middleware ──
_PUBLIC = {"/api/login", "/api/me"}

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/api/") or path in _PUBLIC:
        return await call_next(request)
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        return JSONResponse({"detail": "Não autorizado"}, status_code=401)
    try:
        db = get_db()
        result = db.auth.get_user(token)
        if not result.user:
            return JSONResponse({"detail": "Sessão expirada"}, status_code=401)
    except Exception:
        return JSONResponse({"detail": "Token inválido"}, status_code=401)
    return await call_next(request)

STATIC = Path(__file__).parent / "static"
if STATIC.exists():
    app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)

@app.get("/")
async def root():
    return FileResponse(STATIC / "index.html")


# ---------- Auth ----------

class LoginRequest(BaseModel):
    email: str
    password: str

@app.post("/api/login")
async def login(req: LoginRequest):
    db = get_db()
    try:
        result = db.auth.sign_in_with_password({"email": req.email, "password": req.password})
        if not result.session:
            raise HTTPException(401, "Email ou senha incorretos")
        meta = result.user.user_metadata or {}
        full_name = meta.get("full_name") or meta.get("name") or result.user.email
        return {
            "access_token": result.session.access_token,
            "user": {"email": result.user.email, "name": full_name},
        }
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Email ou senha incorretos")

@app.get("/api/me")
async def me(request: Request):
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(401, "Não autorizado")
    try:
        db = get_db()
        result = db.auth.get_user(token)
        user = result.user
        meta = user.user_metadata or {}
        full_name = meta.get("full_name") or meta.get("name") or user.email
        return {"email": user.email, "name": full_name}
    except Exception:
        raise HTTPException(401, "Sessão expirada")


# ---------- Workspaces ----------

@app.get("/api/workspaces")
async def list_workspaces():
    db = get_db()
    res = db.table("workspaces").select("*").order("name").execute()
    return res.data


@app.post("/api/workspaces/sync")
async def sync(force: bool = False):
    try:
        result = await sync_metadata(force=force)
        return {"success": True, **result}
    except Exception as e:
        raise HTTPException(500, str(e))


# ---------- Datasets ----------

@app.get("/api/reports/{workspace_id}")
async def list_reports(workspace_id: str):
    try:
        reports = await get_reports(workspace_id)
        return reports
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/report-url/{workspace_id}/{report_id}")
async def report_url(
    workspace_id: str,
    report_id: str,
    dataset_id: str | None = None,
    username: str | None = None,
    role: str | None = None,
):
    """Return embed URL + token. Accepts optional RLS identity for datasets with effectiveIdentity."""
    try:
        roles = [role] if role else None
        token_data = await generate_embed_token(workspace_id, report_id, dataset_id, username, roles)
        embed_token = token_data.get("token", "")
        embed_url = f"https://app.powerbi.com/reportEmbed?reportId={report_id}&groupId={workspace_id}"
        return {"url": embed_url, "token": embed_token}
    except Exception as e:
        raise HTTPException(500, f"Erro ao gerar embed token: {e}")


@app.get("/api/datasets/{workspace_id}")
async def list_datasets(workspace_id: str):
    db = get_db()
    # Use count per dataset_id to avoid the 1000-row default Supabase limit
    tables_res = db.table("tables").select("dataset_id").limit(100_000).execute()
    ds_with_schema = {r["dataset_id"] for r in (tables_res.data or [])}

    res = db.table("datasets").select("*").eq("workspace_id", workspace_id).order("name").execute()
    datasets = res.data or []
    # Add has_schema flag; only return refreshable datasets
    result = []
    for d in datasets:
        if d.get("name") in ("Report Usage Metrics Model",):
            continue
        status = get_dataset_sync_status(d["id"]) or d.get("sync_status")
        d["has_schema"] = d["id"] in ds_with_schema
        d["is_gateway"] = status == "gateway"
        d["is_rls"] = status == "rls_required"
        result.append(d)
    return result


@app.get("/api/schema/{dataset_id}")
async def dataset_schema(dataset_id: str):
    return get_dataset_schema(dataset_id)


# ---------- Chat ----------

class HistoryMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str


class DatasetConfigRequest(BaseModel):
    rls_username: str | None = None
    rls_role: str | None = None
    db_dialect: str | None = None
    db_host: str | None = None
    db_port: int | None = None
    db_name: str | None = None
    db_user: str | None = None
    db_password: str | None = None


@app.get("/api/config/{dataset_id}")
async def get_dataset_config(dataset_id: str):
    cfg = config_store.get_config(dataset_id) or {}
    safe = {k: v for k, v in cfg.items() if k != "db_password"}
    safe["has_password"] = bool(cfg.get("db_password"))
    return safe


@app.post("/api/config/{dataset_id}")
async def save_dataset_config(dataset_id: str, req: DatasetConfigRequest):
    db = get_db()
    ds_res = db.table("datasets").select("workspace_id, name").eq("id", dataset_id).single().execute()
    if not ds_res.data:
        raise HTTPException(400, "Dataset não encontrado")
    workspace_id = ds_res.data.get("workspace_id")

    existing = config_store.get_config(dataset_id) or {}
    cfg: dict = {}
    if req.rls_username is not None:
        cfg["rls_username"] = req.rls_username
    elif existing.get("rls_username"):
        cfg["rls_username"] = existing["rls_username"]
    if req.rls_role is not None:
        cfg["rls_role"] = req.rls_role
    elif existing.get("rls_role"):
        cfg["rls_role"] = existing["rls_role"]
    if req.db_dialect is not None:
        cfg["db_dialect"] = req.db_dialect
    elif existing.get("db_dialect"):
        cfg["db_dialect"] = existing["db_dialect"]
    if req.db_host is not None:
        cfg["db_host"] = req.db_host
    elif existing.get("db_host"):
        cfg["db_host"] = existing["db_host"]
    if req.db_port is not None:
        cfg["db_port"] = req.db_port
    elif existing.get("db_port"):
        cfg["db_port"] = existing["db_port"]
    if req.db_name is not None:
        cfg["db_name"] = req.db_name
    elif existing.get("db_name"):
        cfg["db_name"] = existing["db_name"]
    if req.db_user is not None:
        cfg["db_user"] = req.db_user
    elif existing.get("db_user"):
        cfg["db_user"] = existing["db_user"]
    if req.db_password:
        cfg["db_password"] = req.db_password
    elif existing.get("db_password"):
        cfg["db_password"] = existing["db_password"]

    config_store.set_config(dataset_id, cfg)

    result: dict = {"status": "ok", "message": "Configuração salva"}

    if cfg.get("db_host"):
        if not cfg.get("db_password"):
            raise HTTPException(400, "Senha do banco de dados é obrigatória")
        conn_str = build_connection_string(
            cfg.get("db_dialect", "mssql"),
            cfg["db_host"],
            cfg.get("db_port"),
            cfg.get("db_name", ""),
            cfg.get("db_user", ""),
            cfg["db_password"],
        )
        sync_result = await sync_gateway_dataset(dataset_id, conn_str)
        if sync_result["status"] != "ok":
            raise HTTPException(400, f"Erro na conexão com banco de dados: {sync_result.get('error')}")
        set_gateway_config(dataset_id, conn_str)
        result["db_sync"] = sync_result
    elif cfg.get("rls_username") and cfg.get("rls_role"):
        rls_result = await sync_rls_dataset(dataset_id, workspace_id, cfg["rls_username"], cfg["rls_role"])
        result["rls_sync"] = rls_result
        if rls_result["status"] != "ok":
            # Credentials saved — schema sync failed (likely missing Build permission)
            result["warning"] = (
                "Credenciais RLS salvas. Não foi possível sincronizar o schema automaticamente "
                "(o Service Principal precisa de permissão Build no dataset). "
                "Use 'Forçar Resync completo' após conceder a permissão, ou configure via Banco de Dados."
            )

    return result


class GatewayConfigRequest(BaseModel):
    connection_string: str


@app.post("/api/gateway/config/{dataset_id}")
async def configure_gateway(dataset_id: str, req: GatewayConfigRequest):
    db = get_db()
    ds_res = db.table("datasets").select("id").eq("id", dataset_id).single().execute()
    if not ds_res.data:
        raise HTTPException(400, "Dataset não encontrado")
    result = await sync_gateway_dataset(dataset_id, req.connection_string)
    if result["status"] != "ok":
        raise HTTPException(400, result.get("error", "Falha ao conectar"))
    set_gateway_config(dataset_id, req.connection_string)
    return result


class RlsSyncRequest(BaseModel):
    username: str
    role: str


@app.post("/api/schema/sync-rls/{dataset_id}")
async def sync_rls_schema(dataset_id: str, req: RlsSyncRequest):
    db = get_db()
    ds_res = db.table("datasets").select("workspace_id").eq("id", dataset_id).single().execute()
    if not ds_res.data:
        raise HTTPException(400, "Dataset não encontrado")
    workspace_id = ds_res.data.get("workspace_id")
    result = await sync_rls_dataset(dataset_id, workspace_id, req.username, req.role)
    if result["status"] != "ok":
        raise HTTPException(400, result.get("error", "Falha ao sincronizar schema RLS"))
    return result


class ChatRequest(BaseModel):
    question: str
    dataset_id: str
    history: list[HistoryMessage] = []
    effective_username: str | None = None
    effective_role: str | None = None
    report_name: str | None = None


@app.post("/api/chat")
async def chat(req: ChatRequest):
    try:
        return await _chat_handler(req)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Erro interno: {type(e).__name__}: {e}")


async def _chat_handler(req: ChatRequest):
    db = get_db()

    ds_res = db.table("datasets").select("name, workspace_id").eq("id", req.dataset_id).single().execute()
    if not ds_res.data:
        raise HTTPException(400, "Dataset não encontrado. Sincronize os metadados primeiro.")

    schema = get_dataset_schema(req.dataset_id)
    if not schema:
        raise HTTPException(400, "Schema vazio. Clique em 'Sincronizar Metadados' primeiro.")

    dataset_name = ds_res.data["name"]
    workspace_id = ds_res.data.get("workspace_id")

    # If it's a follow-up clarification, answer from context without running DAX
    if is_followup_question(req.question, req.history):
        if not req.history:
            return {"question": req.question, "dax_query": None, "rows": [], "row_count": 0,
                    "answer": "Não tenho contexto da pergunta anterior. Por favor, faça uma pergunta específica sobre os dados."}
        answer = await answer_from_context(req.question, req.history)
        return {"question": req.question, "dax_query": None, "rows": [], "row_count": 0, "answer": answer}

    # Gateway datasets: query source DB directly with SQL
    conn_str = get_gateway_config(req.dataset_id)
    if conn_str:
        dialect = detect_dialect(conn_str)
        sql = await generate_sql(req.question, schema, dataset_name, dialect, req.history, req.report_name)
        rows = []
        query_error = None
        try:
            rows = await execute_sql(conn_str, sql)
        except Exception as e:
            query_error = str(e)
        if query_error:
            try:
                sql = await fix_sql(sql, query_error, req.question, schema, dataset_name, dialect)
                rows = await execute_sql(conn_str, sql)
                query_error = None
            except Exception as e2:
                query_error = str(e2)
        if query_error:
            answer = f"Não foi possível executar a query SQL.\n\nErro: {query_error}"
        else:
            answer = await format_answer(req.question, sql, rows, schema, req.history)
        return {"question": req.question, "dax_query": sql, "rows": rows, "row_count": len(rows), "answer": answer}

    dax = await generate_dax(req.question, schema, dataset_name, req.history, req.report_name)

    eff_user = req.effective_username
    eff_role = req.effective_role
    # Auto-load RLS creds from saved config if not provided in request
    if not (eff_user and eff_role):
        saved = config_store.get_config(req.dataset_id) or {}
        if saved.get("rls_username") and saved.get("rls_role"):
            eff_user = saved["rls_username"]
            eff_role = saved["rls_role"]

    rows = []
    query_error = None
    try:
        rows = await execute_query(req.dataset_id, dax, workspace_id, eff_user, eff_role)
    except Exception as e:
        query_error = str(e)

    # Auto-retry: ask AI to fix DAX on error
    if query_error:
        try:
            dax = await fix_dax(dax, query_error, req.question, schema, dataset_name)
            rows = await execute_query(req.dataset_id, dax, workspace_id, eff_user, eff_role)
            query_error = None
        except Exception as e2:
            query_error = str(e2)

    if query_error:
        answer = (
            f"Não foi possível executar a query mesmo após correção automática.\n\n"
            f"Erro: {query_error}"
        )
    else:
        answer = await format_answer(req.question, dax, rows, schema, req.history)

    return {
        "question": req.question,
        "dax_query": dax,
        "rows": rows,
        "row_count": len(rows),
        "answer": answer,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
