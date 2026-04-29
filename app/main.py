import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

# Ensure app/ directory is in sys.path so sibling modules resolve on Vercel
_APP_DIR = Path(__file__).parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from dotenv import load_dotenv
load_dotenv(override=True)

import json as _json

from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel

from database import get_db, sync_metadata, get_dataset_schema, get_dataset_sync_status, sync_rls_dataset, sync_gateway_dataset, restore_sync_flags_from_db, _sync_flags
from ai import generate_dax, fix_dax, format_answer, is_followup_question, answer_from_context, generate_sql, fix_sql, is_schema_question, answer_schema_question
from powerbi import execute_query, get_reports, generate_embed_token
import gateway as _gw
from gateway import set_gateway_config, get_gateway_config, execute_sql, detect_dialect, build_connection_string
import config_store


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Restore sync_status flags from DB (gateway, rls_required, etc.)
    restore_sync_flags_from_db()

    # Merge dataset configs from Supabase (overrides local file for Vercel)
    config_store.load_from_supabase()


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
        elif cfg.get("rls_username"):
            if dataset_id not in _sync_flags:
                _sync_flags[dataset_id] = "rls_required"
                print(f"[STARTUP] RLS flag restaurado para dataset {dataset_id}")
    yield


app = FastAPI(title="Power BI Copilot", lifespan=lifespan)

# ── Auth middleware ──
_PUBLIC = {"/api/login", "/api/me"}
_PUBLIC_PREFIX = "/api/debug/"

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/api/") or path in _PUBLIC or path.startswith(_PUBLIC_PREFIX):
        return await call_next(request)
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token:
        return JSONResponse({"detail": "Não autorizado"}, status_code=401)
    try:
        db = get_db()
        result = db.auth.get_user(token)
        if not result.user:
            return JSONResponse({"detail": "Sessão expirada"}, status_code=401)
        request.state.user_email = result.user.email or ""
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


@app.get("/api/debug/reports/{workspace_id}")
async def debug_reports(workspace_id: str):
    from powerbi import get_reports
    try:
        reports = await get_reports(workspace_id)
        return {"count": len(reports), "reports": [{"id": r["id"], "name": r["name"], "datasetId": r.get("datasetId")} for r in reports]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/debug/schema-check/{dataset_id}")
async def debug_schema_check(dataset_id: str):
    db = get_db()
    tables_res = db.table("tables").select("dataset_id, name").eq("dataset_id", dataset_id).execute()
    flag = get_dataset_sync_status(dataset_id)
    ds_res = db.table("datasets").select("id, name, sync_status, synced_at").eq("id", dataset_id).execute()
    return {
        "tables_in_db": len(tables_res.data or []),
        "table_names": [r["name"] for r in (tables_res.data or [])],
        "memory_flag": flag,
        "db_row": ds_res.data,
    }


@app.get("/api/datasets/{workspace_id}")
async def list_datasets(workspace_id: str):
    db = get_db()
    res = db.table("datasets").select("*").eq("workspace_id", workspace_id).order("name").execute()
    datasets = res.data or []

    # Filter tables query to only datasets in this workspace to avoid Supabase's 1000-row default limit
    workspace_ds_ids = [d["id"] for d in datasets]
    ds_with_schema: set = set()
    if workspace_ds_ids:
        tables_res = db.table("tables").select("dataset_id").in_("dataset_id", workspace_ds_ids).execute()
        ds_with_schema = {r["dataset_id"] for r in (tables_res.data or [])}

    # Add has_schema flag; only return refreshable datasets
    result = []
    for d in datasets:
        if d.get("name") in ("Report Usage Metrics Model",):
            continue
        status = get_dataset_sync_status(d["id"]) or d.get("sync_status")
        has_schema = d["id"] in ds_with_schema
        d["has_schema"] = has_schema
        # If schema is already synced, dataset is queryable via DAX — never block the chat
        d["is_gateway"] = status == "gateway" and not has_schema
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
async def chat(req: ChatRequest, request: Request):
    try:
        return await _chat_handler(req, getattr(request.state, "user_email", ""))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Erro interno: {type(e).__name__}: {e}")


async def _chat_handler(req: ChatRequest, user_email: str = ""):
    db = get_db()

    ds_res = db.table("datasets").select("name, workspace_id").eq("id", req.dataset_id).single().execute()
    if not ds_res.data:
        raise HTTPException(400, "Dataset não encontrado. Sincronize os metadados primeiro.")

    schema = get_dataset_schema(req.dataset_id)
    if not schema:
        raise HTTPException(400, "Schema vazio. Clique em 'Sincronizar Metadados' primeiro.")

    dataset_name = ds_res.data["name"]
    workspace_id = ds_res.data.get("workspace_id")

    # Schema/structure questions: answer directly from loaded schema, no DAX needed
    if is_schema_question(req.question):
        answer = answer_schema_question(req.question, schema, dataset_name)
        return {"question": req.question, "dax_query": None, "rows": [], "row_count": 0, "answer": answer}

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
    if not eff_user:
        saved = config_store.get_config(req.dataset_id) or {}
        if saved.get("rls_username"):
            eff_user = saved["rls_username"]
            eff_role = saved.get("rls_role") or eff_role

    rows = []
    query_error = None
    try:
        rows = await execute_query(req.dataset_id, dax, workspace_id, eff_user, eff_role)
    except Exception as e:
        err_str = str(e)
        # If 401 and no effectiveIdentity configured, give actionable guidance
        if "401" in err_str and not eff_user:
            raise HTTPException(
                403,
                "Este dataset exige autenticação de usuário Power BI (effectiveIdentity). "
                "Configure o 'Usuário Power BI' nas configurações deste dataset."
            )
        query_error = err_str

    # Auto-retry: ask AI to fix DAX on error
    if query_error:
        try:
            dax = await fix_dax(dax, query_error, req.question, schema, dataset_name)
            rows = await execute_query(req.dataset_id, dax, workspace_id, eff_user, eff_role)
            query_error = None
        except Exception as e2:
            query_error = str(e2)

    # Auto-retry: if query returned all zeros, period filter value may be wrong — retry without filter
    if not query_error and rows and all(
        all(v == 0 or v is None for v in row.values()) for row in rows
    ):
        try:
            hint = (
                "A query retornou apenas zeros. Provavelmente o filtro de período tem formato errado "
                "(ex: o valor real pode ser '2025/1' em vez de '2025-1', ou um inteiro como 20251). "
                "Reformule a query SEM filtro de período específico, ou use CONTAINSSTRING para busca parcial."
            )
            dax_retry = await fix_dax(dax, hint, req.question, schema, dataset_name)
            rows_retry = await execute_query(req.dataset_id, dax_retry, workspace_id, eff_user, eff_role)
            if rows_retry and any(
                any(v not in (0, None) for v in row.values()) for row in rows_retry
            ):
                dax = dax_retry
                rows = rows_retry
        except Exception:
            pass

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


# ── Data Engineering ──────────────────────────────────────────────────────────

class DatabricksAnalyzeRequest(BaseModel):
    host: str
    token: str


@app.post("/api/databricks/analyze")
async def databricks_analyze(req: DatabricksAnalyzeRequest):
    import databricks as _db
    from ai import analyze_databricks_environment

    def _evt(payload: dict) -> str:
        return f"data: {_json.dumps(payload, ensure_ascii=False)}\n\n"

    async def event_stream():
        try:
            yield _evt({"type": "progress", "step": "connect", "message": "Testando conexão com o workspace..."})
            try:
                await _db.test_connection(req.host, req.token)
            except Exception as e:
                yield _evt({"type": "error", "message": str(e)})
                return
            yield _evt({"type": "progress", "step": "connect", "message": "✓ Conexão estabelecida"})

            yield _evt({"type": "progress", "step": "collect", "message": "Coletando clusters, jobs e catálogos..."})
            try:
                env_data = await _db.collect_environment(req.host, req.token)
            except Exception as e:
                yield _evt({"type": "error", "message": f"Erro ao coletar dados: {e}"})
                return

            n_clusters = len(env_data.get("clusters", []))
            n_jobs = len(env_data.get("jobs", []))
            n_cats = len(env_data.get("catalogs", []))
            n_tables = len(env_data.get("tables_sample", []))

            yield _evt({"type": "progress", "step": "collect", "message": f"✓ {n_clusters} clusters · {n_jobs} jobs · {n_cats} catálogos · {n_tables} tabelas amostradas"})
            yield _evt({"type": "progress", "step": "analysis", "message": "Agente analisando o ambiente..."})
            yield _evt({"type": "analysis_start"})

            async for chunk in analyze_databricks_environment(env_data):
                yield _evt({"type": "analysis_chunk", "text": chunk})

            yield _evt({"type": "done"})

        except Exception as e:
            import traceback
            traceback.print_exc()
            yield _evt({"type": "error", "message": f"Erro inesperado: {e}"})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Fabric Lakehouses (for dropdown) ─────────────────────────────────────────

class FabricCredsRequest(BaseModel):
    tenant_id: str
    client_id: str
    client_secret: str


@app.post("/api/fabric/lakehouses")
async def fabric_lakehouses(req: FabricCredsRequest):
    """Return workspaces + their lakehouses for the ingestion target dropdown."""
    import fabric as _fab
    try:
        token = await _fab.get_token(req.tenant_id, req.client_id, req.client_secret)
    except Exception as e:
        raise HTTPException(400, str(e))

    try:
        workspaces = await _fab.get_workspaces(token)
    except Exception as e:
        raise HTTPException(400, f"Erro ao listar workspaces: {e}")

    user_workspaces = [
        w for w in workspaces
        if w.get("type") not in ("PersonalGroup",) and w.get("name") not in ("My workspace",)
    ]

    if not user_workspaces:
        raise HTTPException(404, "Nenhum workspace encontrado. Verifique as permissões do Service Principal no Fabric.")

    async def _with_lakehouses(ws: dict) -> dict:
        lh_error = None
        try:
            lhs = await _fab.get_lakehouses(token, ws["id"])
        except Exception as e:
            lhs = []
            lh_error = str(e)
        return {
            "workspace_id": ws["id"],
            "workspace_name": ws["name"],
            "lakehouses": [{"id": lh["id"], "name": lh["name"]} for lh in lhs],
            "lakehouses_error": lh_error,
        }

    results = await asyncio.gather(*[_with_lakehouses(ws) for ws in user_workspaces])
    return results


# ── Ingestion pipeline ────────────────────────────────────────────────────────

class IngestionRequest(BaseModel):
    db_dialect: str
    db_host: str
    db_port: int | None = None
    db_name: str
    db_user: str
    db_password: str
    sql_query: str
    tenant_id: str
    client_id: str
    client_secret: str
    workspace_id: str
    lakehouse_id: str
    table_name: str


@app.post("/api/ingestion/run")
async def ingestion_run(req: IngestionRequest):
    import ingestion as _ing
    from gateway import build_connection_string

    def _evt(payload: dict) -> str:
        return f"data: {_json.dumps(payload, ensure_ascii=False)}\n\n"

    async def event_stream():
        try:
            conn_str = build_connection_string(
                req.db_dialect, req.db_host, req.db_port,
                req.db_name, req.db_user, req.db_password,
            )
            safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in req.table_name)
            file_path = f"Files/ingestion/{safe_name}/data.parquet"

            # 1. Extract
            yield _evt({"type": "progress", "step": "extract", "message": f"Conectando à fonte ({req.db_dialect})..."})
            try:
                parquet_bytes, row_count, columns = await _ing.extract_data(conn_str, req.sql_query)
            except Exception as e:
                yield _evt({"type": "error", "message": f"Erro na extração: {e}"})
                return

            mb = round(len(parquet_bytes) / 1024 / 1024, 2)
            yield _evt({"type": "progress", "step": "extract",
                        "message": f"✓ {row_count:,} linhas · {len(columns)} colunas · {mb} MB (Parquet)"})

            # 2. Auth
            yield _evt({"type": "progress", "step": "auth", "message": "Autenticando no OneLake..."})
            try:
                onelake_token, fabric_token = await asyncio.gather(
                    _ing.get_onelake_token(req.tenant_id, req.client_id, req.client_secret),
                    _ing.get_fabric_token(req.tenant_id, req.client_id, req.client_secret),
                )
            except Exception as e:
                yield _evt({"type": "error", "message": f"Erro de autenticação: {e}"})
                return
            yield _evt({"type": "progress", "step": "auth", "message": "✓ Tokens obtidos"})

            # 3. Upload
            yield _evt({"type": "progress", "step": "upload",
                        "message": f"Fazendo upload para OneLake → {file_path}..."})
            try:
                await _ing.upload_to_onelake(onelake_token, req.workspace_id, req.lakehouse_id, file_path, parquet_bytes)
            except Exception as e:
                yield _evt({"type": "error", "message": f"Erro no upload: {e}"})
                return
            yield _evt({"type": "progress", "step": "upload", "message": "✓ Upload concluído"})

            # 4. Register as Delta table
            yield _evt({"type": "progress", "step": "table",
                        "message": f"Registrando tabela '{safe_name}' no Fabric..."})
            try:
                op_id = await _ing.load_table(fabric_token, req.workspace_id, req.lakehouse_id, safe_name, file_path)
            except Exception as e:
                yield _evt({"type": "error", "message": f"Erro ao registrar tabela: {e}"})
                return

            if op_id:
                yield _evt({"type": "progress", "step": "table", "message": "Aguardando Fabric processar a tabela..."})
                status = await _ing.poll_operation(fabric_token, op_id)
                if status != "Succeeded":
                    yield _evt({"type": "error", "message": f"Fabric retornou status '{status}' ao criar a tabela."})
                    return

            yield _evt({"type": "progress", "step": "table",
                        "message": f"✓ Tabela '{safe_name}' disponível no Lakehouse"})
            import datetime as _dt
            yield _evt({
                "type": "done", "destination": "fabric",
                "table": safe_name, "rows": row_count, "columns": columns,
                "workspace_id": req.workspace_id, "lakehouse_id": req.lakehouse_id,
                "finished_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

        except Exception as e:
            import traceback
            traceback.print_exc()
            yield _evt({"type": "error", "message": f"Erro inesperado: {e}"})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class FabricAnalyzeRequest(BaseModel):
    tenant_id: str
    client_id: str
    client_secret: str


@app.post("/api/fabric/analyze")
async def fabric_analyze(req: FabricAnalyzeRequest):
    import fabric as _fab
    from ai import analyze_fabric_environment

    def _evt(payload: dict) -> str:
        return f"data: {_json.dumps(payload, ensure_ascii=False)}\n\n"

    async def event_stream():
        try:
            yield _evt({"type": "progress", "step": "token", "message": "Autenticando no Microsoft Entra ID..."})
            try:
                token = await _fab.get_token(req.tenant_id, req.client_id, req.client_secret)
            except Exception as e:
                yield _evt({"type": "error", "message": str(e)})
                return
            yield _evt({"type": "progress", "step": "token", "message": "✓ Token obtido com sucesso"})

            yield _evt({"type": "progress", "step": "collect", "message": "Carregando workspaces, Lakehouses e Pipelines..."})
            try:
                env_data = await _fab.collect_environment(req.tenant_id, req.client_id, req.client_secret)
            except Exception as e:
                yield _evt({"type": "error", "message": f"Erro ao coletar dados: {e}"})
                return

            n_ws = len(env_data.get("workspaces", []))
            total_ws = env_data.get("total_workspaces", n_ws)
            n_lh = sum(len(w.get("lakehouses", [])) for w in env_data.get("workspaces", []))
            n_pip = sum(len(w.get("pipelines", [])) for w in env_data.get("workspaces", []))

            yield _evt({"type": "progress", "step": "collect", "message": f"✓ {total_ws} workspaces · {n_lh} Lakehouses · {n_pip} pipelines"})
            yield _evt({"type": "progress", "step": "analysis", "message": "Agente analisando o ambiente Fabric..."})
            yield _evt({"type": "analysis_start"})

            async for chunk in analyze_fabric_environment(env_data):
                yield _evt({"type": "analysis_chunk", "text": chunk})

            yield _evt({"type": "done"})

        except Exception as e:
            import traceback
            traceback.print_exc()
            yield _evt({"type": "error", "message": f"Erro inesperado: {e}"})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── CSV Upload ────────────────────────────────────────────────────────────────

@app.post("/api/upload/csv")
async def upload_csv_route(
    file: UploadFile = File(...),
    destination: str = Form(...),      # 'fabric' | 'databricks'
    table_name: str = Form(...),
    # Databricks
    db_host: str = Form(None),
    db_token: str = Form(None),
    # Fabric
    tenant_id: str = Form(None),
    client_id: str = Form(None),
    client_secret: str = Form(None),
    workspace_id: str = Form(None),
    lakehouse_id: str = Form(None),
):
    import upload as _up
    import ingestion as _ing

    # Read file into memory before StreamingResponse starts — UploadFile closes after handler returns
    file_bytes = await file.read()

    def _evt(payload: dict) -> str:
        return f"data: {_json.dumps(payload, ensure_ascii=False)}\n\n"

    async def event_stream():
        try:
            size_kb = round(len(file_bytes) / 1024, 1)

            yield _evt({"type": "progress", "step": "parse",
                        "message": f"Lendo CSV ({size_kb} KB)..."})
            try:
                parquet_bytes, row_count, columns = await _up.csv_to_parquet(file_bytes)
            except Exception as e:
                yield _evt({"type": "error", "message": f"Erro ao processar CSV: {e}"})
                return

            mb = round(len(parquet_bytes) / 1024 / 1024, 2)
            yield _evt({"type": "progress", "step": "parse",
                        "message": f"✓ {row_count:,} linhas · {len(columns)} colunas · {mb} MB"})

            if destination == "databricks":
                yield _evt({"type": "progress", "step": "upload",
                            "message": "Enviando para Databricks DBFS..."})
                try:
                    dbfs_path = await _up.upload_csv_to_databricks(
                        db_host, db_token, file_bytes, table_name
                    )
                except Exception as e:
                    yield _evt({"type": "error", "message": f"Erro no upload: {e}"})
                    return
                yield _evt({"type": "progress", "step": "upload",
                            "message": f"✓ Arquivo salvo em {dbfs_path}"})
                import datetime as _dt
                yield _evt({
                    "type": "done", "destination": "databricks",
                    "dbfs_path": dbfs_path, "rows": row_count, "columns": columns,
                    "finished_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })

            elif destination == "fabric":
                safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)
                file_path = f"Files/ingestion/{safe_name}/data.parquet"

                yield _evt({"type": "progress", "step": "auth",
                            "message": "Autenticando no Microsoft Fabric..."})
                try:
                    onelake_token, fabric_token = await asyncio.gather(
                        _ing.get_onelake_token(tenant_id, client_id, client_secret),
                        _ing.get_fabric_token(tenant_id, client_id, client_secret),
                    )
                except Exception as e:
                    yield _evt({"type": "error", "message": f"Erro de autenticação: {e}"})
                    return
                yield _evt({"type": "progress", "step": "auth",
                            "message": "✓ Tokens obtidos"})

                yield _evt({"type": "progress", "step": "upload",
                            "message": f"Fazendo upload para OneLake → {file_path}..."})
                try:
                    await _ing.upload_to_onelake(
                        onelake_token, workspace_id, lakehouse_id, file_path, parquet_bytes
                    )
                except Exception as e:
                    yield _evt({"type": "error", "message": f"Erro no upload: {e}"})
                    return
                yield _evt({"type": "progress", "step": "upload",
                            "message": "✓ Upload concluído"})

                yield _evt({"type": "progress", "step": "table",
                            "message": f"Registrando tabela '{safe_name}' no Lakehouse..."})
                try:
                    op_id = await _ing.load_table(
                        fabric_token, workspace_id, lakehouse_id, safe_name, file_path
                    )
                except Exception as e:
                    yield _evt({"type": "error", "message": f"Erro ao registrar tabela: {e}"})
                    return

                if op_id:
                    yield _evt({"type": "progress", "step": "table",
                                "message": "Aguardando Fabric processar a tabela..."})
                    status = await _ing.poll_operation(fabric_token, op_id)
                    if status != "Succeeded":
                        yield _evt({"type": "error",
                                    "message": f"Fabric retornou status '{status}'"})
                        return

                yield _evt({"type": "progress", "step": "table",
                            "message": f"✓ Tabela '{safe_name}' disponível no Lakehouse"})
                import datetime as _dt
                yield _evt({
                    "type": "done", "destination": "fabric",
                    "table": safe_name, "rows": row_count, "columns": columns,
                    "workspace_id": workspace_id, "lakehouse_id": lakehouse_id,
                    "finished_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
            else:
                yield _evt({"type": "error", "message": f"Destino desconhecido: {destination}"})

        except Exception as e:
            import traceback
            traceback.print_exc()
            yield _evt({"type": "error", "message": f"Erro inesperado: {e}"})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Data Maturity Assessment ──────────────────────────────────────────────────

class DataMaturityRequest(BaseModel):
    governanca: str = ""
    arquitetura: str = ""
    modelagem: str = ""
    armazenamento: str = ""
    seguranca: str = ""
    integracao: str = ""
    metadados: str = ""
    qualidade: str = ""
    bi: str = ""


@app.post("/api/data-maturity/analyze")
async def data_maturity_analyze(req: DataMaturityRequest):
    from ai import analyze_data_maturity

    def _evt(payload: dict) -> str:
        return f"data: {_json.dumps(payload, ensure_ascii=False)}\n\n"

    async def event_stream():
        try:
            yield _evt({"type": "analysis_start"})
            async for chunk in analyze_data_maturity(req.model_dump()):
                yield _evt({"type": "analysis_chunk", "text": chunk})
            yield _evt({"type": "done"})
        except Exception as e:
            import traceback
            traceback.print_exc()
            yield _evt({"type": "error", "message": f"Erro inesperado: {e}"})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class AzureParquetRequest(BaseModel):
    tenant_id: str
    client_id: str
    username: str
    password: str
    storage_account: str
    container_name: str
    prefix: str = ""


@app.post("/api/azure/find-latest-parquet")
async def azure_find_latest_parquet(req: AzureParquetRequest):
    from azure_storage import find_latest_parquet_blobs
    try:
        blobs = await find_latest_parquet_blobs(
            tenant_id=req.tenant_id,
            client_id=req.client_id,
            username=req.username,
            password=req.password,
            storage_account=req.storage_account,
            container_name=req.container_name,
            prefix=req.prefix,
        )
        return {"blobs": blobs, "count": len(blobs)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
