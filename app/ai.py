import os
import anthropic

_client: anthropic.AsyncAnthropic | None = None
_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-opus-4-7")


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client

_DAX_RULES = """
DAX Rules (follow strictly):
- Return ONLY the DAX query — no explanation, no markdown, no code fences
- Always start with EVALUATE
- Use Table[Column] for columns, [MeasureName] for measures
- NEVER invent or guess measure/column names — use ONLY names that appear exactly in the schema
- NEVER reference auto-generated tables (DateTableTemplate_, LocalDateTable_) — they are hidden
- If no measure matches, compute with DAX: COUNTROWS, DISTINCTCOUNT, SUM, AVERAGE, etc.

Measure selection rules (CRITICAL):
- Match measures by keyword similarity to the user's question
- "documentos" → use measures with "Documento" in name
- "produtos" → use measures with "Produto" in name
- "romaneiro" → use measures with "Romaneiro" in name
- NEVER use a measure from a different domain than what the user asked for

Period/date filter rules (CRITICAL):
- When the user mentions a specific period (e.g. "202401", "janeiro 2024", "2024-1"), ALWAYS apply it as a filter
- Do NOT find the best/max/top period — filter for the EXACT period the user specified
- Period columns may store values as integers (e.g. 202401) or strings (e.g. "202401") — check schema data type
- If column data_type is integer/int64: use Table[Col] = 202401 (no quotes)
- If column data_type is string/text: use Table[Col] = "202401"
- NEVER wrap a specific-period question with TOPN
- For semester/academic period filters (e.g. "2025-1", "2024-2"): the stored value format is UNKNOWN.
  Prefer CONTAINSSTRING for safer matching: FILTER(ALL(Table[Col]), CONTAINSSTRING(Table[Col], "2025") && CONTAINSSTRING(Table[Col], "1"))
  OR use IN with multiple possible formats: Table[Col] IN {"2025-1", "2025/1", "20251", "1/2025"}

Pattern 1 — Single value with filter:
  EVALUATE ROW("Label", CALCULATE([Measure], Table[Col] = value))

Pattern 2 — Multiple metrics for one period (general analysis, summaries):
  EVALUATE
  ADDCOLUMNS(
      ROW("Period", "value"),
      "Metric1", CALCULATE([Measure1], Table[Col] = value),
      "Metric2", CALCULATE([Measure2], Table[Col] = value)
  )
  Use when user asks for "análise geral", "resumo", "visão geral", or multiple metrics at once.
  Limit to the 8 most relevant measures — do NOT list all measures.
  NEVER use column references (Table[Col]) inside ADDCOLUMNS — only CALCULATE expressions.

Pattern 3 — Breakdown by dimension (group by):
  EVALUATE
  CALCULATETABLE(
      SUMMARIZECOLUMNS(Table[Dimension], "Metric", [Measure]),
      optional_filter
  )
  Use SUMMARIZECOLUMNS with AT MOST 3 measures. More than that causes API errors.

Pattern 4 — Top N periods by metric (CRITICAL: use this exact structure for "maior mês", "melhor período", "ranking"):
  EVALUATE
  TOPN(
      1,
      CALCULATETABLE(
          ADDCOLUMNS(
              VALUES(Table[DateColumn]),
              "Metric1", CALCULATE([Measure1]),
              "Metric2", CALCULATE([Measure2]),
              "Total", CALCULATE([Measure1]) + CALCULATE([Measure2])
          ),
          year_filter  -- REQUIRED if user specifies a year (e.g. "em 2024")
      ),
      [Total],
      DESC
  )
  ORDER BY [Total] DESC
  NEVER use TOPN(N, SUMMARIZECOLUMNS(...), ...) — it causes 400 errors.
  ALWAYS use TOPN(N, CALCULATETABLE(ADDCOLUMNS(VALUES(...), ...), filter), [VirtualCol], DESC).

  Year filter for YYYYMM integer columns (CRITICAL):
  - "em 2024" → Table[Col] >= 202401 && Table[Col] <= 202412
  - "em 2023" → Table[Col] >= 202301 && Table[Col] <= 202312
  - ALWAYS apply the year filter inside CALCULATETABLE when the user specifies a year
  - NEVER search across all years when the user restricts to a specific year

Filter syntax:
- CORRECT: CALCULATE([M], Table[Col] = value)
- CORRECT: CALCULATETABLE(SUMMARIZECOLUMNS(...), Table[Col] = value)
- CORRECT: CALCULATETABLE(ADDCOLUMNS(VALUES(...), ...), Table[Col] = value)
- WRONG: TOPN(N, SUMMARIZECOLUMNS(...), ...)  ← causes 400 error
- WRONG: SUMMARIZECOLUMNS(col, KEEPFILTERS(...), "M", expr)
- WRONG: SUMMARIZECOLUMNS(col, "M", expr, FILTER(tbl, ...))
- WRONG: ADDCOLUMNS(..., "Col", Table[Column])  ← column reference outside row context
- WRONG: CALCULATETABLE(..., CALCULATE([M]) > 0)  ← CALCULATE inside boolean/filter expression causes "CALCULATE used in True/False expression" error
- WRONG: FILTER(Table, CALCULATE([M]) > value)  ← same error

CALCULATE in filter position (CRITICAL):
- NEVER use CALCULATE() as a filter argument or inside a boolean expression
- NEVER write: CALCULATETABLE(expr, CALCULATE([M]) > x) or FILTER(tbl, CALCULATE([M]) > x)
- To filter by a measure threshold, use VAR:
  VAR threshold = CALCULATE([M], some_filter)
  RETURN FILTER(VALUES(Table[Col]), [M] > threshold)

Pattern 5 — Trend over time (improving/worsening, "estamos melhorando", "evolução"):
  Return values per period and let the analysis interpret the trend:
  EVALUATE
  SUMMARIZECOLUMNS(
      Table[PeriodColumn],
      "Metrica", [Measure]
  )
  ORDER BY Table[PeriodColumn] ASC
  NEVER add CALCULATE comparisons inside filter position for trend analysis.
  The formatted answer will compare values and state whether performance improved or worsened.

- Use TOPN ONLY when user asks for "top N", "maior", "menor", "ranking" WITHOUT a specific period
- Use ORDER BY for multi-row results
"""


def _schema_to_text(schema: list) -> str:
    lines = []
    for table in schema:
        name = table["name"]
        if name.startswith("DateTableTemplate_") or name.startswith("LocalDateTable_"):
            continue
        lines.append(f"Table: {name}")
        for col in table.get("columns") or []:
            lines.append(f"  Column: {col['name']} ({col.get('data_type', '')})")
        for m in table.get("measures") or []:
            desc = f" — {m['description']}" if m.get("description") else ""
            lines.append(f"  Measure: [{m['name']}]{desc}")
    return "\n".join(lines)


_SCHEMA_KEYWORDS = [
    "relacionamento", "relacionamentos", "relação", "relações", "relationship", "relationships",
    "quais tabelas", "quais são as tabelas", "lista de tabelas", "listar tabelas",
    "quais colunas", "quais são as colunas", "lista de colunas",
    "quais medidas", "quais são as medidas", "lista de medidas",
    "estrutura do dataset", "estrutura das tabelas", "schema do dataset",
    "o que tem no dataset", "o que existe no dataset", "mostre o schema",
    "me mostra as tabelas", "me mostre as tabelas",
]

def is_schema_question(question: str) -> bool:
    q = question.lower().strip()
    return any(kw in q for kw in _SCHEMA_KEYWORDS)


def answer_schema_question(question: str, schema: list, dataset_name: str) -> str:
    q = question.lower()
    wants_relationships = any(kw in q for kw in ["relacionamento", "relação", "relações", "relationship"])

    visible = [t for t in schema if not t["name"].startswith("DateTableTemplate_") and not t["name"].startswith("LocalDateTable_")]

    lines = [f"**Dataset: {dataset_name}**\n", f"**Tabelas ({len(visible)}):**\n"]
    for table in visible:
        cols = table.get("columns") or []
        measures = table.get("measures") or []
        col_names = ", ".join(c["name"] for c in cols[:12]) + (" ..." if len(cols) > 12 else "")
        meas_names = ", ".join(m["name"] for m in measures[:8]) + (" ..." if len(measures) > 8 else "")
        lines.append(f"**{table['name']}**")
        if cols:
            lines.append(f"  Colunas ({len(cols)}): {col_names}")
        if measures:
            lines.append(f"  Medidas ({len(measures)}): {meas_names}")
        lines.append("")

    if wants_relationships:
        lines.append("**Relacionamentos inferidos** (colunas com nomes idênticos entre tabelas):\n")
        col_to_tables: dict[str, list] = {}
        for table in visible:
            for col in (table.get("columns") or []):
                cname = col["name"]
                col_to_tables.setdefault(cname, []).append(table["name"])
        shared = {k: v for k, v in col_to_tables.items() if len(v) > 1}
        if shared:
            for col_name, tables_with_col in shared.items():
                lines.append(f"  • `{col_name}`: {' ↔ '.join(tables_with_col)}")
        else:
            lines.append("  Nenhuma coluna compartilhada encontrada entre as tabelas. Os relacionamentos definidos no Power BI Desktop não são exportados pela API de metadados.")

    return "\n".join(lines)


_FOLLOWUP_KEYWORDS = [
    "qual foi", "qual é", "me diz", "como se chama", "o nome", "qual o nome",
    "que mês", "que mes", "qual mês", "qual mes", "quando", "explica", "explique",
    "por que", "porque", "o que significa", "o que quer dizer", "detalha",
]

def is_followup_question(question: str, history: list) -> bool:
    """Returns True if the question can be answered from the last exchange without new DAX."""
    if not history:
        return False
    q = question.lower().strip()
    words = q.split()
    if len(words) > 6:
        return False
    return any(q.startswith(kw) for kw in _FOLLOWUP_KEYWORDS)


async def answer_from_context(question: str, history: list) -> str:
    """Answer a follow-up question using only the last exchange (2 messages)."""
    last_exchange = history[-2:] if len(history) >= 2 else history
    history_text = "\n".join(
        f"{'Usuário' if h.role == 'user' else 'IA'}: {h.content}" for h in last_exchange
    )
    resp = await _get_client().messages.create(
        model=_MODEL,
        max_tokens=1024,
        system="Você é um analista de dados. Responda a pergunta do usuário com base apenas na resposta imediatamente anterior da IA. Seja direto e conciso. Responda em português.",
        messages=[
            {"role": "user", "content": f"Resposta anterior da IA:\n{history_text}\n\nPergunta de acompanhamento: {question}"},
        ],
    )
    return resp.content[0].text.strip()


async def generate_dax(question: str, schema: list, dataset_name: str, history: list = None, report_name: str = None) -> str:
    schema_text = _schema_to_text(schema)

    all_measures = []
    for t in schema:
        for m in t.get("measures") or []:
            all_measures.append(f"[{m['name']}]")
    measures_hint = f"\nAvailable measures (use ONLY these): {', '.join(all_measures)}" if all_measures else ""
    report_hint = f"\nActive report: {report_name}" if report_name else ""

    system = f"""You are an expert Power BI DAX developer.
Dataset: {dataset_name}{report_hint}
{measures_hint}

Schema:
{schema_text}

{_DAX_RULES}"""

    messages = []
    for h in (history or [])[-6:]:
        messages.append({"role": h.role, "content": h.content})
    messages.append({"role": "user", "content": question})

    resp = await _get_client().messages.create(
        model=_MODEL,
        max_tokens=1024,
        system=system,
        messages=messages,
    )
    return resp.content[0].text.strip()


async def fix_dax(original_dax: str, error: str, question: str, schema: list, dataset_name: str) -> str:
    schema_text = _schema_to_text(schema)

    system = f"""You are an expert Power BI DAX developer. Fix the broken DAX query below.
Dataset: {dataset_name}

Schema:
{schema_text}

{_DAX_RULES}"""

    user = f"""The following DAX query returned an error. Fix it.

Original question: {question}

Broken DAX:
{original_dax}

Error:
{error}

Return ONLY the corrected DAX query."""

    resp = await _get_client().messages.create(
        model=_MODEL,
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return resp.content[0].text.strip()


_SQL_RULES = """
SQL Rules (follow strictly):
- Return ONLY the SQL SELECT query — no explanation, no markdown, no code fences
- SELECT only — never INSERT, UPDATE, DELETE, DROP, EXEC
- Use exact table and column names from the schema
- Always add TOP 500 (SQL Server) or LIMIT 500 (PostgreSQL/MySQL) to prevent large results
- For aggregations use GROUP BY with all non-aggregated columns
- For date filters use proper date literals for the dialect
- Never invent table or column names — use ONLY names from the schema
"""


def _schema_to_sql_text(schema: list) -> str:
    lines = []
    for table in schema:
        lines.append(f"Table: {table['name']}")
        for col in table.get("columns") or []:
            lines.append(f"  {col['name']} ({col.get('dataType', '')})")
    return "\n".join(lines)


async def generate_sql(question: str, schema: list, db_name: str, dialect: str = "mssql", history: list = None, report_name: str = None) -> str:
    schema_text = _schema_to_sql_text(schema)
    dialect_note = {
        "mssql": "SQL Server dialect — use TOP N instead of LIMIT",
        "postgresql": "PostgreSQL dialect — use LIMIT N",
        "mysql": "MySQL dialect — use LIMIT N",
    }.get(dialect, "Standard SQL")
    report_hint = f"\nActive report: {report_name}" if report_name else ""

    system = f"""You are an expert SQL developer.
Database: {db_name}{report_hint}
Dialect: {dialect_note}

Schema:
{schema_text}

{_SQL_RULES}"""

    messages = []
    for h in (history or [])[-6:]:
        messages.append({"role": h.role, "content": h.content})
    messages.append({"role": "user", "content": question})

    resp = await _get_client().messages.create(
        model=_MODEL, max_tokens=1024, system=system, messages=messages,
    )
    return resp.content[0].text.strip()


async def fix_sql(original_sql: str, error: str, question: str, schema: list, db_name: str, dialect: str = "mssql") -> str:
    schema_text = _schema_to_sql_text(schema)
    system = f"""You are an expert SQL developer. Fix the broken SQL query.
Database: {db_name}

Schema:
{schema_text}

{_SQL_RULES}"""

    user = f"""The following SQL query returned an error. Fix it.

Original question: {question}

Broken SQL:
{original_sql}

Error:
{error}

Return ONLY the corrected SQL query."""

    resp = await _get_client().messages.create(
        model=_MODEL, max_tokens=1024, system=system,
        messages=[{"role": "user", "content": user}],
    )
    return resp.content[0].text.strip()


async def format_answer(question: str, dax: str, rows: list, schema: list, history: list = None) -> str:
    schema_text = _schema_to_text(schema)
    preview = str(rows[:20])

    system = f"""Você é um assessor executivo de dados, apresentando análises diretas para a alta liderança.
Regras obrigatórias:
- Responda SEMPRE em português, de forma objetiva e executiva
- Use apenas os números exatos dos dados — nunca invente ou extrapole
- Estrutura: 1) resultado principal em 1-2 frases diretas, 2) tabela com os dados mais relevantes, 3) até 3 pontos de atenção ou oportunidades de decisão
- Sem termos técnicos (DAX, SQL, schema, dataset, query) — o leitor é gestor, não técnico
- Sem emojis, sem linguagem informal, sem introduções longas
- Destaque variações, concentrações ou anomalias que exijam atenção da liderança
- Se os dados forem insuficientes, diga claramente em linguagem de negócio

Schema:
{schema_text}"""

    history_ctx = ""
    if history:
        last = history[-4:]
        history_ctx = "\n\nConversa anterior:\n" + "\n".join(
            f"{'Usuário' if h.role == 'user' else 'IA'}: {h.content[:300]}" for h in last
        )

    user = f"""Pergunta: {question}{history_ctx}

Dados retornados ({len(rows)} registros, amostra de até 20):
{preview}"""

    resp = await _get_client().messages.create(
        model=_MODEL,
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return resp.content[0].text.strip()


# ── Databricks Environment Analysis ──────────────────────────────────────────

_DATA_ENGINEER_SYSTEM = """Você é um Senior Data Engineer — um par técnico, não um assistente. Você tem opiniões fortes sobre arquitetura de dados e aponta problemas diretamente. Você já se queimou com schemas ruins, pipelines instáveis e "gambiarras temporárias" que ficaram 3 anos em produção.

Personalidade: obcecado com confiabilidade, disciplinado com schemas, focado em throughput, direto quando necessário.
Estilo: técnico, sem rodeios — pule o preâmbulo, vá direto para os tradeoffs.
Idioma: sempre em Português (Brasil), exceto código e termos técnicos.

Ao analisar um ambiente Databricks:
- Comece pelo que está ERRADO e vai machucar em produção
- Quantifique o impacto quando possível
- Dê recomendações acionáveis com código concreto, não conselhos vagos
- Reconheça o que está bom (brevemente)

Estrutura obrigatória do relatório:
1. 🔴 Riscos Críticos (o que vai quebrar)
2. 🟡 Pontos de Atenção (o que vai degradar)
3. ✅ O que está sólido
4. 🏗️ Arquitetura de Dados (camadas, formatos, organização)
5. ⚙️ Clusters & Custo (configuração, eficiência)
6. 🔄 Jobs & Pipelines (confiabilidade, taxa de falha)
7. 📋 5 Ações Prioritárias (ordenadas por impacto)

Non-negotiables a verificar:
- Idempotência dos pipelines
- Contratos de schema com drift alerting
- Observabilidade: latência, freshness, null rates, row counts
- Custo de cluster (fixo vs efêmero)
- Organização Bronze/Silver/Gold
- Taxa de falha dos jobs e alertas"""


def _build_env_summary(env_data: dict) -> str:
    clusters = env_data.get("clusters", [])
    jobs = env_data.get("jobs", [])
    catalogs = env_data.get("catalogs", [])
    tables = env_data.get("tables_sample", [])
    schemas_by_catalog = env_data.get("schemas_by_catalog", {})

    lines = [f"## Workspace: {env_data.get('workspace_host')}", ""]

    lines.append(f"### Clusters ({len(clusters)} total)")
    for c in clusters:
        autoscale_str = "autoscale=sim" if c.get("autoscale") else f"workers={c.get('num_workers', '?')}"
        lines.append(
            f"- {c['name']} | state={c['state']} | runtime={c['runtime']} "
            f"| node={c['node_type']} | {autoscale_str} | source={c.get('cluster_source', '?')}"
        )

    lines += ["", f"### Jobs/Pipelines ({len(jobs)} total)"]
    for j in jobs:
        runs = j.get("recent_runs", [])
        if runs:
            failed = sum(1 for r in runs if r.get("state") in ("FAILED", "TIMEDOUT", "MAXIMUM_CONCURRENT_RUNS_REACHED"))
            success_rate = f"{round((1 - failed / len(runs)) * 100)}% ({len(runs)} runs)"
        else:
            success_rate = "sem histórico"
        sched = j.get("schedule") or "manual/trigger"
        paused = " [PAUSADO]" if j.get("pause_status") == "PAUSED" else ""
        lines.append(f"- {j['name']}{paused} | schedule={sched} | success_rate={success_rate}")

    lines += ["", f"### Unity Catalog ({len(catalogs)} catálogos)"]
    if catalogs:
        for cat_name, schemas in schemas_by_catalog.items():
            schema_names = ", ".join(s["name"] for s in schemas[:10])
            lines.append(f"- Catálogo: {cat_name} | {len(schemas)} schemas: {schema_names}")
    else:
        lines.append("- Unity Catalog não disponível (workspace legado / Hive Metastore)")

    lines += ["", f"### Tabelas amostradas ({len(tables)} tabelas)"]
    for t in tables[:40]:
        fmt = t.get("data_source_format") or "?"
        ttype = t.get("table_type") or "?"
        lines.append(f"- {t['full_name']} | format={fmt} | type={ttype} | cols={t['column_count']}")

    return "\n".join(lines)


async def analyze_databricks_environment(env_data: dict):
    """Async generator: stream data-engineer analysis of a Databricks workspace."""
    summary = _build_env_summary(env_data)
    user_prompt = (
        f"Analise este ambiente Databricks e produza um relatório técnico completo:\n\n"
        f"{summary}\n\n"
        "Seja direto. Comece pelo que está errado e vai machucar em produção. "
        "Depois o que está bom. Termine com as 5 ações prioritárias ordenadas por impacto."
    )

    async with _get_client().messages.stream(
        model=_MODEL,
        max_tokens=4096,
        system=_DATA_ENGINEER_SYSTEM,
        messages=[{"role": "user", "content": user_prompt}],
    ) as stream:
        async for text in stream.text_stream:
            yield text


# ── Microsoft Fabric Environment Analysis ─────────────────────────────────────

_FABRIC_ENGINEER_SYSTEM = """Você é um Senior Data Engineer especializado em Microsoft Fabric e arquitetura de dados na plataforma Azure/Microsoft. Você é um par técnico, não um assistente. Tem opiniões fortes e aponta problemas diretamente.

Especialidades no Fabric:
- Delta Lake como formato nativo (vs Iceberg em outros)
- Lakehouse vs Warehouse: quando usar cada um
- OneLake como camada de armazenamento unificado e Shortcuts
- Direct Lake mode para Semantic Models (vs Import vs DirectQuery)
- Medallion Architecture no contexto do Fabric
- Data Pipeline vs Dataflow Gen2 vs Notebook: casos de uso corretos
- Capacidades (SKU F/P/E) e otimização de CU

Personalidade: direto, técnico, sem rodeios.
Idioma: sempre em Português (Brasil), exceto código e termos técnicos.

Estrutura obrigatória do relatório:
1. 🔴 Riscos Críticos (o que vai quebrar ou gerar custo inesperado)
2. 🟡 Pontos de Atenção (antipadrões, decisões arquiteturais questionáveis)
3. ✅ O que está sólido
4. 🏗️ Arquitetura de Dados (organização de workspaces, Lakehouses, Warehouses, camadas)
5. 🔄 Pipelines & Notebooks (cobertura de automação, gaps)
6. 💰 Capacidade & Custo (uso de CU, itens que consomem capacidade desnecessariamente)
7. 📋 5 Ações Prioritárias (ordenadas por impacto)

Non-negotiables a verificar no Fabric:
- Separação de workspaces por ambiente (dev/homolog/prod)
- Lakehouses com estrutura Medallion (Bronze/Silver/Gold) vs tudo em um lakehouse só
- Tabelas Delta com Z-Order e vacuum configurados
- Pipelines cobrindo todos os Lakehouses (sem lakehouse órfão sem pipeline)
- Notebooks sendo usados como pipelines ad-hoc (antipadrão)
- Direct Lake vs Import: semantic models sem Direct Lake onde deveria ter
- Shortcut vs cópia física: dados duplicados no OneLake"""


def _build_fabric_summary(env_data: dict) -> str:
    workspaces = env_data.get("workspaces", [])
    total_ws = env_data.get("total_workspaces", len(workspaces))

    lines = [
        f"## Tenant Fabric",
        f"Total de workspaces no tenant: {total_ws} ({len(workspaces)} analisados)",
        "",
    ]

    for ws in workspaces:
        item_counts = ws.get("item_counts", {})
        lakehouses = ws.get("lakehouses", [])
        warehouses = ws.get("warehouses", [])
        pipelines = ws.get("pipelines", [])
        notebooks = ws.get("notebooks", [])

        counts_str = ", ".join(f"{k}={v}" for k, v in sorted(item_counts.items()))
        lines.append(f"### Workspace: {ws['name']} (type={ws.get('type', '?')})")
        lines.append(f"  Itens: {counts_str or 'vazio'}")

        if lakehouses:
            lines.append(f"  Lakehouses ({len(lakehouses)}):")
            for lh in lakehouses:
                tc = lh.get("table_count")
                tc_str = str(tc) if tc is not None else "não consultado"
                lines.append(f"    - {lh['name']} | tabelas={tc_str} | sql_endpoint={'sim' if lh.get('sql_endpoint') else 'não'}")
                for t in lh.get("tables", [])[:15]:
                    lines.append(f"      · {t['name']} | format={t.get('format', '?')} | type={t.get('type', '?')}")

        if warehouses:
            lines.append(f"  Warehouses ({len(warehouses)}): {', '.join(w['name'] for w in warehouses)}")

        if pipelines:
            lines.append(f"  Pipelines ({len(pipelines)}): {', '.join(p['name'] for p in pipelines[:10])}")
        else:
            lines.append(f"  Pipelines: nenhum")

        if notebooks:
            lines.append(f"  Notebooks ({len(notebooks)}): {', '.join(n['name'] for n in notebooks[:8])}")

        lines.append("")

    return "\n".join(lines)


async def analyze_fabric_environment(env_data: dict):
    """Async generator: stream data-engineer analysis of a Microsoft Fabric tenant."""
    summary = _build_fabric_summary(env_data)
    user_prompt = (
        f"Analise este ambiente Microsoft Fabric e produza um relatório técnico completo:\n\n"
        f"{summary}\n\n"
        "Seja direto. Comece pelos riscos críticos e antipadrões que vão machucar em produção ou gerar custo inesperado. "
        "Depois o que está bom. Termine com as 5 ações prioritárias ordenadas por impacto."
    )

    async with _get_client().messages.stream(
        model=_MODEL,
        max_tokens=4096,
        system=_FABRIC_ENGINEER_SYSTEM,
        messages=[{"role": "user", "content": user_prompt}],
    ) as stream:
        async for text in stream.text_stream:
            yield text


# ── Data Maturity Assessment (DAMA-DMBOK) ─────────────────────────────────────

_DMBOK_SYSTEM = """Você é um especialista sênior em gestão de dados, com profundo conhecimento no framework DAMA-DMBOK (Data Management Body of Knowledge).

Sua função é avaliar o nível de maturidade de dados de uma empresa com base nas disciplinas do DMBOK.

Considere os seguintes domínios:
1. Governança de Dados
2. Arquitetura de Dados
3. Modelagem e Design de Dados
4. Armazenamento e Operações de Dados
5. Segurança de Dados
6. Integração e Interoperabilidade de Dados
7. Documentação e Metadados
8. Qualidade de Dados
9. Data Warehousing e BI

Escala de maturidade:
1 = Inicial / Ad hoc
2 = Repetível
3 = Definido
4 = Gerenciado
5 = Otimizado

Estágios de maturidade organizacional:
- Operacional: baixo controle de dados, processos reativos, sem governança formal
- Estruturando: organização iniciando, primeiros processos definidos, governança incipiente
- Orientado a dados: uso consistente de dados para decisão, processos estruturados, governança ativa
- Data-driven: dados como ativo estratégico, cultura orientada a dados, governança madura

Com base nos dados fornecidos, gere:
0. Classificação do estágio organizacional — coloque em destaque logo no início:
   - Indique claramente em qual estágio a empresa se encontra (Operacional / Estruturando / Orientado a dados / Data-driven)
   - Explique de forma objetiva os principais motivos que justificam essa classificação
1. Score geral de maturidade (média ponderada) e classificação
2. Análise detalhada por domínio do DMBOK
3. Diagnóstico executivo (visão estratégica)
4. Principais riscos associados ao nível atual
5. Principais gaps em relação às boas práticas do DMBOK
6. Plano de ação estruturado:
   - Curto prazo (30 dias)
   - Médio prazo (90 dias)
   - Longo prazo (6 a 12 meses)
7. Recomendações práticas (processos, governança, tecnologia)
8. Nível de aderência ao DMBOK (% estimado)
9. Benchmark de mercado (baixo, médio, alto)
10. Nível de urgência (Baixo, Médio, Alto, Crítico)

Regras:
- Comece SEMPRE pela classificação do estágio organizacional antes de qualquer outra seção
- Use linguagem consultiva e executiva
- Baseie-se nas melhores práticas do DMBOK
- Evite respostas genéricas
- Traga recomendações práticas e aplicáveis
- Pense como um consultor contratado para evoluir a empresa
- Responda sempre em Português (Brasil)"""


async def analyze_data_maturity(domains: dict):
    """Async generator: stream DAMA-DMBOK maturity assessment."""
    user_prompt = f"""Avalie a maturidade de dados desta empresa com base nas informações abaixo sobre cada domínio DMBOK:

Governança de Dados: {domains.get('governanca', 'Não informado')}
Arquitetura de Dados: {domains.get('arquitetura', 'Não informado')}
Modelagem e Design de Dados: {domains.get('modelagem', 'Não informado')}
Armazenamento e Operações de Dados: {domains.get('armazenamento', 'Não informado')}
Segurança de Dados: {domains.get('seguranca', 'Não informado')}
Integração e Interoperabilidade de Dados: {domains.get('integracao', 'Não informado')}
Documentação e Metadados: {domains.get('metadados', 'Não informado')}
Qualidade de Dados: {domains.get('qualidade', 'Não informado')}
Data Warehousing e BI: {domains.get('bi', 'Não informado')}

Produza um relatório completo de maturidade seguindo a estrutura definida. Seja específico, consultivo e orientado a ação."""

    async with _get_client().messages.stream(
        model=_MODEL,
        max_tokens=6000,
        system=_DMBOK_SYSTEM,
        messages=[{"role": "user", "content": user_prompt}],
    ) as stream:
        async for text in stream.text_stream:
            yield text
