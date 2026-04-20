import os
import anthropic

client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-opus-4-7")

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
    resp = await client.messages.create(
        model=_MODEL,
        max_tokens=1024,
        system="Você é um analista de dados. Responda a pergunta do usuário com base apenas na resposta imediatamente anterior da IA. Seja direto e conciso. Responda em português.",
        messages=[
            {"role": "user", "content": f"Resposta anterior da IA:\n{history_text}\n\nPergunta de acompanhamento: {question}"},
        ],
    )
    return resp.content[0].text.strip()


async def generate_dax(question: str, schema: list, dataset_name: str, history: list = None) -> str:
    schema_text = _schema_to_text(schema)

    all_measures = []
    for t in schema:
        for m in t.get("measures") or []:
            all_measures.append(f"[{m['name']}]")
    measures_hint = f"\nAvailable measures (use ONLY these): {', '.join(all_measures)}" if all_measures else ""

    system = f"""You are an expert Power BI DAX developer.
Dataset: {dataset_name}
{measures_hint}

Schema:
{schema_text}

{_DAX_RULES}"""

    messages = []
    for h in (history or [])[-6:]:
        messages.append({"role": h.role, "content": h.content})
    messages.append({"role": "user", "content": question})

    resp = await client.messages.create(
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

    resp = await client.messages.create(
        model=_MODEL,
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return resp.content[0].text.strip()


async def format_answer(question: str, dax: str, rows: list, schema: list, history: list = None) -> str:
    schema_text = _schema_to_text(schema)
    preview = str(rows[:20])

    system = f"""Você é um analista de dados sênior apresentando resultados para um executivo.
Regras obrigatórias:
- Responda SEMPRE em português, de forma direta e profissional
- Use apenas os números exatos presentes nos dados — nunca invente ou extrapole
- Estrutura: 1) resultado principal em 1-2 frases, 2) tabela com os dados relevantes, 3) até 3 pontos de atenção ou oportunidades de decisão
- Sem emojis, sem linguagem informal, sem introduções longas
- Destaque variações, concentrações ou anomalias que exijam atenção executiva
- Se os dados forem insuficientes para uma conclusão, diga claramente

Schema:
{schema_text}"""

    history_ctx = ""
    if history:
        last = history[-4:]
        history_ctx = "\n\nConversa anterior:\n" + "\n".join(
            f"{'Usuário' if h.role == 'user' else 'IA'}: {h.content[:300]}" for h in last
        )

    user = f"""Pergunta: {question}{history_ctx}

DAX executado:
{dax}

Resultado ({len(rows)} linhas, mostrando até 20):
{preview}"""

    resp = await client.messages.create(
        model=_MODEL,
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return resp.content[0].text.strip()
