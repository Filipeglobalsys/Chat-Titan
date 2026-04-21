const Anthropic = require('@anthropic-ai/sdk');

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

function buildSchemaContext(schema) {
  return schema
    .map((table) => {
      const cols = table.columns
        .map((c) => `    - ${c.name} (${c.data_type})`)
        .join('\n');
      const measures = table.measures
        .map((m) => {
          let line = `    - [${m.name}]`;
          if (m.description) line += `: ${m.description}`;
          if (m.expression) line += `\n      Expression: ${m.expression}`;
          return line;
        })
        .join('\n');

      return [
        `Table: ${table.name}`,
        cols ? `  Columns:\n${cols}` : '',
        measures ? `  Measures:\n${measures}` : '',
      ]
        .filter(Boolean)
        .join('\n');
    })
    .join('\n\n');
}

async function generateDaxQuery(question, schema, datasetName) {
  const schemaContext = buildSchemaContext(schema);

  const systemPrompt = `You are an expert Power BI DAX developer.
Given the schema of a Power BI dataset, generate a valid DAX query to answer the user's question.

Dataset: ${datasetName}

Schema:
${schemaContext}

Rules:
- Return ONLY the DAX query, no explanation, no markdown code blocks
- Use EVALUATE to return a table result
- Column references must use 'TableName'[ColumnName] syntax
- Measure references use [MeasureName] syntax
- Always order results when relevant using ORDER BY
- Limit results to 1000 rows maximum using TOPN when the query could return many rows

CRITICAL DAX RULES - follow exactly:
- NEVER wrap SUMMARIZECOLUMNS inside CALCULATETABLE — this is a DAX anti-pattern that breaks filter context and returns wrong results
- When filtering SUMMARIZECOLUMNS, add filter tables as arguments directly inside SUMMARIZECOLUMNS, like:
  EVALUATE
  SUMMARIZECOLUMNS(
      Table[GroupColumn],
      FILTER(ALL(Table[FilterCol1], Table[FilterCol2]), Table[FilterCol1] = value1 && Table[FilterCol2] = value2),
      "Measure Name", [MeasureName]
  )
- Use KEEPFILTERS() when you need to preserve existing filter context inside SUMMARIZECOLUMNS
- If you need CALCULATE for a single aggregation without grouping, use EVALUATE ROW("name", CALCULATE(..., filter))
- Never use SUMMARIZECOLUMNS inside CALCULATE or CALCULATETABLE`;

  const response = await anthropic.messages.create({
    model: process.env.ANTHROPIC_MODEL || 'claude-opus-4-7',
    max_tokens: 1024,
    system: systemPrompt,
    messages: [{ role: 'user', content: question }],
  });

  return response.content[0].text.trim();
}

async function formatAnswer(question, daxQuery, rows, schema) {
  const schemaContext = buildSchemaContext(schema);
  const dataPreview = JSON.stringify(rows.slice(0, 20), null, 2);

  const systemPrompt = `You are a helpful Power BI analyst assistant. Answer in the same language as the user's question.
The user asked a question, a DAX query was executed, and you received the exact results below.

CRITICAL RULES:
- Use ONLY the exact numbers from the query results provided. NEVER invent, estimate, or hallucinate values.
- List each row/category with its exact value from the results.
- Do NOT summarize or generalize if the data has distinct categories — show each one.
- If the data is empty, say so clearly.
- Do not add insights or explanations beyond what the data shows.

Dataset schema:
${schemaContext}`;

  const userMessage = `Question: ${question}

DAX Query executed:
${daxQuery}

Query results (first 20 rows):
${dataPreview}

Total rows returned: ${rows.length}`;

  const response = await anthropic.messages.create({
    model: process.env.ANTHROPIC_MODEL || 'claude-opus-4-7',
    max_tokens: 1024,
    system: systemPrompt,
    messages: [{ role: 'user', content: userMessage }],
  });

  return response.content[0].text.trim();
}

module.exports = { generateDaxQuery, formatAnswer };
