const express = require('express');
const router = express.Router();
const { supabase } = require('../supabase/client');
const { getDatasetSchema } = require('../supabase/metadata');
const { generateDaxQuery, formatAnswer } = require('../ai/openai');
const { executeQuery } = require('../powerbi/datasets');

router.post('/', async (req, res) => {
  const { question, datasetId } = req.body;

  if (!question || !datasetId) {
    return res.status(400).json({ error: 'question and datasetId are required' });
  }

  try {
    const { data: dataset } = await supabase
      .from('datasets')
      .select('name')
      .eq('id', datasetId)
      .single();

    const schema = await getDatasetSchema(datasetId);

    if (!schema.length) {
      return res.status(400).json({
        error: 'No schema found for this dataset. Please sync metadata first.',
      });
    }

    const daxQuery = await generateDaxQuery(question, schema, dataset?.name || datasetId);

    let rows = [];
    let queryError = null;

    try {
      rows = await executeQuery(datasetId, daxQuery);
    } catch (err) {
      queryError = err.response?.data?.error?.message || err.message;
    }

    let answer;
    if (queryError) {
      answer = `I generated the following DAX query but encountered an error executing it:\n\n\`\`\`dax\n${daxQuery}\n\`\`\`\n\nError: ${queryError}\n\nPlease check if the query syntax is correct for your dataset.`;
    } else {
      answer = await formatAnswer(question, daxQuery, rows, schema);
    }

    res.json({
      question,
      daxQuery,
      rows,
      answer,
      rowCount: rows.length,
    });
  } catch (err) {
    console.error('Chat error:', err);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
