const express = require('express');
const router = express.Router();
const { supabase } = require('../supabase/client');
const { getDatasetSchema } = require('../supabase/metadata');

router.get('/:workspaceId', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('datasets')
      .select('*')
      .eq('workspace_id', req.params.workspaceId)
      .order('name');

    if (error) throw error;
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/:datasetId/schema', async (req, res) => {
  try {
    const schema = await getDatasetSchema(req.params.datasetId);
    res.json(schema);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
