const { powerBiRequest } = require('./client');

async function getDatasets(workspaceId) {
  const result = await powerBiRequest('GET', `/groups/${workspaceId}/datasets`);
  return result.value || [];
}

async function getTables(datasetId) {
  const result = await powerBiRequest('GET', `/datasets/${datasetId}/tables`);
  return result.value || [];
}

async function executeQuery(datasetId, daxQuery) {
  const result = await powerBiRequest('POST', `/datasets/${datasetId}/executeQueries`, {
    queries: [{ query: daxQuery }],
    serializerSettings: { includeNulls: true },
  });
  return result.results?.[0]?.tables?.[0]?.rows || [];
}

module.exports = { getDatasets, getTables, executeQuery };
