const { powerBiRequest } = require('./client');

async function getWorkspaces() {
  const result = await powerBiRequest('GET', '/groups');
  return result.value || [];
}

module.exports = { getWorkspaces };
