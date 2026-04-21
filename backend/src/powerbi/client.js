const axios = require('axios');
const { getAccessToken } = require('../auth/entraAuth');

const BASE_URL = 'https://api.powerbi.com/v1.0/myorg';

async function powerBiRequest(method, path, data = null) {
  const token = await getAccessToken();
  const response = await axios({
    method,
    url: `${BASE_URL}${path}`,
    data,
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
  });
  return response.data;
}

module.exports = { powerBiRequest };
