/**
 * One-off test: fetch MEPs from historic vs current-term and log raw API response
 * to see if country/party are present.
 * Run: node src/scripts/test-historic-mep-api.js
 */
const axios = require('axios');
const API_BASE = 'https://data.europarl.europa.eu/api/v2';
const TERM = 9;
const LIMIT = 2;

async function main() {
  // --- Historic: /meps?parliamentary-term=N ---
  console.log(`\n=== Historic term ${TERM}: GET /meps?parliamentary-term=${TERM} (limit=${LIMIT}) ===\n`);
  const historicRes = await axios.get(`${API_BASE}/meps`, {
    params: { language: 'EN', 'parliamentary-term': TERM, format: 'application/ld+json', limit: LIMIT, offset: 0 },
    headers: { Accept: 'application/ld+json', 'User-Agent': 'EUROWATCH-test-1.0' }
  });
  const historicData = (historicRes.data && historicRes.data.data) || [];
  if (historicData.length) {
    const first = historicData[0];
    console.log('Keys:', Object.keys(first).sort().join(', '));
    console.log('Sample:', JSON.stringify(first, null, 2));
  }

  // --- Current: /meps/show-current ---
  console.log(`\n=== Current term: GET /meps/show-current (limit=${LIMIT}) ===\n`);
  const currentRes = await axios.get(`${API_BASE}/meps/show-current`, {
    params: { language: 'EN', format: 'application/ld+json', limit: LIMIT, offset: 0 },
    headers: { Accept: 'application/ld+json', 'User-Agent': 'EUROWATCH-test-1.0' }
  });
  const currentData = (currentRes.data && currentRes.data.data) || [];
  if (currentData.length) {
    const first = currentData[0];
    console.log('Keys:', Object.keys(first).sort().join(', '));
    console.log('Sample (country/party):', {
      'api:country-of-representation': first['api:country-of-representation'],
      'api:political-group': first['api:political-group']
    });
  }

  console.log('\nDone.');
}

main().catch(e => { console.error(e.message); process.exit(1); });
