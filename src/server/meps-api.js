/**
 * Fetches MEPs from the Europarl data API.
 * - fetchAllMeps: current term only (backward compatible).
 * - fetchAllMepsFromTerm5: all MEPs from term 5 onwards (1999), merged by person; used for Build MEP Dataset.
 */
const axios = require('axios');
const { API_BASE } = require('./config');
const { createProgressBar } = require('./progress');

const USER_AGENT = 'EUROWATCH-dev-1.0';

/**
 * Paginate one endpoint and return all items.
 */
async function paginateMepRequest(url, params, log = console.log) {
  const limit = 500;
  let offset = 0;
  let all = [];
  while (true) {
    const response = await axios.get(url, {
      params: { ...params, format: 'application/ld+json', limit, offset },
      headers: { Accept: 'application/ld+json', 'User-Agent': USER_AGENT }
    });
    const data = (response.data && response.data.data) || [];
    all = all.concat(data);
    if (data.length < limit) break;
    offset += limit;
  }
  return all;
}

/**
 * Fetch current-term MEPs only (show-current). Used by init and refresh-meps.
 */
async function fetchAllMeps(lang = 'EN') {
  console.log('Starting MEP fetch...');
  const list = await fetchCurrentTermMeps(lang);
  console.log(`\nTotal MEPs fetched: ${list.length}`);
  return list;
}

/**
 * Internal: paginate show-current only, no extra logging.
 */
async function fetchCurrentTermMeps(lang = 'EN') {
  const limit = 500;
  let offset = 0;
  let allMeps = [];
  let estimatedTotal = 0;
  let firstBatch = true;
  const mepStartTime = Date.now();

  while (true) {
    const response = await axios.get(`${API_BASE}/meps/show-current`, {
      params: { language: lang, format: 'application/ld+json', limit, offset },
      headers: { Accept: 'application/ld+json', 'User-Agent': USER_AGENT }
    });
    const meps = (response.data && response.data.data) || [];
    allMeps = allMeps.concat(meps);

    if (firstBatch && meps.length > 0) {
      const meta = response.data.meta;
      if (meta && meta.total) {
        estimatedTotal = meta.total;
        console.log(`Estimated total MEPs: ${estimatedTotal}`);
      }
      firstBatch = false;
    }

    if (estimatedTotal > 0) {
      const progressBar = createProgressBar(allMeps.length, estimatedTotal, 30);
      const rate = allMeps.length / ((Date.now() - mepStartTime) / 1000);
      process.stdout.write(`\r${progressBar} | Rate: ${rate.toFixed(1)}/sec`);
    } else {
      console.log(`Fetched ${meps.length} MEPs (total: ${allMeps.length})`);
    }

    if (meps.length < limit) {
      if (estimatedTotal > 0) console.log('\nReached end of MEP data');
      break;
    }
    offset += limit;
  }

  return allMeps;
}

/**
 * Fetch MEPs for a single parliamentary term (5 = 1999, 10 = current).
 * Uses API list endpoint with parliamentary-term filter.
 */
async function fetchMepsForTerm(term, lang = 'EN') {
  const url = `${API_BASE}/meps`;
  const list = await paginateMepRequest(url, {
    language: lang,
    'parliamentary-term': term
  });
  return list;
}

/**
 * Fetch all MEPs from term 5 onwards (1999), merge by identifier.
 * Current-term (10) data wins for label/country/group; is_current true only for term-10.
 */
async function fetchAllMepsFromTerm5(lang = 'EN') {
  const mepStartTime = Date.now();
  console.log('Starting MEP fetch (term 5 to current)...');

  // 1) Current term (10)
  const current = await fetchCurrentTermMeps(lang);
  const byId = new Map();
  for (const m of current) {
    byId.set(m.identifier, { ...m, is_current: true });
  }
  console.log(`\nCurrent term: ${current.length} MEPs`);

  // 2) Historic terms 5–9 in parallel
  const historicTerms = [5, 6, 7, 8, 9];
  console.log('Fetching terms 5–9 in parallel...');
  const termResults = await Promise.allSettled(
    historicTerms.map((term) => fetchMepsForTerm(term, lang))
  );
  termResults.forEach((result, idx) => {
    const term = historicTerms[idx];
    if (result.status === 'fulfilled' && result.value.length) {
      for (const m of result.value) {
        const id = m.identifier;
        if (!id || byId.has(id)) continue;
        byId.set(id, { ...m, is_current: false });
      }
      console.log(`  Term ${term}: ${result.value.length} MEPs`);
    } else if (result.status === 'rejected') {
      console.warn(`  Term ${term} failed: ${result.reason?.message || result.reason}`);
    }
  });
  console.log(`  Total unique (with current): ${byId.size}`);

  const merged = Array.from(byId.values());
  const mepTime = (Date.now() - mepStartTime) / 1000;
  console.log(`\nTotal MEPs (term 5–current): ${merged.length} in ${mepTime.toFixed(1)}s (${merged.filter(m => m.is_current).length} current)`);
  return merged;
}

module.exports = { fetchAllMeps, fetchAllMepsFromTerm5, fetchMepsForTerm };
