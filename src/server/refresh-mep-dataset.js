/**
 * Build the best possible MEP dataset from API + speeches.
 * CLEAN refresh: wipe MEP data first, then rebuild from API + link + historic + normalizer.
 * 0. Clear all MEP data (unlink speeches, delete meps).
 * 1. Fetch MEPs from API (term 5 to current).
 * 2. Insert API MEPs.
 * 3. Link speeches to existing MEPs by name.
 * 4. Create one historic MEP per remaining speaker (one per person).
 * 5. Run group normalizer on individual_speeches (political_group_std).
 */
const { execFile } = require('child_process');
const path = require('path');

const { fetchAllMepsFromTerm5 } = require('./meps-api');
const historicMeps = require('./historic-meps');
const { syncMepAffiliationsFromSpeeches } = require('./sync-mep-affiliations');

/**
 * Clear all MEP data: unlink speeches then delete meps table.
 */
function clearAllMepData(db) {
  return new Promise((resolve, reject) => {
    db.run('UPDATE individual_speeches SET mep_id = NULL', function (err) {
      if (err) return reject(err);
      const unlinked = this.changes;
      db.run('DELETE FROM meps', function (err2) {
        if (err2) return reject(err2);
        resolve({ unlinked: unlinked, mepsDeleted: this.changes });
      });
    });
  });
}

/**
 * Run the group-normalizer script with --apply (same DB).
 * @param {string} projectRoot - Path to project root
 * @returns {Promise<void>}
 */
function runGroupNormalizer(projectRoot, log = console.log) {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(projectRoot, 'src', 'core', 'group-normalizer.js');
    log('[MEP-DATASET] Running group normalizer...');
    execFile('node', [scriptPath, '--apply'], { cwd: projectRoot, maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
      if (stdout) log(stdout);
      if (stderr) console.error(stderr);
      if (err) {
        reject(err);
        return;
      }
      log('[MEP-DATASET] Group normalizer finished.');
      resolve();
    });
  });
}

/**
 * Run the full MEP dataset refresh.
 * @param {object} db - SQLite3 database instance
 * @param {{ log?: function, projectRoot?: string }} options - log function; projectRoot for group-normalizer (default: parent of src)
 * @returns {Promise<{ apiMeps: number, linked: number, createdHistoric: number, linkedSpeeches: number }>}
 */
async function runRefreshMepDataset(db, options = {}) {
  const log = options.log || console.log;
  const projectRoot = options.projectRoot || path.resolve(__dirname, '..', '..');

  log('[MEP-DATASET] Step 0: Clearing all MEP data (clean refresh)...');
  const clearResult = await clearAllMepData(db);
  log(`[MEP-DATASET] Cleared: ${clearResult.unlinked} speeches unlinked, ${clearResult.mepsDeleted} MEPs removed.`);

  log('[MEP-DATASET] Step 1: Fetching MEPs from API (term 5 to current)...');
  const mepsFromApi = await fetchAllMepsFromTerm5();
  log(`[MEP-DATASET] Fetched ${mepsFromApi.length} MEPs (term 5 onwards).`);

  log('[MEP-DATASET] Step 2: Inserting API MEPs...');
  const apiCount = await new Promise((resolve, reject) => {
    historicMeps.upsertApiMeps(db, mepsFromApi).then(resolve).catch(reject);
  });
  log(`[MEP-DATASET] Inserted ${apiCount} API MEPs.`);

  const linked = await historicMeps.linkSpeechesToMeps(db, log);
  log(`[MEP-DATASET] Linked ${linked} speaker names to existing MEPs.`);

  const historicResult = await historicMeps.createHistoricMepsOnePerPerson(db, log);
  log(`[MEP-DATASET] Created ${historicResult.createdHistoricMeps} historic MEPs, linked ${historicResult.linkedSpeeches} speeches.`);

  log('[MEP-DATASET] Step 5: Normalizing political groups (group-normalizer)...');
  await runGroupNormalizer(projectRoot, log);

  log('[MEP-DATASET] Step 6: Syncing MEP affiliations from speeches to meps.politicalGroup...');
  const syncResult = await syncMepAffiliationsFromSpeeches(db, { log });
  log(`[MEP-DATASET] Synced affiliations for ${syncResult.updated} MEPs.`);

  return {
    cleared: clearResult,
    apiMeps: apiCount,
    linked,
    createdHistoric: historicResult.createdHistoricMeps,
    linkedSpeeches: historicResult.linkedSpeeches,
    syncedMeps: syncResult.updated
  };
}

module.exports = { runRefreshMepDataset, runGroupNormalizer };
