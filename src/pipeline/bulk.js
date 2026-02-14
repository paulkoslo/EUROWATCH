/**
 * Bulk pipeline: process many sittings with fetch/parse and AI classification in parallel.
 * Env: FETCH_CONCURRENCY, AI_WORKERS, TOPIC_BATCH_SIZE
 */

process.env.DOTENV_CONFIG_QUIET = '1';
require('dotenv').config();

const sqlite3 = require('sqlite3').verbose();
const { fetchSittingHTML, listDatesInRange, filterDatesNotInDb, filterDatesNeedingProcessing, getSittingHtmlFromDb } = require('../core/parliament-fetch');
const { parseSitting } = require('../scripts/step-3-parse-sitting');
const { classifyBatch, BATCH_SIZE } = require('../core/topic-agent');
const { load: loadMacroTopics, addAllIfNew: addMacroTopics } = require('../core/macro-topics');
const { run: storeSitting } = require('../scripts/step-5-store-sitting');
const { DB_PATH } = require('../core/db');

const FETCH_CONCURRENCY = parseInt(process.env.FETCH_CONCURRENCY || '20', 10);
const AI_WORKERS = parseInt(process.env.AI_WORKERS || process.env.TOPIC_POOL_SIZE || '50', 10);
const path = require('path');
const fs = require('fs');

const FAILURES_LOG = path.join(__dirname, '..', 'data', 'bulk-failures.log');

function appendFailureLog(type, dateOrId, err) {
  try {
    const line = `[${new Date().toISOString()}] ${type} ${dateOrId}: ${(err && err.message) || err}\n`;
    fs.appendFileSync(FAILURES_LOG, line);
  } catch (_) {}
}

function semaphore(max) {
  let count = 0;
  const waiters = [];
  return {
    async acquire() {
      if (count < max) { count++; return; }
      await new Promise(r => waiters.push(r));
      count++;
    },
    release() {
      count--;
      if (waiters.length > 0) waiters.shift()();
    }
  };
}

async function runBulk(options = {}) {
  const log = options.log || console.log;
  const { startDate, endDate, skipExisting = true, includeUnclassified = false, db: externalDb } = options;

  if (!startDate || !endDate) {
    throw new Error('startDate and endDate required (YYYY-MM-DD)');
  }

  const db = externalDb || new sqlite3.Database(DB_PATH);
  const closeDb = !externalDb;
  if (externalDb) {
    log('[REFRESH] Using existing database connection (no lock contention).');
  }

  try {
    log(`[REFRESH] Date range: ${startDate} → ${endDate}`);
    log(`[REFRESH] Workers: ${AI_WORKERS}, Fetch concurrency: ${FETCH_CONCURRENCY}`);

    const allDates = listDatesInRange(startDate, endDate);
    let datesToProcess = includeUnclassified
      ? await filterDatesNeedingProcessing(allDates, db)
      : (skipExisting ? await filterDatesNotInDb(allDates, db) : allDates);
    const alreadyDone = allDates.length - datesToProcess.length;
    log(`[REFRESH] New sittings to fetch and store: ${datesToProcess.length} dates. Already in DB for this range: ${alreadyDone} dates.`);

    if (datesToProcess.length === 0) {
      log('[REFRESH] No new sittings to process.');
      return { processed: 0, failed: 0, fetchSkipped: 0, aiFailed: 0, pending: 0 };
    }

    const taskQueue = [];
    let producerDone = false;
    const sittingState = new Map();
    const fetchSem = semaphore(FETCH_CONCURRENCY);
    let processed = 0;
    let failed = 0;
    let fetchSkipped = 0;
    let aiFailed = 0;
    const storePromises = [];

    function onSittingComplete(sittingId) {
      const state = sittingState.get(sittingId);
      if (!state || state.stored) return;
      const { sitting, topicTitles, batchResults } = state;
      const numBatches = Math.ceil(topicTitles.length / BATCH_SIZE);
      if (Object.keys(batchResults).length !== numBatches) return;

      const results = [];
      for (let i = 0; i < numBatches; i++) results.push(...(batchResults[i] || []));
      const topicMap = {};
      for (const r of results) {
        topicMap[r.topic] = { macro_topic: r.macro_topic, specific_focus: r.specific_focus, confidence: r.confidence };
      }

      state.stored = true;
      log(`  [REFRESH] Storing sitting ${sitting.date} (${sitting.speeches.length} speeches)...`);
      const p = storeSitting({ ...sitting, topicMap }, { log: () => {}, db, replaceExisting: !!state.replaceExisting })
        .then(() => {
          processed++;
          log(`  [REFRESH] Stored sitting ${sitting.date} (${sitting.speeches.length} speeches).`);
        })
        .catch(err => {
          failed++;
          log(`  [REFRESH] Store failed for ${sittingId}: ${err.message}`);
          appendFailureLog('STORE', sittingId, err);
        });
      storePromises.push(p);
    }

    async function fetchAndParse(date) {
      await fetchSem.acquire();
      try {
        let html = includeUnclassified ? await getSittingHtmlFromDb(date, db) : null;
        const replaceExisting = !!html;
        if (!html) html = await fetchSittingHTML(date, 1);
        if (!html || html.length < 500) return;
        if (!/<html|arrow_title_doc\.gif|<table|<td/i.test(html)) return;

        const sittingId = `sitting-${date}`;
        const { topics, speeches, sections } = parseSitting(html, sittingId, () => {});
        const topicTitles = topics.map(t => t.title);
        if (topicTitles.length === 0) return;

        const state = { sitting: { date, html, sittingId, topics, speeches, sections }, topicTitles, batchResults: {}, stored: false, replaceExisting };
        sittingState.set(sittingId, state);
        log(`  [REFRESH] Fetched sitting ${date} (${speeches.length} speeches).`);

        for (let i = 0; i < topicTitles.length; i += BATCH_SIZE) {
          taskQueue.push({ sittingId, batchIndex: Math.floor(i / BATCH_SIZE), topicTitles: topicTitles.slice(i, i + BATCH_SIZE) });
        }
      } catch (err) {
        fetchSkipped++;
        /* fetch errors (404 etc) — skip silently, don't log to file */
      } finally {
        fetchSem.release();
      }
    }

    async function aiWorker(workerId) {
      let cachedTopics = null;
      const getTopics = () => {
        if (!cachedTopics) cachedTopics = loadMacroTopics();
        return cachedTopics;
      };
      while (true) {
        let task = null;
        if (taskQueue.length > 0) task = taskQueue.shift();
        else if (producerDone) break;
        else { await new Promise(r => setTimeout(r, 50)); continue; }
        if (!task) continue;

        try {
          const existingTopics = getTopics();
          const { results, newTopics } = await classifyBatch(task.topicTitles, existingTopics, () => {});
          if (newTopics.length > 0) {
            await addMacroTopics(newTopics);
            cachedTopics = null;
          }
          const state = sittingState.get(task.sittingId);
          if (state) {
            state.batchResults[task.batchIndex] = results;
            onSittingComplete(task.sittingId);
          }
        } catch (err) {
          aiFailed++;
          log(`  ✗ AI ${task.sittingId}: ${err.message}`);
          appendFailureLog('AI', task.sittingId, err);
        }
      }
    }

    async function produceLoop() {
      for (let i = 0; i < datesToProcess.length; i += FETCH_CONCURRENCY) {
        await Promise.all(datesToProcess.slice(i, i + FETCH_CONCURRENCY).map(d => fetchAndParse(d)));
      }
      producerDone = true;
    }

    await Promise.all([produceLoop(), ...Array.from({ length: AI_WORKERS }, (_, i) => aiWorker(i + 1))]);
    await Promise.all(storePromises);

    const pending = sittingState.size - processed - failed;
    log(`[REFRESH] Pipeline complete — sittings stored: ${processed}, store failed: ${failed}, fetch skipped: ${fetchSkipped}, AI failed: ${aiFailed}, pending: ${pending}.`);
    if (aiFailed > 0 || failed > 0) {
      log(`[REFRESH] Failures logged to: ${FAILURES_LOG}`);
    }
    return { processed, failed, fetchSkipped, aiFailed, pending };
  } finally {
    if (closeDb) db.close();
  }
}

module.exports = { runBulk, FETCH_CONCURRENCY, AI_WORKERS };
