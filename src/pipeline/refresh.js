/**
 * Refresh: run bulk from most recent fully processed sitting onwards.
 * Same log format as bulk — Fetched / stored only.
 */

process.env.DOTENV_CONFIG_QUIET = '1';
require('dotenv').config();

const sqlite3 = require('sqlite3').verbose();
const { findMostRecentFullyProcessedSitting, listDatesInRange } = require('../core/parliament-fetch');
const { runBulk } = require('./bulk');
const { DB_PATH } = require('../core/db');

const EARLIEST = '1999-07-20';

function addDays(dateStr, days) {
  const d = new Date(dateStr);
  d.setDate(d.getDate() + days);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

async function runRefresh(options = {}) {
  const log = options.log || console.log;
  const externalDb = options.db;

  const db = externalDb || new sqlite3.Database(DB_PATH);
  const closeDb = !externalDb;

  try {
    log('[REFRESH] Checking for new sitting dates (from last fully processed sitting through today)...');
    const lastComplete = await findMostRecentFullyProcessedSitting(db);
    const startDate = lastComplete ? addDays(lastComplete, 1) : EARLIEST;
    const endDate = new Date().toISOString().slice(0, 10);
    if (lastComplete) {
      log(`[REFRESH] Last fully processed sitting in DB: ${lastComplete}. Will fetch from ${startDate} through ${endDate}.`);
    } else {
      log(`[REFRESH] No fully processed sitting found in DB. Will fetch from ${startDate} through ${endDate}.`);
    }

    const result = await runBulk({
      startDate,
      endDate,
      includeUnclassified: true,
      log,
      db
    });

    return {
      success: result.processed >= 0,
      processed: result.processed,
      failed: result.failed,
      fetchSkipped: result.fetchSkipped,
      aiFailed: result.aiFailed
    };
  } finally {
    if (closeDb) db.close();
  }
}

module.exports = { runRefresh };
