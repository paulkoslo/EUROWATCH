#!/usr/bin/env node
/**
 * Step 1: Discover the most recent sitting date not in DB
 * Uses HTML scraping only — iterates backwards from today, probes Europarl document URLs
 *
 * Usage: node scripts/step-1-discover-date.js [--max-days 365]
 * Output: prints date (YYYY-MM-DD) or "none"
 */

require('dotenv').config();
const sqlite3 = require('sqlite3').verbose();
const { discoverNextSittingDate } = require('../core/parliament-fetch');
const { DB_PATH } = require('../core/db');

async function run(options = {}) {
  const log = options.log || (() => {});
  const maxDaysBack = options.maxDaysBack ?? 365;
  const db = new sqlite3.Database(DB_PATH);

  try {
    const date = await discoverNextSittingDate(db, log, maxDaysBack);
    return date;
  } finally {
    db.close();
  }
}

if (require.main === module) {
  const maxIdx = process.argv.indexOf('--max-days');
  const maxDays = maxIdx !== -1 && process.argv[maxIdx + 1] ? parseInt(process.argv[maxIdx + 1], 10) : 365;

  run({ log: console.log, maxDaysBack: maxDays })
    .then(date => {
      if (date) {
        console.log(date);
        process.exit(0);
      } else {
        console.log('none');
        process.exit(1);
      }
    })
    .catch(err => {
      console.error('Error:', err.message);
      process.exit(1);
    });
}

module.exports = { run };
