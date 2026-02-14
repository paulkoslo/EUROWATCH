#!/usr/bin/env node
/**
 * Run language detection on individual_speeches (CLD3 + franc, EU-constrained).
 * Use after import or when backfilling language for existing rows.
 *
 *   node scripts/detect-language.js           # only rows where language IS NULL
 *   node scripts/detect-language.js --all    # all rows (re-detect)
 */

require('dotenv').config();
const sqlite3 = require('sqlite3').verbose();
const { DB_PATH } = require('../core/db');
const { runDetectionOnDb } = require('../core/detect-language');

const onlyNull = !process.argv.includes('--all');

const db = new sqlite3.Database(DB_PATH);

runDetectionOnDb(db, { onlyNull, log: console.log })
  .then((result) => {
    console.log('[LANG] Result:', result);
    db.close();
    process.exit(0);
  })
  .catch((err) => {
    console.error('[LANG] Error:', err);
    db.close();
    process.exit(1);
  });
