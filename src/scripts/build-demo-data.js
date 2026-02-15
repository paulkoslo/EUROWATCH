#!/usr/bin/env node
/**
 * Build Demo Data
 *
 * Creates data/demo-data/ with a copy of ep_data.db containing only sittings
 * from 2020 onwards. Use for lighter demo or testing (e.g. Render).
 *
 * Usage:
 *   node src/scripts/build-demo-data.js           # Ep data only
 *   node src/scripts/build-demo-data.js --analytics  # Also generate analytics.db
 *
 * npm run demo-data
 */

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');
const sqlite3 = require('sqlite3').verbose();

const { DB_PATH } = require('../core/db');
const DEMO_DIR = path.join(__dirname, '..', '..', 'data', 'demo-data');
const DEMO_DB = path.join(DEMO_DIR, 'ep_data.db');
const DEMO_ANALYTICS = path.join(DEMO_DIR, 'analytics.db');
const CUTOFF = '2020-01-01';

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

async function main() {
  const withAnalytics = process.argv.includes('--analytics');
  console.log('Building demo data (sittings from 2020 onwards)...');
  console.log('Source:', DB_PATH);

  if (!fs.existsSync(DB_PATH)) {
    console.error('Source database not found:', DB_PATH);
    process.exit(1);
  }

  fs.mkdirSync(DEMO_DIR, { recursive: true });

  const sourceDb = new sqlite3.Database(DB_PATH, (err) => {
    if (err) {
      console.error('Error opening source database:', err);
      process.exit(1);
    }
  });

  try {
    await run(sourceDb, 'PRAGMA wal_checkpoint(TRUNCATE)');
    sourceDb.close();
  } catch (e) {
    sourceDb.close();
    throw e;
  }

  fs.copyFileSync(DB_PATH, DEMO_DB);
  console.log('Copied database to', DEMO_DB);

  const demoDb = new sqlite3.Database(DEMO_DB, (err) => {
    if (err) {
      console.error('Error opening demo database:', err);
      process.exit(1);
    }
  });

  try {
    const beforeSittings = await get(demoDb, 'SELECT COUNT(*) as n FROM sittings');
    const beforeSpeeches = await get(demoDb, 'SELECT COUNT(*) as n FROM individual_speeches');

    await run(demoDb, `DELETE FROM individual_speeches WHERE sitting_id IN (
      SELECT id FROM sittings WHERE activity_date < ? OR activity_date IS NULL
    )`, [CUTOFF]);
    const delSpeeches = beforeSpeeches.n - (await get(demoDb, 'SELECT COUNT(*) as n FROM individual_speeches')).n;

    await run(demoDb, `DELETE FROM sittings WHERE activity_date < ? OR activity_date IS NULL`, [CUTOFF]);
    const delSittings = beforeSittings.n - (await get(demoDb, 'SELECT COUNT(*) as n FROM sittings')).n;

    await run(demoDb, 'DELETE FROM speeches');
    await run(demoDb, `INSERT OR REPLACE INTO speeches (id, type, label, activity_date, content, last_updated)
      SELECT id, type, label, activity_date, content, last_updated FROM sittings WHERE id IS NOT NULL`);

    await run(demoDb, 'DELETE FROM sittings_cache');
    await run(demoDb, `UPDATE cache_status SET meps_last_updated = 0, speeches_last_updated = 0, total_speeches = (SELECT COUNT(*) FROM individual_speeches) WHERE id = 1`);
    await run(demoDb, `INSERT OR IGNORE INTO cache_status (id, meps_last_updated, speeches_last_updated, total_speeches) VALUES (1, 0, 0, (SELECT COUNT(*) FROM individual_speeches))`);

    await run(demoDb, 'VACUUM');

    const afterSittings = await get(demoDb, 'SELECT COUNT(*) as n FROM sittings');
    const afterSpeeches = await get(demoDb, 'SELECT COUNT(*) as n FROM individual_speeches');

    console.log('Removed', delSittings, 'sittings and', delSpeeches, 'individual speeches');
    console.log('Demo DB now has', afterSittings.n, 'sittings and', afterSpeeches.n, 'individual speeches');
  } finally {
    demoDb.close();
  }

  if (withAnalytics) {
    console.log('\nGenerating analytics for demo database...');
    const result = spawnSync('node', ['src/scripts/generate-analytics.js'], {
      cwd: path.join(__dirname, '..', '..'),
      env: {
        ...process.env,
        DB_PATH: DEMO_DB,
        ANALYTICS_DB_PATH: DEMO_ANALYTICS
      },
      stdio: 'inherit'
    });
    if (result.status !== 0) {
      process.exit(result.status || 1);
    }
  }

  console.log('\nDemo data ready at:', DEMO_DIR);
  console.log('Files:', fs.readdirSync(DEMO_DIR).join(', '));
  console.log('\nTo use locally:');
  console.log('  DB_PATH=' + DEMO_DB + ' ANALYTICS_DB_PATH=' + DEMO_ANALYTICS + ' npm start');
  console.log('\nSee docs/RENDER_DEMO.md for Render deployment.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
