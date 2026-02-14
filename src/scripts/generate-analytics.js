#!/usr/bin/env node
/**
 * Generate Analytics Database
 * 
 * Pre-computes all analytics data into a separate database for instant loading.
 * Run this after adding new data to regenerate analytics.
 */

const sqlite3 = require('sqlite3').verbose();
const { DB_PATH } = require('../core/db');
const { generateAnalyticsDatabase } = require('../core/analytics-db');

console.log('🚀 Generating analytics database...');
console.log(`📁 Source database: ${DB_PATH}`);

const sourceDb = new sqlite3.Database(DB_PATH, sqlite3.OPEN_READONLY, (err) => {
  if (err) {
    console.error('❌ Error opening source database:', err);
    process.exit(1);
  }
  
  console.log('✅ Source database opened');
  console.log('⏳ This may take 1-5 minutes depending on data size...\n');
  
  const startTime = Date.now();
  
  generateAnalyticsDatabase(sourceDb, console.log)
    .then(() => {
      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      console.log(`\n✅ Analytics database generated successfully in ${duration} seconds!`);
      console.log('📊 Analytics will now load instantly from the pre-computed database.');
      sourceDb.close();
      process.exit(0);
    })
    .catch((err) => {
      console.error('\n❌ Error generating analytics database:', err);
      sourceDb.close();
      process.exit(1);
    });
});
