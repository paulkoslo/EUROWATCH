#!/usr/bin/env node
/**
 * Standalone script to optimize the database
 * Run this manually if you want to optimize without starting the server
 */

const sqlite3 = require('sqlite3').verbose();
const { DB_PATH } = require('../core/db');
const { optimizeDatabase } = require('../core/db-optimize');

console.log('🚀 Starting database optimization...');
console.log(`📁 Database: ${DB_PATH}`);

const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    console.error('❌ Error opening database:', err);
    process.exit(1);
  }
  
  console.log('✅ Database connection opened');
  
  optimizeDatabase(db, console.log)
    .then(() => {
      console.log('\n✅ Optimization complete!');
      
      // Verify WAL mode
      db.get('PRAGMA journal_mode', (err, row) => {
        if (err) {
          console.error('⚠️ Could not verify journal mode:', err);
        } else {
          console.log(`📊 Journal mode: ${row.journal_mode}`);
          if (row.journal_mode === 'wal') {
            console.log('✅ WAL mode is active');
          }
        }
        
        // List all indexes
        db.all("SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY tbl_name, name", (err, indexes) => {
          if (err) {
            console.error('⚠️ Could not list indexes:', err);
          } else {
            console.log(`\n📊 Created ${indexes.length} indexes:`);
            const byTable = {};
            indexes.forEach(idx => {
              if (!byTable[idx.tbl_name]) {
                byTable[idx.tbl_name] = [];
              }
              byTable[idx.tbl_name].push(idx.name);
            });
            
            Object.keys(byTable).sort().forEach(table => {
              console.log(`  ${table}:`);
              byTable[table].forEach(idx => {
                console.log(`    - ${idx}`);
              });
            });
          }
          
          db.close((err) => {
            if (err) {
              console.error('⚠️ Error closing database:', err);
            } else {
              console.log('\n✅ Database connection closed');
            }
            process.exit(0);
          });
        });
      });
    })
    .catch((err) => {
      console.error('❌ Optimization failed:', err);
      db.close();
      process.exit(1);
    });
});
