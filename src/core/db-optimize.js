/**
 * Database Optimization Module
 * 
 * Enables WAL mode, sets performance PRAGMAs, and creates indexes
 * for optimal query performance. This function is idempotent and
 * safe to run multiple times.
 * 
 * @param {sqlite3.Database} db - SQLite database instance
 * @param {Function} log - Logging function (default: console.log)
 * @returns {Promise<void>} Resolves when optimization is complete
 */
function optimizeDatabase(db, log = console.log) {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      let completed = 0;
      let errors = [];
      const totalSteps = 1 + 4 + 9 + 3 + 3 + 1; // WAL + PRAGMAs + speech indexes (9 now) + sitting indexes + mep indexes + ANALYZE
      
      // Helper to track completion
      const checkComplete = () => {
        completed++;
        if (completed === totalSteps) {
          if (errors.length > 0) {
            log(`⚠️ [DB] Optimization completed with ${errors.length} warning(s)`);
            errors.forEach(err => log(`   ⚠️ ${err}`));
          } else {
            log(`✅ [DB] Database optimization complete`);
          }
          resolve();
        }
      };
      
      // Helper to handle errors gracefully
      const handleError = (step, err) => {
        if (err) {
          const errorMsg = `${step}: ${err.message}`;
          errors.push(errorMsg);
          log(`⚠️ [DB] ${errorMsg}`);
        }
        checkComplete();
      };
      
      // A. Enable WAL Mode
      db.run('PRAGMA journal_mode = WAL', (err) => {
        if (err) {
          handleError('WAL mode', err);
        } else {
          log('✅ [DB] WAL mode enabled');
          checkComplete();
        }
      });
      
      // B. Set Performance PRAGMAs
      db.run('PRAGMA cache_size = -16000', (err) => {
        handleError('cache_size', err);
      });
      
      db.run('PRAGMA synchronous = NORMAL', (err) => {
        handleError('synchronous', err);
      });
      
      db.run('PRAGMA temp_store = MEMORY', (err) => {
        handleError('temp_store', err);
      });
      
      db.run('PRAGMA foreign_keys = ON', (err) => {
        handleError('foreign_keys', err);
      });
      
      // C. Create Indexes for individual_speeches table
      const speechIndexes = [
        { name: 'idx_speeches_sitting_id', sql: 'CREATE INDEX IF NOT EXISTS idx_speeches_sitting_id ON individual_speeches(sitting_id)' },
        { name: 'idx_speeches_mep_id', sql: 'CREATE INDEX IF NOT EXISTS idx_speeches_mep_id ON individual_speeches(mep_id)' },
        { name: 'idx_speeches_macro_topic', sql: 'CREATE INDEX IF NOT EXISTS idx_speeches_macro_topic ON individual_speeches(macro_topic)' },
        { name: 'idx_speeches_political_group', sql: 'CREATE INDEX IF NOT EXISTS idx_speeches_political_group ON individual_speeches(political_group_std, political_group)' },
        { name: 'idx_speeches_language', sql: 'CREATE INDEX IF NOT EXISTS idx_speeches_language ON individual_speeches(language)' },
        { name: 'idx_speeches_date', sql: 'CREATE INDEX IF NOT EXISTS idx_speeches_date ON individual_speeches(created_at)' },
        { name: 'idx_speeches_topic_date', sql: 'CREATE INDEX IF NOT EXISTS idx_speeches_topic_date ON individual_speeches(macro_topic, created_at)' },
        { name: 'idx_speeches_group_topic', sql: 'CREATE INDEX IF NOT EXISTS idx_speeches_group_topic ON individual_speeches(political_group_std, macro_topic)' },
        { name: 'idx_speeches_sitting_topic', sql: 'CREATE INDEX IF NOT EXISTS idx_speeches_sitting_topic ON individual_speeches(sitting_id, macro_topic)' }
      ];
      
      let speechIndexCount = 0;
      speechIndexes.forEach(index => {
        db.run(index.sql, (err) => {
          if (err) {
            handleError(`Index ${index.name}`, err);
          } else {
            speechIndexCount++;
            if (speechIndexCount === speechIndexes.length) {
              log(`✅ [DB] Created ${speechIndexes.length}/${speechIndexes.length} speech indexes (including composite)`);
            }
          }
          checkComplete();
        });
      });
      
      // D. Create Indexes for sittings table
      const sittingIndexes = [
        { name: 'idx_sittings_activity_date', sql: 'CREATE INDEX IF NOT EXISTS idx_sittings_activity_date ON sittings(activity_date)' },
        { name: 'idx_sittings_date', sql: 'CREATE INDEX IF NOT EXISTS idx_sittings_date ON sittings(date)' },
        { name: 'idx_sittings_person_id', sql: 'CREATE INDEX IF NOT EXISTS idx_sittings_person_id ON sittings(personId)' }
      ];
      
      let sittingIndexCount = 0;
      sittingIndexes.forEach(index => {
        db.run(index.sql, (err) => {
          if (err) {
            handleError(`Index ${index.name}`, err);
          } else {
            sittingIndexCount++;
            if (sittingIndexCount === sittingIndexes.length) {
              log(`✅ [DB] Created ${sittingIndexes.length}/${sittingIndexes.length} sitting indexes`);
            }
          }
          checkComplete();
        });
      });
      
      // E. Create Indexes for meps table
      const mepIndexes = [
        { name: 'idx_meps_country', sql: 'CREATE INDEX IF NOT EXISTS idx_meps_country ON meps(country)' },
        { name: 'idx_meps_political_group', sql: 'CREATE INDEX IF NOT EXISTS idx_meps_political_group ON meps(politicalGroup)' },
        { name: 'idx_meps_is_current', sql: 'CREATE INDEX IF NOT EXISTS idx_meps_is_current ON meps(is_current)' }
      ];
      
      let mepIndexCount = 0;
      mepIndexes.forEach(index => {
        db.run(index.sql, (err) => {
          if (err) {
            handleError(`Index ${index.name}`, err);
          } else {
            mepIndexCount++;
            if (mepIndexCount === mepIndexes.length) {
              log(`✅ [DB] Created ${mepIndexes.length}/${mepIndexes.length} MEP indexes`);
            }
          }
          checkComplete();
        });
      });
      
      // F. Run ANALYZE to update query planner statistics
      // This is critical for SQLite to use indexes effectively
      db.run('ANALYZE', (err) => {
        if (err) {
          handleError('ANALYZE', err);
        } else {
          log('✅ [DB] Query planner statistics updated (ANALYZE)');
        }
        checkComplete();
      });
    });
  });
}

module.exports = { optimizeDatabase };
