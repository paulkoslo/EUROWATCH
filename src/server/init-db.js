/**
 * Database initialization: create tables, run migrations, seed MEPs and optionally speeches.
 */
const { optimizeDatabase } = require('../core/db-optimize');
const { ensureLanguageColumn } = require('../core/detect-language');
const { fetchAllMeps } = require('./meps-api');
const speechesFetch = require('./speeches-fetch');
const { parseRecentSpeeches } = require('./parse-speeches');

function initDatabase(db) {
  return new Promise((resolve, reject) => {
    optimizeDatabase(db, console.log)
      .then(() => {
        db.serialize(() => {
          createTablesAndInit(db);
        });
      })
      .catch((err) => {
        console.error('[INIT] Database optimization warning:', err.message);
        db.serialize(() => {
          createTablesAndInit(db);
        });
      });

    function createTablesAndInit(db) {
      db.run(`CREATE TABLE IF NOT EXISTS meps (
        id INTEGER PRIMARY KEY,
        label TEXT,
        givenName TEXT,
        familyName TEXT,
        sortLabel TEXT,
        country TEXT,
        politicalGroup TEXT,
        is_current BOOLEAN DEFAULT 0,
        source TEXT DEFAULT 'api',
        last_updated INTEGER DEFAULT 0
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS sittings (
        id TEXT PRIMARY KEY,
        type TEXT,
        label TEXT,
        personId INTEGER,
        date TEXT,
        content TEXT UNIQUE,
        docIdentifier TEXT,
        notationId TEXT,
        activity_type TEXT,
        activity_date TEXT,
        activity_start_date TEXT,
        last_updated INTEGER DEFAULT 0
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS individual_speeches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sitting_id TEXT,
        speaker_name TEXT,
        political_group TEXT,
        title TEXT,
        speech_content TEXT,
        speech_order INTEGER,
        created_at INTEGER DEFAULT (strftime('%s', 'now')),
        FOREIGN KEY (sitting_id) REFERENCES sittings (id)
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS sittings_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT,
        last_updated INTEGER
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS cache_status (
        id INTEGER PRIMARY KEY,
        meps_last_updated INTEGER DEFAULT 0,
        speeches_last_updated INTEGER DEFAULT 0,
        total_speeches INTEGER DEFAULT 0
      )`);

      // Legacy: some DBs have individual_speeches.FK referencing "speeches". Ensure that table exists and matches sittings.
      db.run(`CREATE TABLE IF NOT EXISTS speeches (
        id TEXT PRIMARY KEY,
        type TEXT,
        label TEXT,
        personId INTEGER,
        date TEXT,
        content TEXT,
        docIdentifier TEXT,
        notationId TEXT,
        activity_type TEXT,
        activity_date TEXT,
        activity_start_date TEXT,
        last_updated INTEGER DEFAULT 0
      )`, () => {});

      db.run(`INSERT OR REPLACE INTO speeches (id, type, label, activity_date, content, last_updated)
              SELECT id, type, label, activity_date, content, last_updated FROM sittings WHERE id IS NOT NULL`, () => {});

      ensureLanguageColumn(db).catch(e => console.error('[MIGRATION] Language column:', e.message));

      db.get("PRAGMA table_info(sittings)", (err, row) => {
        if (err) return;
        db.all("PRAGMA table_info(sittings)", (err, columns) => {
          if (err) return;
          const columnNames = (columns || []).map(col => col.name);
          const required = ['docIdentifier', 'notationId', 'activity_type', 'activity_date', 'activity_start_date', 'last_updated'];
          const missing = required.filter(col => !columnNames.includes(col));
          if (missing.length > 0) {
            db.run('DROP TABLE IF EXISTS sittings', (err) => {
              if (!err) {
                db.run(`CREATE TABLE sittings (
                  id TEXT PRIMARY KEY, type TEXT, label TEXT, personId INTEGER, date TEXT, content TEXT UNIQUE,
                  docIdentifier TEXT, notationId TEXT, activity_type TEXT, activity_date TEXT, activity_start_date TEXT, last_updated INTEGER DEFAULT 0
                )`);
              }
            });
          }
        });
      });

      db.all("PRAGMA table_info(meps)", (err, columns) => {
        if (err) return;
        const names = (columns || []).map(col => col.name);
        if (!names.includes('is_current')) {
          db.run('ALTER TABLE meps ADD COLUMN is_current BOOLEAN DEFAULT 0', () => {});
        }
        if (!names.includes('source')) {
          db.run('ALTER TABLE meps ADD COLUMN source TEXT DEFAULT "api"', () => {});
        }
      });

      db.get('SELECT COUNT(*) as count FROM meps', async (err, mepRow) => {
        if (err) return reject(err);
        const mepCount = mepRow.count;
        console.log(`[INIT] Found ${mepCount} MEPs in database`);

        db.get('SELECT COUNT(*) as count FROM sittings', async (err, speechRow) => {
          if (err) return reject(err);
          const speechCount = speechRow.count;
          console.log(`[INIT] Found ${speechCount} sittings in database`);

          if (mepCount === 0) {
            try {
              console.log('[INIT] Database empty, fetching MEPs from API...');
              const meps = await fetchAllMeps();
              const stmt = db.prepare(`INSERT OR REPLACE INTO meps (id, label, givenName, familyName, sortLabel, country, politicalGroup, is_current, source, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
              for (const m of meps) {
                stmt.run(parseInt(m.identifier, 10), m.label, m.givenName, m.familyName, m.sortLabel, m['api:country-of-representation'], m['api:political-group'], 1, 'api', Date.now());
              }
              stmt.finalize();
              console.log(`[INIT] Seeded ${meps.length} MEP records`);
            } catch (err) {
              return reject(err);
            }
          }

          if (process.env.ENABLE_AUTO_INIT === 'true' && speechCount === 0) {
            try {
              console.log('[INIT] No sittings in database, fetching all sittings from API...');
              await speechesFetch.cacheAllSpeeches(db);
            } catch (err) {
              console.error('[INIT] Error fetching speeches:', err);
            }
          } else if (process.env.ENABLE_AUTO_INIT === 'true') {
            const twoYearsAgo = new Date();
            twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
            const cutoffDate = twoYearsAgo.toISOString().split('T')[0];
            db.get('SELECT COUNT(*) as count FROM sittings WHERE content != "" AND LENGTH(content) > 100 AND date >= ?', [cutoffDate], async (err, contentRow) => {
              const speechesWithContent = contentRow?.count || 0;
              if (speechesWithContent < 50) {
                try {
                  await speechesFetch.addContentToExistingSpeeches(db);
                } catch (e) {
                  console.error('[INIT] Error adding content:', e);
                }
              }
              db.get(`SELECT COUNT(*) as count FROM sittings s LEFT JOIN individual_speeches i ON s.id = i.sitting_id WHERE s.activity_date >= date('now', '-1 year') AND s.content IS NOT NULL AND s.content != '' AND s.content != 'No content available' AND i.sitting_id IS NULL`, async (err, row) => {
                if ((row?.count || 0) > 0) {
                  try {
                    await parseRecentSpeeches(db);
                  } catch (e) {
                    console.error('[INIT] Error parsing recent speeches:', e);
                  }
                }
                resolve();
              });
            });
          } else {
            resolve();
          }
        });
      });
    }
  });
}

module.exports = { initDatabase };
