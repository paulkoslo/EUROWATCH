/**
 * Analytics Database Module
 * 
 * Creates and maintains a separate SQLite database with pre-computed analytics data.
 * This eliminates the need to recalculate analytics on every request.
 */

const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const { DB_PATH } = require('./db');

const ANALYTICS_DB_PATH = path.join(__dirname, '..', '..', 'data', 'analytics.db');

/**
 * Initialize the analytics database schema
 */
function initAnalyticsDatabase(db) {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // Enable WAL mode and performance settings
      db.run('PRAGMA journal_mode = WAL');
      db.run('PRAGMA cache_size = -16000');
      db.run('PRAGMA synchronous = NORMAL');
      
      // Metadata table to track when analytics were last computed
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_metadata (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
      `);
      
      // Normalized topics and their variants
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_topics (
          normalized_topic TEXT PRIMARY KEY,
          variants TEXT,
          updated_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
      `);
      
      // Distinct periods (months/quarters)
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_periods (
          period TEXT PRIMARY KEY,
          interval_type TEXT,
          updated_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
      `);
      
      // Pre-computed time series: monthly
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_timeseries_month (
          period TEXT,
          topic TEXT,
          count INTEGER,
          PRIMARY KEY (period, topic)
        )
      `);
      db.run('CREATE INDEX IF NOT EXISTS idx_ts_month_period ON analytics_timeseries_month(period)');
      db.run('CREATE INDEX IF NOT EXISTS idx_ts_month_topic ON analytics_timeseries_month(topic)');
      
      // Pre-computed time series: quarterly
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_timeseries_quarter (
          period TEXT,
          topic TEXT,
          count INTEGER,
          PRIMARY KEY (period, topic)
        )
      `);
      db.run('CREATE INDEX IF NOT EXISTS idx_ts_quarter_period ON analytics_timeseries_quarter(period)');
      db.run('CREATE INDEX IF NOT EXISTS idx_ts_quarter_topic ON analytics_timeseries_quarter(topic)');
      
      // Pre-computed time series: yearly
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_timeseries_year (
          period TEXT,
          topic TEXT,
          count INTEGER,
          PRIMARY KEY (period, topic)
        )
      `);
      db.run('CREATE INDEX IF NOT EXISTS idx_ts_year_period ON analytics_timeseries_year(period)');
      db.run('CREATE INDEX IF NOT EXISTS idx_ts_year_topic ON analytics_timeseries_year(topic)');
      
      // Pre-computed by-group data
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_by_group (
          topic TEXT,
          group_name TEXT,
          count INTEGER,
          PRIMARY KEY (topic, group_name)
        )
      `);
      db.run('CREATE INDEX IF NOT EXISTS idx_by_group_topic ON analytics_by_group(topic)');
      db.run('CREATE INDEX IF NOT EXISTS idx_by_group_name ON analytics_by_group(group_name)');
      
      // Pre-computed by-language data (macro topic × language)
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_by_language (
          topic TEXT,
          language TEXT,
          count INTEGER,
          PRIMARY KEY (topic, language)
        )
      `);
      db.run('CREATE INDEX IF NOT EXISTS idx_by_language_topic ON analytics_by_language(topic)');
      db.run('CREATE INDEX IF NOT EXISTS idx_by_language_name ON analytics_by_language(language)');
      
      // Pre-computed languages
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_languages (
          language TEXT PRIMARY KEY,
          count INTEGER,
          updated_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
      `);
      
      // Overview statistics
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_overview (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
      `);
      
      // Top topics (for overview)
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_top_topics (
          topic TEXT PRIMARY KEY,
          count INTEGER,
          rank INTEGER,
          updated_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
      `);
      
      // Top specific focuses
      db.run(`
        CREATE TABLE IF NOT EXISTS analytics_top_focuses (
          topic TEXT,
          focus TEXT,
          count INTEGER,
          PRIMARY KEY (topic, focus)
        )
      `);
      
      db.run('ANALYZE', (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });
}

/**
 * Generate analytics database from main database
 */
async function generateAnalyticsDatabase(sourceDb, log = console.log) {
  return new Promise((resolve, reject) => {
    const analyticsDb = new sqlite3.Database(ANALYTICS_DB_PATH, (err) => {
      if (err) {
        reject(err);
        return;
      }
      
      log('[ANALYTICS-DB] Initializing analytics database schema...');
      initAnalyticsDatabase(analyticsDb)
        .then(() => {
          log('[ANALYTICS-DB] Schema initialized');
          return computeAllAnalytics(sourceDb, analyticsDb, log);
        })
        .then(() => {
          // Update metadata
          const now = Math.floor(Date.now() / 1000);
          analyticsDb.run(
            'INSERT OR REPLACE INTO analytics_metadata (key, value, updated_at) VALUES (?, ?, ?)',
            ['last_computed', now.toString(), now],
            (err) => {
              if (err) {
                log(`[ANALYTICS-DB] Warning: Could not update metadata: ${err.message}`);
              } else {
                log(`[ANALYTICS-DB] Analytics database generated successfully at ${new Date().toISOString()}`);
              }
              analyticsDb.close((closeErr) => {
                if (closeErr) {
                  reject(closeErr);
                } else {
                  resolve();
                }
              });
            }
          );
        })
        .catch((err) => {
          analyticsDb.close();
          reject(err);
        });
    });
  });
}

/**
 * Compute all analytics and store in analytics database
 */
function computeAllAnalytics(sourceDb, analyticsDb, log) {
  return new Promise((resolve, reject) => {
    analyticsDb.serialize(() => {
      // Clear existing data
      log('[ANALYTICS-DB] Clearing old analytics data...');
      analyticsDb.run('DELETE FROM analytics_timeseries_month');
      analyticsDb.run('DELETE FROM analytics_timeseries_quarter');
      analyticsDb.run('DELETE FROM analytics_timeseries_year');
      analyticsDb.run('DELETE FROM analytics_by_group');
      analyticsDb.run('DELETE FROM analytics_by_language');
      analyticsDb.run('DELETE FROM analytics_languages');
      analyticsDb.run('DELETE FROM analytics_topics');
      analyticsDb.run('DELETE FROM analytics_periods');
      analyticsDb.run('DELETE FROM analytics_overview');
      analyticsDb.run('DELETE FROM analytics_top_topics');
      analyticsDb.run('DELETE FROM analytics_top_focuses');
      
      // Step 1: Get all topics and normalize them
      log('[ANALYTICS-DB] Step 1/6: Loading and normalizing topics...');
      sourceDb.all(`
        SELECT DISTINCT i.macro_topic AS topic
        FROM individual_speeches i
        INNER JOIN sittings s ON s.id = i.sitting_id
        WHERE i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
          AND s.activity_date IS NOT NULL
      `, [], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        
        const normalizeTopic = (topic) => {
          if (!topic) return topic;
          return topic
            .replace(/&amp;/g, '&')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&quot;/g, '"')
            .replace(/&#39;/g, "'")
            .replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]/g, '-')
            .trim();
        };
        // Collapse key: merge variants that differ only by hyphenation (e.g. "Audio-visual" vs "Audiovisual")
        const collapseKey = (t) => (normalizeTopic(t) || '').toLowerCase().replace(/\s*-\s*/g, ' ').replace(/\s+/g, ' ').trim();
        
        const byCollapse = new Map();
        rows.forEach(row => {
          const ck = collapseKey(row.topic);
          if (!byCollapse.has(ck)) byCollapse.set(ck, []);
          const list = byCollapse.get(ck);
          if (!list.includes(row.topic)) list.push(row.topic);
        });
        const normalizedMap = new Map();
        byCollapse.forEach((variants) => {
          const canonical = normalizeTopic(variants[0]);
          normalizedMap.set(canonical, variants);
        });
        
        // Store topics
        const topicStmt = analyticsDb.prepare('INSERT INTO analytics_topics (normalized_topic, variants) VALUES (?, ?)');
        normalizedMap.forEach((variants, normalized) => {
          topicStmt.run(normalized, JSON.stringify(variants));
        });
        topicStmt.finalize();
        log(`[ANALYTICS-DB] Found ${normalizedMap.size} normalized topics`);
        
        // Step 2: Compute time series
        log('[ANALYTICS-DB] Step 2/6: Computing time series (this may take a minute)...');
        computeTimeSeries(sourceDb, analyticsDb, normalizedMap, log)
          .then(() => {
            // Step 3: Compute by-group
            log('[ANALYTICS-DB] Step 3/6: Computing by-group data...');
            return computeByGroup(sourceDb, analyticsDb, normalizedMap, log);
          })
          .then(() => {
            // Step 4: Compute by-language (macro topic × language)
            log('[ANALYTICS-DB] Step 4/6: Computing by-language data...');
            return computeByLanguage(sourceDb, analyticsDb, normalizedMap, log);
          })
          .then(() => {
            // Step 5: Compute languages
            log('[ANALYTICS-DB] Step 5/6: Computing languages...');
            return computeLanguages(sourceDb, analyticsDb, log);
          })
          .then(() => {
            // Step 6: Compute overview
            log('[ANALYTICS-DB] Step 6/6: Computing overview statistics...');
            return computeOverview(sourceDb, analyticsDb, log);
          })
          .then(() => {
            log('[ANALYTICS-DB] All analytics computed successfully!');
            resolve();
          })
          .catch(reject);
      });
    });
  });
}

function computeTimeSeries(sourceDb, analyticsDb, normalizedMap, log) {
  return new Promise((resolve, reject) => {
    const allVariants = Array.from(normalizedMap.values()).flat();
    const placeholders = allVariants.map(() => '?').join(',');
    
    // Monthly time series
    sourceDb.all(`
      SELECT substr(s.activity_date,1,7) AS period, i.macro_topic AS topic, COUNT(*) AS cnt
      FROM individual_speeches i
      INNER JOIN sittings s ON s.id = i.sitting_id
      WHERE i.macro_topic IN (${placeholders})
        AND s.activity_date IS NOT NULL
        AND TRIM(i.macro_topic) <> ''
      GROUP BY period, i.macro_topic
      ORDER BY period ASC
    `, allVariants, (err, rows) => {
      if (err) {
        reject(err);
        return;
      }
      
      const stmt = analyticsDb.prepare('INSERT INTO analytics_timeseries_month (period, topic, count) VALUES (?, ?, ?)');
      const periods = new Set();
      rows.forEach(row => {
        periods.add(row.period);
        stmt.run(row.period, row.topic, row.cnt);
      });
      stmt.finalize();
      
      // Store periods
      const periodStmt = analyticsDb.prepare('INSERT INTO analytics_periods (period, interval_type) VALUES (?, ?)');
      periods.forEach(p => periodStmt.run(p, 'month'));
      periodStmt.finalize();
      
      log(`[ANALYTICS-DB] Monthly time series: ${periods.size} periods, ${rows.length} topic-period combinations`);
      
      // Quarterly time series
      sourceDb.all(`
        SELECT 
          substr(s.activity_date,1,4) || '-Q' || ((cast(substr(s.activity_date,6,2) as integer)+2)/3) AS period,
          i.macro_topic AS topic,
          COUNT(*) AS cnt
        FROM individual_speeches i
        INNER JOIN sittings s ON s.id = i.sitting_id
        WHERE i.macro_topic IN (${placeholders})
          AND s.activity_date IS NOT NULL
          AND TRIM(i.macro_topic) <> ''
        GROUP BY period, i.macro_topic
        ORDER BY period ASC
      `, allVariants, (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        
        const stmt = analyticsDb.prepare('INSERT INTO analytics_timeseries_quarter (period, topic, count) VALUES (?, ?, ?)');
        const periods = new Set();
        rows.forEach(row => {
          periods.add(row.period);
          stmt.run(row.period, row.topic, row.cnt);
        });
        stmt.finalize();
        
        // Store periods
        const periodStmt = analyticsDb.prepare('INSERT INTO analytics_periods (period, interval_type) VALUES (?, ?)');
        periods.forEach(p => periodStmt.run(p, 'quarter'));
        periodStmt.finalize();
        
        log(`[ANALYTICS-DB] Quarterly time series: ${periods.size} periods, ${rows.length} topic-period combinations`);
        
        // Yearly time series
        sourceDb.all(`
          SELECT substr(s.activity_date,1,4) AS period, i.macro_topic AS topic, COUNT(*) AS cnt
          FROM individual_speeches i
          INNER JOIN sittings s ON s.id = i.sitting_id
          WHERE i.macro_topic IN (${placeholders})
            AND s.activity_date IS NOT NULL
            AND TRIM(i.macro_topic) <> ''
          GROUP BY period, i.macro_topic
          ORDER BY period ASC
        `, allVariants, (err, rows) => {
          if (err) {
            reject(err);
            return;
          }
          const stmt = analyticsDb.prepare('INSERT INTO analytics_timeseries_year (period, topic, count) VALUES (?, ?, ?)');
          const periods = new Set();
          rows.forEach(row => {
            periods.add(row.period);
            stmt.run(row.period, row.topic, row.cnt);
          });
          stmt.finalize();
          const periodStmt = analyticsDb.prepare('INSERT INTO analytics_periods (period, interval_type) VALUES (?, ?)');
          periods.forEach(p => periodStmt.run(p, 'year'));
          periodStmt.finalize();
          log(`[ANALYTICS-DB] Yearly time series: ${periods.size} periods, ${rows.length} topic-period combinations`);
          resolve();
        });
      });
    });
  });
}

function computeByGroup(sourceDb, analyticsDb, normalizedMap, log) {
  return new Promise((resolve, reject) => {
    // Get top groups
    sourceDb.all(`
      SELECT COALESCE(political_group_std, political_group) AS grp, COUNT(*) AS cnt
      FROM individual_speeches
      WHERE COALESCE(political_group_std, political_group) IS NOT NULL 
        AND TRIM(COALESCE(political_group_std, political_group))<>''
      GROUP BY grp
      ORDER BY cnt DESC LIMIT 10
    `, [], (err, groups) => {
      if (err) {
        reject(err);
        return;
      }
      
      const groupsList = groups.map(r => r.grp);
      const allVariants = Array.from(normalizedMap.values()).flat();
      const pT = allVariants.map(() => '?').join(',');
      const pG = groupsList.map(() => '?').join(',');
      
      sourceDb.all(`
        SELECT i.macro_topic AS topic, COALESCE(i.political_group_std, i.political_group) AS grp, COUNT(*) AS cnt
        FROM individual_speeches i
        WHERE i.macro_topic IN (${pT})
          AND COALESCE(i.political_group_std, i.political_group) IN (${pG})
        GROUP BY i.macro_topic, COALESCE(i.political_group_std, i.political_group)
      `, [...allVariants, ...groupsList], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        
        const normalizeTopic = (topic) => {
          if (!topic) return topic;
          return topic
            .replace(/&amp;/g, '&')
            .replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]/g, '-')
            .trim();
        };
        
        // Aggregate by (normalized topic, group) to avoid UNIQUE constraint — multiple raw topics can normalize to the same value
        const aggregated = new Map();
        rows.forEach(row => {
          const normalized = normalizeTopic(row.topic);
          const key = `${normalized}\t${row.grp}`;
          aggregated.set(key, (aggregated.get(key) || 0) + row.cnt);
        });
        
        const stmt = analyticsDb.prepare('INSERT INTO analytics_by_group (topic, group_name, count) VALUES (?, ?, ?)');
        aggregated.forEach((cnt, key) => {
          const [topic, group_name] = key.split('\t');
          stmt.run(topic, group_name, cnt);
        });
        stmt.finalize();
        
        log(`[ANALYTICS-DB] By-group: ${groupsList.length} groups, ${aggregated.size} combinations`);
        resolve();
      });
    });
  });
}

function computeByLanguage(sourceDb, analyticsDb, normalizedMap, log) {
  return new Promise((resolve, reject) => {
    // Get top languages (same normalisation as elsewhere)
    sourceDb.all(`
      SELECT UPPER(COALESCE(language,'UNK')) AS language, COUNT(*) AS cnt
      FROM individual_speeches
      GROUP BY UPPER(COALESCE(language,'UNK'))
      ORDER BY cnt DESC LIMIT 24
    `, [], (err, languages) => {
      if (err) {
        reject(err);
        return;
      }
      
      const languagesList = languages.map(r => r.language).filter(Boolean);
      const allVariants = Array.from(normalizedMap.values()).flat();
      const pT = allVariants.map(() => '?').join(',');
      const pL = languagesList.map(() => '?').join(',');
      
      sourceDb.all(`
        SELECT i.macro_topic AS topic, UPPER(COALESCE(i.language,'UNK')) AS language, COUNT(*) AS cnt
        FROM individual_speeches i
        WHERE i.macro_topic IN (${pT})
          AND UPPER(COALESCE(i.language,'UNK')) IN (${pL})
        GROUP BY i.macro_topic, UPPER(COALESCE(i.language,'UNK'))
      `, [...allVariants, ...languagesList], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        
        const normalizeTopic = (topic) => {
          if (!topic) return topic;
          return topic
            .replace(/&amp;/g, '&')
            .replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]/g, '-')
            .trim();
        };
        
        const aggregated = new Map();
        rows.forEach(row => {
          const normalized = normalizeTopic(row.topic);
          const key = `${normalized}\t${row.language}`;
          aggregated.set(key, (aggregated.get(key) || 0) + row.cnt);
        });
        
        const stmt = analyticsDb.prepare('INSERT INTO analytics_by_language (topic, language, count) VALUES (?, ?, ?)');
        aggregated.forEach((cnt, key) => {
          const [topic, language] = key.split('\t');
          stmt.run(topic, language, cnt);
        });
        stmt.finalize();
        
        log(`[ANALYTICS-DB] By-language: ${languagesList.length} languages, ${aggregated.size} combinations`);
        resolve();
      });
    });
  });
}

function computeLanguages(sourceDb, analyticsDb, log) {
  return new Promise((resolve, reject) => {
    sourceDb.all(`
      SELECT UPPER(COALESCE(language,'UNK')) AS language, COUNT(*) AS cnt
      FROM individual_speeches
      GROUP BY UPPER(COALESCE(language,'UNK'))
      ORDER BY cnt DESC
    `, [], (err, rows) => {
      if (err) {
        reject(err);
        return;
      }
      
      const stmt = analyticsDb.prepare('INSERT INTO analytics_languages (language, count) VALUES (?, ?)');
      rows.forEach(row => {
        stmt.run(row.language, row.cnt);
      });
      stmt.finalize();
      
      log(`[ANALYTICS-DB] Languages: ${rows.length} languages`);
      resolve();
    });
  });
}

function computeOverview(sourceDb, analyticsDb, log) {
  return new Promise((resolve, reject) => {
    Promise.all([
      new Promise((resolve, reject) => {
        sourceDb.get(`
          SELECT 
            COUNT(*) AS total,
            SUM(CASE WHEN macro_topic IS NOT NULL AND TRIM(macro_topic) <> '' THEN 1 ELSE 0 END) AS with_macro
          FROM individual_speeches
        `, [], (err, row) => err ? reject(err) : resolve(row));
      }),
      new Promise((resolve, reject) => {
        sourceDb.all(`
          SELECT macro_topic AS topic, COUNT(*) AS count
          FROM individual_speeches
          WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
          GROUP BY macro_topic
          ORDER BY count DESC LIMIT 20
        `, [], (err, rows) => err ? reject(err) : resolve(rows));
      }),
      new Promise((resolve, reject) => {
        sourceDb.all(`
          SELECT macro_topic AS topic, macro_specific_focus AS focus, COUNT(*) AS count
          FROM individual_speeches
          WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
            AND macro_specific_focus IS NOT NULL AND TRIM(macro_specific_focus) <> ''
          GROUP BY macro_topic, macro_specific_focus
          ORDER BY count DESC LIMIT 20
        `, [], (err, rows) => err ? reject(err) : resolve(rows));
      })
    ]).then(([coverage, macroTopics, specificFocus]) => {
      const total = coverage?.total || 0;
      const withMacro = coverage?.with_macro || 0;
      const pct = total ? Math.round((withMacro / total) * 1000) / 10 : 0;
      
      // Store overview
      analyticsDb.run('INSERT OR REPLACE INTO analytics_overview (key, value) VALUES (?, ?)', ['total', total.toString()]);
      analyticsDb.run('INSERT OR REPLACE INTO analytics_overview (key, value) VALUES (?, ?)', ['with_macro', withMacro.toString()]);
      analyticsDb.run('INSERT OR REPLACE INTO analytics_overview (key, value) VALUES (?, ?)', ['pct_with_macro', pct.toString()]);
      
      // Store top topics
      const topicStmt = analyticsDb.prepare('INSERT INTO analytics_top_topics (topic, count, rank) VALUES (?, ?, ?)');
      macroTopics.forEach((row, idx) => {
        topicStmt.run(row.topic, row.count, idx + 1);
      });
      topicStmt.finalize();
      
      // Store top focuses
      const focusStmt = analyticsDb.prepare('INSERT INTO analytics_top_focuses (topic, focus, count) VALUES (?, ?, ?)');
      specificFocus.forEach(row => {
        focusStmt.run(row.topic, row.focus, row.count);
      });
      focusStmt.finalize();
      
      log(`[ANALYTICS-DB] Overview: ${total} total, ${withMacro} with macro topics`);
      resolve();
    }).catch(reject);
  });
}

/**
 * Load analytics data from pre-computed database
 */
function loadAnalyticsFromDatabase(log = console.log) {
  return new Promise((resolve, reject) => {
    const analyticsDb = new sqlite3.Database(ANALYTICS_DB_PATH, sqlite3.OPEN_READONLY, (err) => {
      if (err) {
        reject(new Error(`Analytics database not found. Run 'node src/scripts/generate-analytics.js' first.`));
        return;
      }
      
      const cacheData = {};
      
      // Load topics
      analyticsDb.all('SELECT normalized_topic, variants FROM analytics_topics', [], (err, rows) => {
        if (err) {
          analyticsDb.close();
          reject(err);
          return;
        }
        
        const normalizedMap = new Map();
        rows.forEach(row => {
          normalizedMap.set(row.normalized_topic, JSON.parse(row.variants));
        });
        cacheData.allTopics = Array.from(normalizedMap.keys());
        cacheData.topicVariants = normalizedMap;
        
        // Load time series
        Promise.all([
          loadTimeSeries(analyticsDb, 'month', normalizedMap),
          loadTimeSeries(analyticsDb, 'quarter', normalizedMap),
          loadTimeSeries(analyticsDb, 'year', normalizedMap),
          loadByGroup(analyticsDb, normalizedMap),
          loadByLanguage(analyticsDb, normalizedMap),
          loadLanguages(analyticsDb),
          loadOverview(analyticsDb)
        ]).then(([monthTS, quarterTS, yearTS, byGroup, byLanguage, languages, overview]) => {
          cacheData.timeseries_month = monthTS;
          cacheData.timeseries_quarter = quarterTS;
          cacheData.timeseries_year = yearTS;
          cacheData.byGroup = byGroup;
          cacheData.byLanguage = byLanguage;
          cacheData.languages = languages;
          cacheData.overview = overview;
          
          analyticsDb.close();
          log('[ANALYTICS-DB] Loaded analytics from pre-computed database');
          resolve(cacheData);
        }).catch((err) => {
          analyticsDb.close();
          reject(err);
        });
      });
    });
  });
}

function loadTimeSeries(analyticsDb, interval, normalizedMap) {
  return new Promise((resolve, reject) => {
    const table = interval === 'year' ? 'analytics_timeseries_year' : interval === 'quarter' ? 'analytics_timeseries_quarter' : 'analytics_timeseries_month';
    
    Promise.all([
      new Promise((resolve, reject) => {
        analyticsDb.all(`SELECT period, topic, count AS cnt FROM ${table} ORDER BY period ASC`, [], (err, rows) => {
          err ? reject(err) : resolve(rows);
        });
      }),
      new Promise((resolve, reject) => {
        analyticsDb.all(`SELECT DISTINCT period FROM analytics_periods WHERE interval_type = ? ORDER BY period ASC`, [interval], (err, rows) => {
          err ? reject(err) : resolve(rows.map(r => r.period));
        });
      })
    ]).then(([dataRows, labels]) => {
      const dataIndex = new Map();
      dataRows.forEach(row => {
        const topic = (row.topic != null ? String(row.topic) : '').trim();
        const period = row.period != null ? String(row.period) : '';
        const key = `${topic}|${period}`;
        dataIndex.set(key, (dataIndex.get(key) || 0) + (row.cnt || 0));
      });
      const totalCount = dataRows.reduce((s, r) => s + (r.cnt || 0), 0);
      if (labels.length && totalCount === 0) {
        log(`[ANALYTICS-DB] No counts in ${table} — regenerate analytics (Data → Analyze) to populate.`);
      }
      
      const datasets = Array.from(normalizedMap.keys()).map(normalizedTopic => {
        const variants = normalizedMap.get(normalizedTopic) || [];
        return {
          label: normalizedTopic,
          data: labels.map(p => {
            return variants.reduce((sum, variant) => {
              const topic = (variant != null ? String(variant) : '').trim();
              const key = `${topic}|${p}`;
              return sum + (dataIndex.get(key) || 0);
            }, 0);
          })
        };
      });
      
      resolve({ labels, datasets, topics: Array.from(normalizedMap.keys()) });
    }).catch(reject);
  });
}

function loadByGroup(analyticsDb, normalizedMap) {
  return new Promise((resolve, reject) => {
    analyticsDb.all(`
      SELECT DISTINCT group_name FROM analytics_by_group ORDER BY group_name
    `, [], (err, groups) => {
      if (err) {
        reject(err);
        return;
      }
      
      const groupsList = groups.map(r => r.group_name);
      analyticsDb.all('SELECT topic, group_name AS grp, count AS cnt FROM analytics_by_group', [], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        
        resolve({
          topics: Array.from(normalizedMap.keys()),
          groups: groupsList,
          rows: rows.map(r => ({ topic: r.topic, grp: r.grp, cnt: r.cnt })),
          topicVariants: normalizedMap
        });
      });
    });
  });
}

function loadByLanguage(analyticsDb, normalizedMap) {
  return new Promise((resolve, reject) => {
    analyticsDb.all(`
      SELECT DISTINCT language FROM analytics_by_language ORDER BY language
    `, [], (err, languages) => {
      if (err) {
        reject(err);
        return;
      }
      
      const languagesList = languages.map(r => r.language).filter(Boolean);
      analyticsDb.all('SELECT topic, language, count AS cnt FROM analytics_by_language', [], (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        
        resolve({
          topics: Array.from(normalizedMap.keys()),
          languages: languagesList,
          rows: rows.map(r => ({ topic: r.topic, language: r.language, cnt: r.cnt })),
          topicVariants: normalizedMap
        });
      });
    });
  });
}

function loadLanguages(analyticsDb) {
  return new Promise((resolve, reject) => {
    analyticsDb.all('SELECT language, count AS cnt FROM analytics_languages ORDER BY cnt DESC', [], (err, rows) => {
      if (err) {
        reject(err);
        return;
      }
      resolve({ rows });
    });
  });
}

function loadOverview(analyticsDb) {
  return new Promise((resolve, reject) => {
    Promise.all([
      new Promise((resolve, reject) => {
        analyticsDb.all('SELECT key, value FROM analytics_overview', [], (err, rows) => {
          if (err) {
            reject(err);
            return;
          }
          const obj = {};
          rows.forEach(r => obj[r.key] = r.value);
          resolve(obj);
        });
      }),
      new Promise((resolve, reject) => {
        analyticsDb.all('SELECT topic, count FROM analytics_top_topics ORDER BY rank', [], (err, rows) => {
          err ? reject(err) : resolve(rows.map(r => ({ topic: r.topic, count: r.count })));
        });
      }),
      new Promise((resolve, reject) => {
        analyticsDb.all('SELECT topic, focus, count FROM analytics_top_focuses ORDER BY count DESC LIMIT 20', [], (err, rows) => {
          err ? reject(err) : resolve(rows.map(r => ({ topic: r.topic, focus: r.focus, count: r.count })));
        });
      })
    ]).then(([overview, macroTopics, specificFocus]) => {
      resolve({
        coverage: {
          total: parseInt(overview.total || 0),
          with_macro: parseInt(overview.with_macro || 0),
          pct_with_macro: parseFloat(overview.pct_with_macro || 0)
        },
        macroTopicDistribution: macroTopics,
        topSpecificFocus: specificFocus
      });
    }).catch(reject);
  });
}

module.exports = {
  ANALYTICS_DB_PATH,
  generateAnalyticsDatabase,
  loadAnalyticsFromDatabase,
  initAnalyticsDatabase
};
