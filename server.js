require('dotenv').config();
const express = require('express');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

const app = express();
const { PORT, API_BASE, LOCALRUN } = require('./src/server/config');

/** Middleware: reject data actions when LOCALRUN env is not set (production) */
const requireLocalRun = (req, res, next) => {
  if (!LOCALRUN) return res.status(403).json({ error: 'Data actions require LOCALRUN env (local run only)' });
  next();
};

// Initialize SQLite database
const { DB_PATH } = require('./src/core/db');
const db = new sqlite3.Database(DB_PATH);
const { optimizeDatabase } = require('./src/core/db-optimize');
const { loadAnalyticsFromDatabase, generateAnalyticsDatabase } = require('./src/core/analytics-db');
const { runDetectionOnDb, ensureLanguageColumn } = require('./src/core/detect-language');
const { getDistinctTopics, suggestRules } = require('./src/core/normalize-topics-agent');
const { saveRules, applyRules } = require('./src/core/normalize-topics-apply');

// Server glue: config, progress, fetch, meps, parse, speeches-fetch, analytics-cache, historic-meps, init-db, CLI
const { analyticsCache, warmAnalyticsCache, normalizeTopic } = require('./src/server/analytics-cache');
const { createProgressBar, demoProgressBars } = require('./src/server/progress');
const { fetchSpeechContentFromHTML } = require('./src/server/fetch-speech-html');
const { fetchAllMeps } = require('./src/server/meps-api');
const { parseIndividualSpeeches, storeIndividualSpeeches, parseRecentSpeeches, parseAllSpeechesWithContent } = require('./src/server/parse-speeches');
const speechesFetch = require('./src/server/speeches-fetch');
const historicMeps = require('./src/server/historic-meps');
const { runRefreshMepDataset, runGroupNormalizer } = require('./src/server/refresh-mep-dataset');
const { syncMepAffiliationsFromSpeeches } = require('./src/server/sync-mep-affiliations');
const { initDatabase } = require('./src/server/init-db');
const { handleCli } = require('./src/server/cli');

/** Last log line from running Data jobs (polled by frontend console panel) */
let lastJobLogLine = '';

if (handleCli(db)) return;

// Start server after database init
(async () => {
  try {
    await new Promise((resolve, reject) => {
      db.run('PRAGMA busy_timeout = 15000', (err) => (err ? reject(err) : resolve()));
    });
    await initDatabase(db);

    // Serve static assets (static site files located in public directory)
    app.use(express.static(path.join(__dirname, 'public')));

    // GET /api/meps: return all MEPs from DB with speech counts
    app.get('/api/meps', (req, res) => {
      res.setHeader('Cache-Control', 'no-store');
      console.log('[CACHE] Fetching MEPs from database with speech counts...');
      db.all(`
        SELECT 
          m.*,
          COALESCE(COUNT(s.id), 0) as speech_count
        FROM meps m
        LEFT JOIN individual_speeches s ON m.id = s.mep_id
        GROUP BY m.id
        ORDER BY speech_count DESC, m.label ASC
      `, (err, rows) => {
        if (err) {
          console.error('[CACHE] DB error fetching MEPs:', err);
          return res.status(500).json({ error: err.toString() });
        }
        console.log(`[CACHE] Retrieved ${rows.length} MEPs from database with speech counts`);
        
        // Get standardized political groups and role information for all MEPs in a batch
        const mepIds = rows.map(r => r.id);
        db.all(`
          SELECT DISTINCT mep_id, political_group_std, political_group_kind, political_group_raw
          FROM individual_speeches 
          WHERE mep_id IN (${mepIds.map(() => '?').join(',')}) 
          AND political_group_std IS NOT NULL
        `, mepIds, (err2, groupRows) => {
          if (err2) {
            console.error('[CACHE] Error fetching standardized groups:', err2);
            // Continue without standardized groups if there's an error
          }
          
          // Create a map of mep_id -> role/affiliation info
          const roleMap = {};
          if (groupRows) {
            groupRows.forEach(g => {
              if (!roleMap[g.mep_id]) {
                // Determine the best display value based on kind
                let displayValue = g.political_group_std;
                
                if (g.political_group_kind === 'institution') {
                  // For institutions, show a cleaned up version
                  if (g.political_group_raw.includes('Commission')) {
                    displayValue = 'European Commission';
                  } else if (g.political_group_raw.includes('Council')) {
                    displayValue = 'Council of the EU';
                  } else if (g.political_group_raw.includes('High Representative')) {
                    displayValue = 'High Representative';
                  } else {
                    displayValue = 'EU Institution';
                  }
                } else if (g.political_group_kind === 'role') {
                  // For parliamentary roles, show a cleaned up version
                  if (g.political_group_raw.includes('rapporteur')) {
                    displayValue = 'Committee Rapporteur';
                  } else if (g.political_group_raw.includes('Chair') || g.political_group_raw.includes('chair')) {
                    displayValue = 'Committee Chair';
                  } else if (g.political_group_raw.includes('delegat')) {
                    displayValue = 'Delegation Member';
                  } else {
                    displayValue = 'Parliamentary Role';
                  }
                } else if (g.political_group_std === 'NI' && g.political_group_kind === 'group') {
                  displayValue = 'Non-Attached';
                }
                
                roleMap[g.mep_id] = displayValue;
              }
            });
          }
          
          const data = rows.map(r => ({
            id: `person/${r.id}`,
            type: 'Person',
            identifier: r.id.toString(),
            label: r.label,
            familyName: r.familyName,
            givenName: r.givenName,
            sortLabel: r.sortLabel,
            'api:country-of-representation': r.country,
            'api:political-group': roleMap[r.id] || r.politicalGroup || 'No affiliation in speeches',
            isCurrent: Boolean(r.is_current),
            source: r.source || 'api',
            speechCount: r.speech_count
          }));

          // Group affiliations with fewer than 10 members into "Other" for display and export
          const OTHER_THRESHOLD = 10;
          const groupCounts = {};
          data.forEach(m => {
            const g = m['api:political-group'] || 'Unknown';
            groupCounts[g] = (groupCounts[g] || 0) + 1;
          });
          const smallGroups = new Set(Object.entries(groupCounts).filter(([, n]) => n < OTHER_THRESHOLD).map(([g]) => g));
          if (smallGroups.size > 0) {
            data.forEach(m => {
              const g = m['api:political-group'] || 'Unknown';
              if (smallGroups.has(g)) m['api:political-group'] = 'Other';
            });
          }

          res.json({ data });
        });
      });
    });

    // GET /api/meps/:id: return single MEP by ID
    app.get('/api/meps/:id', (req, res) => {
      const id = parseInt(req.params.id, 10);
      db.get('SELECT * FROM meps WHERE id = ?', [id], (err, row) => {
        if (err) {
          console.error('DB error fetching MEP:', err);
          return res.status(500).json({ error: err.toString() });
        }
        if (!row) {
          return res.status(404).json({ error: 'MEP not found' });
        }
        
        // Get standardized political group and role info for this MEP
        db.get(`
          SELECT political_group_std, political_group_kind, political_group_raw
          FROM individual_speeches 
          WHERE mep_id = ? 
          AND political_group_std IS NOT NULL 
          LIMIT 1
        `, [id], (err2, groupRow) => {
          let displayValue = row.politicalGroup || 'Unknown';
          
          if (groupRow) {
            displayValue = groupRow.political_group_std;
            
            if (groupRow.political_group_kind === 'institution') {
              if (groupRow.political_group_raw.includes('Commission')) {
                displayValue = 'European Commission';
              } else if (groupRow.political_group_raw.includes('Council')) {
                displayValue = 'Council of the EU';
              } else if (groupRow.political_group_raw.includes('High Representative')) {
                displayValue = 'High Representative';
              } else {
                displayValue = 'EU Institution';
              }
            } else if (groupRow.political_group_kind === 'role') {
              if (groupRow.political_group_raw.includes('rapporteur')) {
                displayValue = 'Committee Rapporteur';
              } else if (groupRow.political_group_raw.includes('Chair') || groupRow.political_group_raw.includes('chair')) {
                displayValue = 'Committee Chair';
              } else if (groupRow.political_group_raw.includes('delegat')) {
                displayValue = 'Delegation Member';
              } else {
                displayValue = 'Parliamentary Role';
              }
            } else if (groupRow.political_group_std === 'NI' && groupRow.political_group_kind === 'group') {
              displayValue = 'Non-Attached';
            }
          }
          
          const mep = {
            id: `person/${row.id}`,
            type: 'Person',
            identifier: row.id.toString(),
            label: row.label,
            familyName: row.familyName,
            givenName: row.givenName,
            sortLabel: row.sortLabel,
            'api:country-of-representation': row.country,
            'api:political-group': displayValue
          };
          res.json({ data: mep });
        });
      });
    });

    // GET /api/meps/:id/speeches: get all speeches by a specific MEP (optional: ?macro_topic=... to filter by macro topic)
    app.get('/api/meps/:id/speeches', (req, res) => {
      const mepId = parseInt(req.params.id, 10);
      const limit = parseInt(req.query.limit, 10) || 100;
      const offset = parseInt(req.query.offset, 10) || 0;
      const macroTopic = typeof req.query.macro_topic === 'string' && req.query.macro_topic.trim() ? req.query.macro_topic.trim() : null;
      
      console.log(`[MEP-SPEECHES] Fetching speeches for MEP ID: ${mepId} (limit: ${limit}, offset: ${offset}${macroTopic ? `, macro_topic: ${macroTopic}` : ''})`);
      
      // Get MEP info first
      db.get('SELECT * FROM meps WHERE id = ?', [mepId], (err, mep) => {
        if (err) {
          console.error('[MEP-SPEECHES] Error fetching MEP:', err);
          res.status(500).json({ error: err.toString() });
          return;
        }
        if (!mep) {
          res.status(404).json({ error: 'MEP not found' });
          return;
        }
        
        const whereClause = 'WHERE i.mep_id = ?' + (macroTopic ? ' AND TRIM(i.macro_topic) = ?' : '');
        const speechParams = macroTopic ? [mepId, macroTopic, limit, offset] : [mepId, limit, offset];
        const countParams = macroTopic ? [mepId, macroTopic] : [mepId];
        
        // Get speeches for this MEP (optionally filtered by macro topic)
        db.all(`
          SELECT 
            i.id,
            i.speaker_name,
            i.political_group,
            i.title,
            i.speech_content,
            i.speech_order,
            i.language,
            i.macro_topic,
            s.date,
            s.label as sitting_title,
            s.docIdentifier,
            s.notationId
          FROM individual_speeches i
          JOIN sittings s ON i.sitting_id = s.id
          ${whereClause}
          ORDER BY s.date DESC, i.speech_order ASC
          LIMIT ? OFFSET ?
        `, speechParams, (err, speeches) => {
          if (err) {
            console.error('[MEP-SPEECHES] Error fetching speeches:', err);
            res.status(500).json({ error: err.toString() });
            return;
          }
          
          const countSql = 'SELECT COUNT(*) as total FROM individual_speeches i WHERE i.mep_id = ?' + (macroTopic ? ' AND TRIM(i.macro_topic) = ?' : '');
          db.get(countSql, countParams, (err, countRow) => {
            if (err) {
              console.error('[MEP-SPEECHES] Error fetching count:', err);
              res.status(500).json({ error: err.toString() });
              return;
            }
            
            console.log(`[MEP-SPEECHES] Found ${speeches.length} speeches for ${mep.givenName} ${mep.familyName} (total: ${countRow.total})`);
            
            res.json({
              mep: mep,
              speeches: speeches,
              macroTopicFilter: macroTopic || null,
              pagination: {
                total: countRow.total,
                limit: limit,
                offset: offset,
                hasMore: (offset + speeches.length) < countRow.total
              }
            });
          });
        });
      });
    });

    // GET /api/speeches: return cached speeches (optionally filter by MEP, startDate, endDate)
    app.get('/api/speeches', (req, res) => {
        const mepId = req.query.personId;
        const startDate = req.query.startDate;
        const endDate = req.query.endDate;
        const rawLimit = parseInt(req.query.limit, 10);
        const limit = mepId ? (rawLimit || 50) : (rawLimit || 100000);
        const offset = parseInt(req.query.offset, 10) || 0;
      
      console.log(`[CACHE] Fetching speeches - MEP ID: ${mepId || 'all'}, limit: ${limit}, offset: ${offset}${startDate ? `, startDate: ${startDate}` : ''}${endDate ? `, endDate: ${endDate}` : ''}`);
      
      // Select metadata only (no content) — avoid LENGTH(content) (reads blobs, very slow on large DB)
      let query = `
        SELECT s.id, s.type, s.label, s.activity_date, s.docIdentifier, s.notationId,
               COUNT(i.id) as individual_speech_count
        FROM sittings s
        INNER JOIN individual_speeches i ON s.id = i.sitting_id
      `;
      let params = [];
      
      if (mepId) {
        query += ' AND s.personId = ?';
        params.push(parseInt(mepId, 10));
        console.log(`[CACHE] Filtering speeches for MEP ID: ${mepId}`);
      }
      if (startDate) {
        query += ' AND s.activity_date >= ?';
        params.push(startDate);
      }
      if (endDate) {
        query += ' AND s.activity_date <= ?';
        params.push(endDate);
      }
      
      query += ' GROUP BY s.id ORDER BY s.activity_date DESC LIMIT ? OFFSET ?';
      params.push(limit, offset);
      
      // Get total count (sittings with speeches — no content read, fast)
      let countQuery = `SELECT COUNT(DISTINCT s.id) as total FROM sittings s
        INNER JOIN individual_speeches i ON s.id = i.sitting_id WHERE 1=1`;
      let countParams = [];
      if (mepId) {
        countQuery += ' AND s.personId = ?';
        countParams.push(parseInt(mepId, 10));
      }
      if (startDate) {
        countQuery += ' AND s.activity_date >= ?';
        countParams.push(startDate);
      }
      if (endDate) {
        countQuery += ' AND s.activity_date <= ?';
        countParams.push(endDate);
      }
      
      db.get(countQuery, countParams, (err, countRow) => {
        if (err) {
          console.error('[CACHE] DB error getting speech count:', err);
          return res.status(500).json({ error: err.toString() });
        }
        
        const total = countRow.total;
        console.log(`[CACHE] Total speeches with content in database: ${total}`);
        
        db.all(query, params, (err, rows) => {
          if (err) {
            console.error('[CACHE] DB error fetching speeches:', err);
            return res.status(500).json({ error: err.toString() });
          }
          
          console.log(`[CACHE] Retrieved ${rows.length} sittings (metadata only)`);
          
          const data = rows.map(row => ({
            id: row.id,
            type: row.type,
            label: row.label,
            date: row.activity_date,
            activity_date: row.activity_date,
            individual_speech_count: row.individual_speech_count,
            docIdentifier: row.docIdentifier,
            notationId: row.notationId
          }));
          
        res.json({ data, meta: { total } });
        });
      });
    });

    // GET /api/speeches/:id/individual: return individual speeches for a sitting
    app.get('/api/speeches/:id/individual', (req, res) => {
      const sittingId = req.params.id;
      console.log(`[INDIVIDUAL] Fetching individual speeches for sitting: ${sittingId}`);
      
      db.all('SELECT *, language FROM individual_speeches WHERE sitting_id = ? ORDER BY speech_order', [sittingId], (err, rows) => {
        if (err) {
          console.error('[INDIVIDUAL] Database error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        console.log(`[INDIVIDUAL] Found ${rows.length} individual speeches for sitting ${sittingId}`);
        res.json({ 
          sitting_id: sittingId,
          individual_speeches: rows,
          count: rows.length 
        });
      });
    });

    // GET /api/speeches/:id: return detailed speech info from database
    app.get('/api/speeches/:id', async (req, res) => {
      try {
        const rawId = req.params.id;
        const speechId = decodeURIComponent(rawId);
        
        console.log(`[SPEECH] Fetching speech details for ID: ${speechId}`);
        
        // First try to get from database
        db.get('SELECT * FROM sittings WHERE id = ?', [speechId], (err, row) => {
          if (err) {
            console.error('[SPEECH] DB error:', err);
            return res.status(500).json({ error: 'Database error' });
          }
          
          if (row) {
            console.log(`[SPEECH] Found speech in database: ${speechId}`);
            
            // Convert database row to API format — sittings use activity_date, not date
            const dateVal = row.date || row.activity_date || (row.id && String(row.id).startsWith('sitting-') ? String(row.id).replace(/^sitting-/, '') : null);
            const speechData = {
              id: row.id,
              type: row.type,
              label: row.label,
              date: dateVal,
              content: row.content,
              docIdentifier: row.docIdentifier,
              notationId: row.notationId,
              activity_type: row.activity_type,
              activity_date: row.activity_date,
              activity_start_date: row.activity_start_date,
              last_updated: row.last_updated,
              // Add some mock fields for compatibility
              had_activity_type: row.activity_type,
              recorded_in_a_realization_of: row.docIdentifier ? [{
                identifier: row.docIdentifier,
                notation_speechId: row.notationId
              }] : []
            };
            
            return res.json(speechData);
          }
          
          // Fallback to remote API if not in database
          console.log(`[SPEECH] Speech not in database, falling back to remote API: ${speechId}`);
        const lang = req.query.lang || req.query['search-language'] || 'EN';
        const params = { 'search-language': lang, format: 'application/ld+json' };
        if (req.query.text) params.text = req.query.text;
        if (req.query['include-output']) params['include-output'] = req.query['include-output'];
          
          axios.get(`${API_BASE}/speeches/${speechId}`, {
          params,
          headers: { Accept: 'application/ld+json' }
          }).then(response => {
        res.json(response.data);
          }).catch(error => {
            console.error('[SPEECH] Remote API error:', error.toString());
            res.status(500).json({ error: error.toString() });
          });
        });
      } catch (error) {
        console.error('[SPEECH] Error:', error.toString());
        res.status(500).json({ error: error.toString() });
      }
    });

    // =============================================
    // Analytics Endpoints
    // =============================================
    // GET /api/analytics/overview
    app.get('/api/analytics/overview', (req, res) => {
      // Serve from cache if available
      if (analyticsCache.data) {
        console.log('⚡ [CACHE] Served overview from cache');
        return res.json(analyticsCache.data.overview);
      }
      
      const topLimit = parseInt(req.query.limit, 10) || 20;
      const trendMonths = parseInt(req.query.months, 10) || 12;

      const result = { coverage: {}, macroTopicDistribution: [], topSpecificFocus: [], trendsMonthly: [] };

      // 1) Coverage
      db.get(`
        SELECT 
          COUNT(*) AS total,
          SUM(CASE WHEN macro_topic IS NOT NULL AND TRIM(macro_topic) <> '' THEN 1 ELSE 0 END) AS with_macro
        FROM individual_speeches
      `, [], (err1, cov) => {
        if (err1) return res.status(500).json({ error: err1.message });
        const total = cov?.total || 0;
        const withMacro = cov?.with_macro || 0;
        const pct = total ? Math.round((withMacro / total) * 1000) / 10 : 0;
        result.coverage = { total, with_macro: withMacro, pct_with_macro: pct };

        // 2) Macro Topic Distribution (top N)
        db.all(`
          SELECT macro_topic AS topic, COUNT(*) AS count
          FROM individual_speeches
          WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
          GROUP BY macro_topic
          ORDER BY count DESC
          LIMIT ?
        `, [topLimit], (err2, rowsTopic) => {
          if (err2) return res.status(500).json({ error: err2.message });
          result.macroTopicDistribution = rowsTopic || [];

          // 3) Top Specific Focus (overall top N)
          db.all(`
            SELECT macro_topic AS topic, macro_specific_focus AS focus, COUNT(*) AS count
            FROM individual_speeches
            WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
              AND macro_specific_focus IS NOT NULL AND TRIM(macro_specific_focus) <> ''
            GROUP BY macro_topic, macro_specific_focus
            ORDER BY count DESC
            LIMIT ?
          `, [topLimit], (err3, rowsFocus) => {
            if (err3) return res.status(500).json({ error: err3.message });
            result.topSpecificFocus = rowsFocus || [];

            // 4) Trends for last X months for top 5 topics
            const top5 = (rowsTopic || []).slice(0, 5).map(r => r.topic).filter(Boolean);
            if (top5.length === 0) return res.json(result);

            // Build monthly trend and then trim to last N months
            const placeholders = top5.map(() => '?').join(',');
            db.all(`
              SELECT substr(s.activity_date, 1, 7) AS ym, i.macro_topic AS topic, COUNT(*) AS count
              FROM individual_speeches i
              JOIN sittings s ON s.id = i.sitting_id
              WHERE i.macro_topic IN (${placeholders})
                AND s.activity_date IS NOT NULL
              GROUP BY ym, i.macro_topic
              ORDER BY ym ASC
            `, top5, (err4, rowsTrend) => {
              if (err4) return res.status(500).json({ error: err4.message });
              // Keep only last N months
              const months = Array.from(new Set((rowsTrend || []).map(r => r.ym))).sort();
              const lastMonths = months.slice(-trendMonths);
              result.trendsMonthly = (rowsTrend || []).filter(r => lastMonths.includes(r.ym));
              res.json(result);
            });
          });
        });
      });
    });

    // Cache status endpoint
    app.get('/api/analytics/cache-status', (req, res) => {
      res.json({
        ready: analyticsCache.data !== null,
        warming: analyticsCache.isWarming,
        lastUpdated: analyticsCache.lastUpdated,
        progress: analyticsCache.progress
      });
    });

    // Trigger cache warm on demand (e.g. when user opens Descriptive Analytics tab).
    // Loads from existing analytics DB if present; does NOT regenerate (use Data → Analyze for that).
    app.post('/api/analytics/warm', async (req, res) => {
      if (analyticsCache.data) {
        return res.json({ started: false, ready: true, message: 'Cache already ready' });
      }
      if (analyticsCache.isWarming) {
        return res.json({ started: false, warming: true, message: 'Cache warming in progress' });
      }
      const { ANALYTICS_DB_PATH } = require('./src/core/analytics-db');
      if (fs.existsSync(ANALYTICS_DB_PATH)) {
        try {
          await warmAnalyticsCache(db);
          return res.json({ started: true, ready: true, message: 'Analytics loaded from database' });
        } catch (err) {
          console.error('[CACHE] Error loading analytics database:', err);
          // Fall through to compute on the fly
        }
      }
      warmAnalyticsCache(db).catch(err => console.error('[CACHE] Warm failed:', err));
      res.json({ started: true, message: 'Cache warming started' });
    });

    // GET /api/analytics/time-series?interval=month&from=YYYY-MM&to=YYYY-MM&top=5
    app.get('/api/analytics/time-series', (req, res) => {
      const startTime = Date.now();
      const interval = (req.query.interval || 'month').toLowerCase();
      const from = req.query.from || null; // 'YYYY-MM' or 'YYYY'
      const to = req.query.to || null;     // 'YYYY-MM' or 'YYYY'
      
      // Serve from cache if available (with optional from/to filter)
      if (analyticsCache.data) {
        const cached = analyticsCache.data[`timeseries_${interval}`];
        if (cached) {
          let out = cached;
          if (from || to) {
            const labels = (cached.labels || []).filter(p => {
              if (from && p < from) return false;
              if (to && p > to) return false;
              return true;
            });
            out = {
              labels,
              datasets: (cached.datasets || []).map(ds => ({
                label: ds.label,
                data: labels.map(period => {
                  const i = (cached.labels || []).indexOf(period);
                  return i >= 0 && ds.data ? ds.data[i] : 0;
                })
              })),
              topics: cached.topics
            };
          }
          const totalTime = Date.now() - startTime;
          console.log(`⚡ [CACHE] Served time-series from cache in ${totalTime}ms${from || to ? ` (filtered ${from || ''}-${to || ''})` : ''}`);
          return res.json(out);
        }
      }
      
      const top = req.query.top ? Math.max(1, parseInt(req.query.top, 10) || 1) : null; // if missing => ALL
      const returnAll = String(req.query.all || '').toLowerCase() === 'true' || req.query.all === '1';
      // topics can be CSV or JSON array
      let topicsFilter = null;
      if (req.query.topics) {
        try {
          topicsFilter = Array.isArray(req.query.topics)
            ? req.query.topics
            : (String(req.query.topics).trim().startsWith('[')
                ? JSON.parse(String(req.query.topics))
                : String(req.query.topics).split(',').map(s => s.trim()).filter(Boolean));
        } catch (_) { topicsFilter = null; }
      }

      let periodExpr = `substr(s.activity_date,1,7)`; // month
      if (interval === 'year') periodExpr = `substr(s.activity_date,1,4)`;
      if (interval === 'quarter') periodExpr = `substr(s.activity_date,1,4) || '-Q' || ((cast(substr(s.activity_date,6,2) as integer)+2)/3)`;

      const where = ['s.activity_date IS NOT NULL'];
      const params = [];
      if (from) { where.push(`${periodExpr} >= ?`); params.push(from); }
      if (to) { where.push(`${periodExpr} <= ?`); params.push(to); }

      const whereSql = where.length ? 'WHERE ' + where.join(' AND ') : '';

      const useTopics = async (topics) => {
        // For SQL query, we need to match both normalized and original forms
        // Get all distinct macro topics from the database
        const allTopicsSql = `
          SELECT DISTINCT i.macro_topic AS topic
          FROM individual_speeches i
          JOIN sittings s ON s.id = i.sitting_id
          ${whereSql}
          AND i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
        `;
        db.all(allTopicsSql, params, (errAllTopics, allTopicsRows) => {
          if (errAllTopics) return res.status(500).json({ error: errAllTopics.message });
          
          // Map normalized topics to all their variants in DB
          const topicVariants = new Map();
          topics.forEach(normalizedTopic => {
            const variants = allTopicsRows
              .filter(row => normalizeTopic(row.topic) === normalizedTopic)
              .map(row => row.topic);
            topicVariants.set(normalizedTopic, variants);
          });
          
          // Flatten all variants for the SQL query
          const allVariants = Array.from(topicVariants.values()).flat();
          if (allVariants.length === 0) {
            return res.json({ labels: [], datasets: [], topics });
          }
          
          const placeholders = allVariants.map(()=>'?').join(',');
          const params2 = [...params, ...allVariants];
          const sql = `
            SELECT ${periodExpr} AS period, i.macro_topic AS topic, COUNT(*) AS cnt
            FROM individual_speeches i
            JOIN sittings s ON s.id = i.sitting_id
            ${whereSql} AND i.macro_topic IN (${placeholders})
            GROUP BY period, i.macro_topic
            ORDER BY period ASC
          `;
          db.all(sql, params2, (err, rows) => {
            if (err) return res.status(500).json({ error: err.message });
            
            // Get all periods from the entire database to ensure full timeline
            const allPeriodsSql = `
              SELECT DISTINCT ${periodExpr} AS period
              FROM individual_speeches i
              JOIN sittings s ON s.id = i.sitting_id
              WHERE s.activity_date IS NOT NULL
              ORDER BY period ASC
            `;
            db.all(allPeriodsSql, [], (errPeriods, periodRows) => {
              if (errPeriods) return res.status(500).json({ error: errPeriods.message });
              
              // Use ALL periods from database for complete timeline
              const labels = (periodRows || []).map(r => r.period);
              
              // Aggregate counts by normalized topic
              const datasets = topics.map(normalizedTopic => {
                const variants = topicVariants.get(normalizedTopic) || [];
                return {
                  label: normalizedTopic,
                  data: labels.map(p => {
                    // Sum counts for all variants of this normalized topic
                    const matchingRows = rows.filter(x => 
                      x.period === p && variants.includes(x.topic)
                    );
                    return matchingRows.reduce((sum, r) => sum + r.cnt, 0);
                  })
                };
              });
              const totalTime = Date.now() - startTime;
              console.log(`[SERVER] /api/analytics/time-series completed in ${totalTime}ms (${topics.length} topics, ${labels.length} periods)`);
              res.json({ labels, datasets, topics });
            });
          });
        });
      };

      if (topicsFilter && topicsFilter.length) {
        return useTopics(topicsFilter);
      }

      // 1) Determine topics (either ALL in range, or top N)
      const topicSqlAll = `
        SELECT DISTINCT i.macro_topic AS topic
        FROM individual_speeches i
        JOIN sittings s ON s.id = i.sitting_id
        ${whereSql}
        AND i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
      `;
      const topicSqlTop = `
        SELECT i.macro_topic AS topic, COUNT(*) AS cnt
        FROM individual_speeches i
        JOIN sittings s ON s.id = i.sitting_id
        ${whereSql}
        AND i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
        GROUP BY i.macro_topic
        ORDER BY cnt DESC
        LIMIT ${top || 50}
      `;

      db.all(returnAll || !top ? topicSqlAll : topicSqlTop, params, (errTop, rows) => {
        if (errTop) return res.status(500).json({ error: errTop.message });
        // Normalize and deduplicate topics
        const rawTopics = (rows||[]).map(r => r.topic).filter(Boolean);
        const normalizedMap = new Map();
        rawTopics.forEach(topic => {
          const normalized = normalizeTopic(topic);
          if (!normalizedMap.has(normalized)) {
            normalizedMap.set(normalized, topic); // Keep first occurrence
          }
        });
        const topics = Array.from(normalizedMap.keys());
        if (topics.length === 0) return res.json({ labels: [], datasets: [] });
        return useTopics(topics);
      });
    });

    // GET /api/analytics/by-group?topTopics=10&topGroups=10&topics=...
    app.get('/api/analytics/by-group', (req, res) => {
      // Check if specific topics are requested
      let topicsFilter = null;
      if (req.query.topics) {
        try {
          topicsFilter = Array.isArray(req.query.topics)
            ? req.query.topics
            : (String(req.query.topics).trim().startsWith('[')
                ? JSON.parse(String(req.query.topics))
                : String(req.query.topics).split(',').map(s => s.trim()).filter(Boolean));
        } catch (_) { topicsFilter = null; }
      }
      
      // Serve from cache (with optional filtering)
      if (analyticsCache.data) {
        const cached = analyticsCache.data.byGroup;
        
        if (!topicsFilter || topicsFilter.length === 0) {
          // No filter - return full cache
          console.log('⚡ [CACHE] Served by-group from cache (all topics)');
          return res.json(cached);
        }
        
        // Filter cached data by selected topics (rows are already normalized)
        const filteredRows = cached.rows.filter(row => 
          topicsFilter.includes(row.topic)
        );
        
        const filteredTopics = topicsFilter.filter(t => 
          cached.topics.includes(t)
        );
        
        console.log(`⚡ [CACHE] Served by-group from cache (filtered to ${filteredTopics.length} topics)`);
        return res.json({
          topics: filteredTopics,
          groups: cached.groups,
          rows: filteredRows
        });
      }
      
      // Fallback to database if cache not ready
      const topTopics = Math.max(1, parseInt(req.query.topTopics, 10) || 10);
      const topGroups = Math.max(1, parseInt(req.query.topGroups, 10) || 10);
      
      const processWithTopics = (topics) => {
        // top groups
        db.all(`
          SELECT COALESCE(political_group_std, political_group) AS grp, COUNT(*) AS cnt
          FROM individual_speeches
          WHERE COALESCE(political_group_std, political_group) IS NOT NULL AND TRIM(COALESCE(political_group_std, political_group))<>''
          GROUP BY grp
          ORDER BY cnt DESC
          LIMIT ?
        `, [topGroups], (e2, grows) => {
          if (e2) return res.status(500).json({ error: e2.message });
          const groups = (grows||[]).map(r=>r.grp);
          const placeholdersT = topics.map(()=>'?').join(',');
          const placeholdersG = groups.map(()=>'?').join(',');
          const params = [...topics, ...groups];
          db.all(`
            SELECT i.macro_topic AS topic, COALESCE(i.political_group_std, i.political_group) AS grp, COUNT(*) AS cnt
            FROM individual_speeches i
            WHERE i.macro_topic IN (${placeholdersT})
              AND COALESCE(i.political_group_std, i.political_group) IN (${placeholdersG})
            GROUP BY i.macro_topic, COALESCE(i.political_group_std, i.political_group)
          `, params, (e3, rows) => {
            if (e3) return res.status(500).json({ error: e3.message });
            res.json({ topics, groups, rows });
          });
        });
      };
      
      // If topics filter is provided, use it directly; otherwise get top N
      if (topicsFilter && topicsFilter.length > 0) {
        return processWithTopics(topicsFilter);
      }
      
      // Get top topics
      db.all(`
        SELECT i.macro_topic AS topic, COUNT(*) AS cnt
        FROM individual_speeches i
        WHERE i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
        GROUP BY i.macro_topic
        ORDER BY cnt DESC
        LIMIT ?
      `, [topTopics], (e1, trows) => {
        if (e1) return res.status(500).json({ error: e1.message });
        const topics = (trows||[]).map(r=>r.topic);
        if (!topics.length) return res.json({ groups: [], topics: [], rows: [] });
        processWithTopics(topics);
      });
    });

    // GET /api/analytics/by-language?topTopics=10&topLanguages=24&topics=...
    app.get('/api/analytics/by-language', (req, res) => {
      let topicsFilter = null;
      if (req.query.topics) {
        try {
          topicsFilter = Array.isArray(req.query.topics)
            ? req.query.topics
            : (String(req.query.topics).trim().startsWith('[')
                ? JSON.parse(String(req.query.topics))
                : String(req.query.topics).split(',').map(s => s.trim()).filter(Boolean));
        } catch (_) { topicsFilter = null; }
      }
      
      if (analyticsCache.data) {
        const cached = analyticsCache.data.byLanguage;
        if (!cached) return res.status(500).json({ error: 'Analytics cache missing byLanguage' });
        
        if (!topicsFilter || topicsFilter.length === 0) {
          console.log('⚡ [CACHE] Served by-language from cache (all topics)');
          return res.json(cached);
        }
        
        const filteredRows = cached.rows.filter(row => topicsFilter.includes(row.topic));
        const filteredTopics = topicsFilter.filter(t => cached.topics.includes(t));
        console.log(`⚡ [CACHE] Served by-language from cache (filtered to ${filteredTopics.length} topics)`);
        return res.json({
          topics: filteredTopics,
          languages: cached.languages,
          rows: filteredRows
        });
      }
      
      const topTopics = Math.max(1, parseInt(req.query.topTopics, 10) || 10);
      const topLanguages = Math.max(1, parseInt(req.query.topLanguages, 10) || 24);
      
      const processWithTopics = (topics) => {
        db.all(`
          SELECT UPPER(COALESCE(language,'UNK')) AS language, COUNT(*) AS cnt
          FROM individual_speeches
          GROUP BY UPPER(COALESCE(language,'UNK'))
          ORDER BY cnt DESC LIMIT ?
        `, [topLanguages], (e2, lrows) => {
          if (e2) return res.status(500).json({ error: e2.message });
          const languages = (lrows || []).map(r => r.language).filter(Boolean);
          const placeholdersT = topics.map(() => '?').join(',');
          const placeholdersL = languages.map(() => '?').join(',');
          db.all(`
            SELECT i.macro_topic AS topic, UPPER(COALESCE(i.language,'UNK')) AS language, COUNT(*) AS cnt
            FROM individual_speeches i
            WHERE i.macro_topic IN (${placeholdersT})
              AND UPPER(COALESCE(i.language,'UNK')) IN (${placeholdersL})
            GROUP BY i.macro_topic, UPPER(COALESCE(i.language,'UNK'))
          `, [...topics, ...languages], (e3, rows) => {
            if (e3) return res.status(500).json({ error: e3.message });
            res.json({ topics, languages, rows });
          });
        });
      };
      
      if (topicsFilter && topicsFilter.length > 0) {
        return processWithTopics(topicsFilter);
      }
      
      db.all(`
        SELECT i.macro_topic AS topic, COUNT(*) AS cnt
        FROM individual_speeches i
        WHERE i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
        GROUP BY i.macro_topic
        ORDER BY cnt DESC LIMIT ?
      `, [topTopics], (e1, trows) => {
        if (e1) return res.status(500).json({ error: e1.message });
        const topics = (trows || []).map(r => r.topic);
        if (!topics.length) return res.json({ languages: [], topics: [], rows: [] });
        processWithTopics(topics);
      });
    });

    // GET /api/analytics/languages?topics=...
    app.get('/api/analytics/languages', (req, res) => {
      // Check if specific topics are requested
      let topicsFilter = null;
      if (req.query.topics) {
        try {
          topicsFilter = Array.isArray(req.query.topics)
            ? req.query.topics
            : (String(req.query.topics).trim().startsWith('[')
                ? JSON.parse(String(req.query.topics))
                : String(req.query.topics).split(',').map(s => s.trim()).filter(Boolean));
        } catch (_) { topicsFilter = null; }
      }
      
      // Serve from cache (with optional filtering)
      if (analyticsCache.data) {
        if (!topicsFilter || topicsFilter.length === 0) {
          // No filter - return full cache
          console.log('⚡ [CACHE] Served languages from cache (all topics)');
          return res.json(analyticsCache.data.languages);
        }
        
        // For filtered languages, we need to query the database with topic filter
        // This is because the cache doesn't store per-topic language breakdown
        console.log('[QUERY] Computing languages for filtered topics');
      }
      
      // Query database for filtered topics or if cache not ready
      let sql = `
        SELECT UPPER(COALESCE(language,'UNK')) AS language, COUNT(*) AS cnt
        FROM individual_speeches
      `;
      let params = [];
      
      if (topicsFilter && topicsFilter.length > 0) {
        // Need to get all variants of the normalized topics
        if (analyticsCache.data) {
          const topicVariants = analyticsCache.data.topicVariants;
          const allVariants = topicsFilter.flatMap(t => topicVariants.get(t) || [t]);
          const placeholders = allVariants.map(()=>'?').join(',');
          sql += ` WHERE macro_topic IN (${placeholders})`;
          params = allVariants;
        } else {
          const placeholders = topicsFilter.map(()=>'?').join(',');
          sql += ` WHERE macro_topic IN (${placeholders})`;
          params = topicsFilter;
        }
      }
      
      sql += ` GROUP BY UPPER(COALESCE(language,'UNK')) ORDER BY cnt DESC`;
      
      db.all(sql, params, (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ rows });
      });
    });

    // GET /api/analytics/top-meps?topic=...&top=10
    app.get('/api/analytics/top-meps', (req, res) => {
      const topic = req.query.topic || null;
      const top = Math.max(1, Math.min(100, parseInt(req.query.top, 10) || 10));
      const params = [];
      let where = 'WHERE 1=1';
      if (topic) { where += ' AND i.macro_topic = ?'; params.push(topic); }
      const sql = `
        SELECT
          m.id,
          COALESCE(m.label, i.speaker_name) AS label,
          m.country,
          COALESCE(i.political_group_std, i.political_group) AS grp,
          COUNT(*) AS cnt
        FROM individual_speeches i
        LEFT JOIN meps m ON m.id = i.mep_id
        ${where}
        GROUP BY m.id, COALESCE(m.label, i.speaker_name), m.country, COALESCE(i.political_group_std, i.political_group)
        ORDER BY cnt DESC
        LIMIT ${top}
      `;
      db.all(sql, params, (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ rows });
      });
    });

    // Endpoint: Export speeches to CSV
    app.get('/api/export/speeches', (req, res) => {
      const requestStartTime = Date.now();
      console.log('[EXPORT] ========================================');
      console.log('[EXPORT] Export request received');
      
      const { startDate, endDate, fields, countOnly } = req.query;
      console.log(`[EXPORT] Query params - startDate: ${startDate}, endDate: ${endDate}, fields: ${fields ? fields.substring(0, 50) + '...' : 'default'}, countOnly: ${countOnly}`);
      
      // Build WHERE clause for date filtering
      const params = [];
      let whereClauses = [];
      
      if (startDate) {
        whereClauses.push('s.activity_date >= ?');
        params.push(startDate);
        console.log(`[EXPORT] Adding start date filter: ${startDate}`);
      }
      
      if (endDate) {
        whereClauses.push('s.activity_date <= ?');
        params.push(endDate);
        console.log(`[EXPORT] Adding end date filter: ${endDate}`);
      }
      
      const whereClause = whereClauses.length > 0 ? 'WHERE ' + whereClauses.join(' AND ') : '';
      console.log(`[EXPORT] WHERE clause: ${whereClause || '(none - all data)'}`);
      
      // If only count is requested
      if (countOnly === 'true') {
        console.log('[EXPORT] Count-only request');
        const countSql = `
          SELECT COUNT(*) as count
          FROM individual_speeches i
          LEFT JOIN sittings s ON i.sitting_id = s.id
          ${whereClause}
        `;
        
        console.log('[EXPORT] Executing count query...');
        const queryStartTime = Date.now();
        
        db.get(countSql, params, (err, row) => {
          const queryTime = Date.now() - queryStartTime;
          const totalTime = Date.now() - requestStartTime;
          
          if (err) {
            console.error('[EXPORT] Error counting speeches:', err);
            console.log(`[EXPORT] Failed after ${totalTime}ms`);
            return res.status(500).json({ error: err.message });
          }
          console.log(`[EXPORT] Count query completed: ${row.count} speeches`);
          console.log(`[EXPORT] Query time: ${queryTime}ms`);
          console.log(`[EXPORT] Total count request time: ${totalTime}ms`);
          res.json({ count: row.count });
        });
        return;
      }
      
      // Parse requested fields
      console.log('[EXPORT] Full CSV export request');
      const requestedFields = fields ? fields.split(',') : [
        // Default fields if none specified
        'id', 'sitting_id', 'date', 'speaker_name', 'political_group', 
        'title', 'speech_content', 'language', 'macro_topic', 'specific_focus',
        'topic', 'country', 'mep_id'
      ];
      
      console.log(`[EXPORT] Requested ${requestedFields.length} fields: ${requestedFields.join(', ')}`);
      
      // Map field names to SQL columns
      const fieldMapping = {
        // Basic Information
        'id': 'i.id',
        'sitting_id': 'i.sitting_id',
        'date': 's.activity_date',
        'activity_start_date': 's.activity_start_date',
        'activity_type': 's.activity_type',
        'speech_order': 'i.speech_order',
        'created_at': 'i.created_at',
        
        // Speaker Information
        'speaker_name': 'i.speaker_name',
        'mep_id': 'i.mep_id',
        'country': 'm.country',
        'political_group': 'COALESCE(i.political_group_std, i.political_group)',
        'political_group_raw': 'i.political_group_raw',
        'political_group_std': 'i.political_group_std',
        'political_group_kind': 'i.political_group_kind',
        'political_group_reason': 'i.political_group_reason',
        
        // Content
        'title': 'i.title',
        'speech_content': 'i.speech_content',
        'language': 'i.language',
        'sitting_content': 's.content',
        'sitting_label': 's.label',
        'sitting_type': 's.type',
        'doc_identifier': 's.docIdentifier',
        'notation_id': 's.notationId',
        
        // Topic Classification
        'topic': 'i.topic',
        'macro_topic': 'i.macro_topic',
        'specific_focus': 'i.macro_specific_focus',
        'macro_confidence': 'i.macro_confidence',
        'macro_classified_by': 'i.macro_classified_by',
        'macro_classified_at': 'i.macro_classified_at',
        'macro_classification_cost': 'i.macro_classification_cost'
      };
      
      // Build SELECT clause
      const selectFields = requestedFields
        .filter(f => fieldMapping[f])
        .map(f => `${fieldMapping[f]} as ${f}`);
      
      console.log(`[EXPORT] Mapped to ${selectFields.length} SQL fields`);
      
      if (selectFields.length === 0) {
        console.error('[EXPORT] No valid fields selected');
        return res.status(400).json({ error: 'No valid fields selected' });
      }
      
      const baseSql = `
        SELECT ${selectFields.join(', ')}
        FROM individual_speeches i
        LEFT JOIN sittings s ON i.sitting_id = s.id
        LEFT JOIN meps m ON i.mep_id = m.id
        ${whereClause}
        ORDER BY s.activity_date DESC, i.speech_order ASC
      `;
      
      console.log(`[EXPORT] Executing batch streaming export...`);
      console.log(`[EXPORT] SQL query length: ${baseSql.length} chars`);
      
      // Helper function to escape CSV values
      const escapeCSV = (value) => {
        if (value === null || value === undefined) return '';
        const str = String(value);
        // If contains comma, quote, or newline, wrap in quotes and escape quotes
        if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
          return '"' + str.replace(/"/g, '""') + '"';
        }
        return str;
      };
      
      // Set response headers for streaming CSV
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', 'attachment; filename="eu_speeches_export.csv"');
      
      // Write CSV header with BOM for Excel compatibility
      const headers = requestedFields.join(',');
      res.write('\ufeff' + headers + '\n');
      
      console.log('[EXPORT] Starting batch streaming export...');
      const queryStartTime = Date.now();
      
      const BATCH_SIZE = 5000; // Process 5000 rows at a time for better performance
      let offset = 0;
      let totalRowCount = 0;
      let totalBytesWritten = 0;
      let lastLogTime = Date.now();
      let hasError = false;
      
      // Recursive function to process batches with backpressure handling
      function processBatch() {
        if (hasError) return;
        
        const batchSql = baseSql + ` LIMIT ${BATCH_SIZE} OFFSET ${offset}`;
        
        db.all(batchSql, params, (err, rows) => {
          if (err) {
            console.error('[EXPORT] Error fetching batch:', err);
            hasError = true;
            if (!res.headersSent) {
              return res.status(500).json({ error: err.message });
            }
            return res.end();
          }
          
          // If no rows, we're done
          if (rows.length === 0) {
            const queryTime = Date.now() - queryStartTime;
            const totalTime = Date.now() - requestStartTime;
            const sizeMB = (totalBytesWritten / 1024 / 1024).toFixed(2);
            const avgRate = totalRowCount / (queryTime / 1000);
            
            // Finalize the response
            res.end();
            
            console.log(`[EXPORT] Stream completed successfully`);
            console.log(`[EXPORT] Total rows exported: ${totalRowCount}`);
            console.log(`📦 [EXPORT] Total size: ${totalBytesWritten} bytes (${sizeMB} MB)`);
            console.log(`[EXPORT] Query + streaming time: ${queryTime}ms (${(queryTime/1000).toFixed(2)}s)`);
            console.log(`📈 [EXPORT] Average rate: ${avgRate.toFixed(1)} rows/sec`);
            console.log('[EXPORT] ========================================');
            return;
          }
          
          // Convert batch to CSV - optimized for speed
          let batchCSV = '';
          for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            for (let j = 0; j < requestedFields.length; j++) {
              if (j > 0) batchCSV += ',';
              batchCSV += escapeCSV(row[requestedFields[j]]);
            }
            batchCSV += '\n';
          }
          
          // Update counters
          totalRowCount += rows.length;
          totalBytesWritten += batchCSV.length;
          offset += rows.length;
          
          // Log progress every 5000 rows
          const now = Date.now();
          if (totalRowCount % 5000 === 0 || now - lastLogTime > 5000) {
            const elapsed = (now - queryStartTime) / 1000;
            const rate = totalRowCount / elapsed;
            const sizeMB = (totalBytesWritten / 1024 / 1024).toFixed(2);
            console.log(`[EXPORT] Progress: ${totalRowCount} rows, ${sizeMB} MB, ${rate.toFixed(1)} rows/sec`);
            lastLogTime = now;
          }
          
          // Write batch with backpressure handling
          const canContinue = res.write(batchCSV);
          
          if (!canContinue) {
            // Buffer is full, wait for drain event
            res.once('drain', () => {
              // Continue processing after drain
              setImmediate(processBatch);
            });
          } else {
            // Continue immediately
            setImmediate(processBatch);
          }
        });
      }
      
      // Start processing batches
      processBatch();
    });

    // Endpoint: fetch and parse table of contents for a given date
    app.get('/api/speech-toc', async (req, res) => {
      const { date } = req.query;
      if (!date) return res.status(400).json({ error: 'Missing date' });
      try {
        const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
        const response = await axios.get(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
          }
        });
        const html = response.data;
        const $ = require('cheerio').load(html);
        // Find the table of contents (usually a <ul> or <ol> with links to #creitemX)
        let toc = [];
        let found = false;
        $('a[href^="#creitem"]').each((i, el) => {
          const anchor = $(el).attr('href');
          const title = $(el).text().trim();
          if (anchor && title) {
            toc.push({ anchor, title, index: i });
            found = true;
          }
        });
        if (!found || toc.length === 0) {
          console.error('No TOC items found for date', date);
          return res.status(404).json({ error: 'No table of contents found for this date.' });
        }
        res.json({ toc });
      } catch (err) {
        console.error('TOC fetch failed:', err.toString());
        res.status(500).json({ error: 'Failed to fetch or parse TOC', details: err.toString() });
      }
    });

    // Endpoint: fetch and extract content for a specific anchor (speech) on a given date
    app.get('/api/speech-content-by-anchor', async (req, res) => {
      const { date, anchor } = req.query;
      if (!date || !anchor) return res.status(400).json({ error: 'Missing date or anchor' });
      try {
        const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
        const response = await axios.get(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)'
          }
        });
        const html = response.data;
        const $ = require('cheerio').load(html);
        // Find the anchor element
        const anchorElem = $(anchor);
        if (!anchorElem.length) {
          console.error('Anchor not found:', anchor, 'for date', date);
          return res.status(404).json({ error: 'Anchor not found in document.' });
        }
        // Collect all elements from anchor to next anchor or end of document
        let contentHtml = '';
        let contentText = '';
        let found = false;
        let next = anchorElem[0];
        while (next) {
          // Stop if we hit the next anchor
          if (found && next.attribs && next.attribs.id && next.attribs.id.startsWith('creitem')) break;
          // Skip the anchor itself if it's not a content node
          if (next !== anchorElem[0] || anchorElem[0].type !== 'tag' || anchorElem[0].name !== 'a') {
            contentHtml += $.html(next);
            contentText += $(next).text() + '\n';
          }
          found = true;
          next = next.nextSibling;
        }
        if (!contentHtml) {
          console.error('No content found for anchor:', anchor, 'on date', date);
          // Fallback: extract all <p> text from the document (like /api/speech-html-content)
          let paragraphs = $('p').toArray().map(p => $(p).text().trim()).filter(Boolean);
          let fallbackText = paragraphs.join('\n\n');
          if (!fallbackText || fallbackText.length < 100) {
            // Fallback: extract all text from <body>
            fallbackText = $('body').text().replace(/\s+/g, ' ').trim();
            fallbackText = fallbackText.slice(0, 2000);
            console.log('Fallback to <body> text:', fallbackText.slice(0, 200));
          } else {
            fallbackText = fallbackText.slice(0, 2000);
            console.log('Fallback to <p> text:', fallbackText.slice(0, 200));
          }
          return res.json({ html: '', text: fallbackText || null, fallback: true });
        }
        res.json({ html: contentHtml, text: contentText });
      } catch (err) {
        console.error('Speech content by anchor fetch failed:', err.toString());
        res.status(500).json({ error: 'Failed to fetch or parse speech content by anchor', details: err.toString() });
      }
    });

    // Start listening (analytics cache warms on demand when user clicks Calculate)
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`Server is running on http://localhost:${PORT}`);
    });
  } catch (e) {
    console.error('Failed to initialize application:', e);
  }
})();

const cheerio = require('cheerio');

app.get('/api/speech-preview', async (req, res) => {
  const { date } = req.query;
  if (!date) return res.status(400).json({ error: 'Missing date' });

  try {
    const url = `https://www.europarl.europa.eu/doceo/document/CRE-10-${date}_EN.html`;
    const response = await axios.get(url);
    const html = response.data;
    const $ = cheerio.load(html);

    const paragraphs = $('p').toArray().map(p => $(p).text().trim());
    const preview = paragraphs.slice(0, 3).join(' ').substring(0, 300);

    res.json({ preview });
  } catch (err) {
    console.error('Preview fetch failed:', err.toString());
    res.status(500).json({ error: 'Could not fetch preview' });
  }
});

// New endpoint: fetch speech content from database (preferred) or fallback to HTML
app.get('/api/speech-html-content', async (req, res) => {
  const { date, speechId } = req.query;
  if (!date) return res.status(400).json({ error: 'Missing date' });
  
  console.log(`[SPEECH] Fetching content for date: ${date}, speechId: ${speechId || 'all'}`);
  
  try {
    // First, try to get content from database
    if (speechId) {
      // Get specific speech content
      db.get('SELECT content FROM sittings WHERE id = ? AND content != "" AND LENGTH(content) > 100', [speechId], (err, row) => {
        if (err) {
          console.error('[SPEECH] DB error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        if (row && row.content) {
          console.log(`[SPEECH] Found content in database for speech ${speechId} (${row.content.length} chars)`);
          return res.json({ content: row.content });
        }
        
        // Fallback to HTML if no database content
        console.log(`[SPEECH] No database content for ${speechId}, falling back to HTML`);
        speechesFetch.fetchFromHTML(date, res, speechId, db);
      });
    } else {
      // Get all sittings for this date (sittings use activity_date)
      db.all('SELECT content FROM sittings WHERE (date = ? OR activity_date = ?) AND content != "" AND LENGTH(content) > 100', [date, date], (err, rows) => {
        if (err) {
          console.error('[SPEECH] DB error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        if (rows && rows.length > 0) {
          const combinedContent = rows.map(row => row.content).join('\n\n---\n\n');
          console.log(`[SPEECH] Found ${rows.length} speeches in database for date ${date} (${combinedContent.length} chars total)`);
          return res.json({ content: combinedContent });
        }
        
        // Fallback to HTML if no database content
        console.log(`[SPEECH] No database content for date ${date}, falling back to HTML`);
        speechesFetch.fetchFromHTML(date, res, null, db);
      });
    }
  } catch (err) {
    console.error('[SPEECH] Error:', err);
    res.status(500).json({ error: 'Failed to fetch content' });
  }
});

app.get('/api/sittings', (req, res) => {
  db.get('SELECT data, last_updated FROM sittings_cache ORDER BY id DESC LIMIT 1', (err, row) => {
    if (err || !row) {
      return res.status(404).json({ error: 'No cached sittings found.' });
    }
    res.json({ data: JSON.parse(row.data), last_updated: row.last_updated });
  });
});


// GET /api/cache-status: get current cache status
app.get('/api/job-last-log', (req, res) => {
  res.json({ line: lastJobLogLine || '' });
});

// GET /api/localrun: whether data menu/floater is allowed (LOCALRUN env set)
app.get('/api/localrun', (req, res) => {
  res.json({ localrun: LOCALRUN });
});

app.get('/api/cache-status', (req, res) => {
  console.log('[CACHE] Fetching cache status...');
  db.get('SELECT * FROM cache_status WHERE id = 1', (err, row) => {
    if (err) {
      console.error('[CACHE] DB error getting cache status:', err);
      return res.status(500).json({ error: err.toString() });
    }
    
    if (!row) {
      console.log('[CACHE] No cache status found, returning defaults');
      return res.json({
        meps_last_updated: 0,
        speeches_last_updated: 0,
        total_speeches: 0
      });
    }
    
    console.log(`[CACHE] Cache status - MEPs: ${row.meps_last_updated ? new Date(row.meps_last_updated).toLocaleString() : 'Never'}, Speeches: ${row.total_speeches} (${row.speeches_last_updated ? new Date(row.speeches_last_updated).toLocaleString() : 'Never'})`);
    
    res.json({
      meps_last_updated: row.meps_last_updated,
      speeches_last_updated: row.speeches_last_updated,
      total_speeches: row.total_speeches
    });
  });
});

// POST /api/refresh-all: refresh all cached data (incremental for speeches)
app.post('/api/refresh-all', async (req, res) => {
  try {
    console.log('[REFRESH] Starting data refresh...');
    
    // Refresh MEPs (full refresh)
    console.log('[REFRESH] Refreshing MEPs...');
    const meps = await fetchAllMeps();
    console.log(`[REFRESH] Fetched ${meps.length} MEPs from API`);
    
    db.run('DELETE FROM meps');
    console.log('[REFRESH] Cleared existing MEPs from database');
    
    const mepStmt = db.prepare(`INSERT OR REPLACE INTO meps 
      (id, label, givenName, familyName, sortLabel, country, politicalGroup, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const m of meps) {
      const pid = parseInt(m.identifier, 10);
      mepStmt.run(pid, m.label, m.givenName, m.familyName, m.sortLabel,
        m['api:country-of-representation'], m['api:political-group'], Date.now());
    }
    mepStmt.finalize();
    console.log(`[REFRESH] Cached ${meps.length} MEPs to database`);
    
    // Refresh speeches (incremental - only new ones)
    console.log('[REFRESH] Checking for new speeches...');
    const newSpeechCount = await speechesFetch.fetchNewSpeechesIncremental(db);
    
    // Fetch content for any new speeches that don't have content yet
    console.log('[REFRESH] Fetching content for new speeches...');
    const contentCount = await speechesFetch.addContentToExistingSpeeches(db);
    
    // Detect language for any speeches that don't have it set yet
    console.log('[REFRESH] Detecting language for speeches without language set...');
    const langResult = await runDetectionOnDb(db, { onlyNull: true, log: console.log });
    
    // Update cache status
    const now = Date.now();
    db.get('SELECT total_speeches FROM cache_status WHERE id = 1', (err, row) => {
      const totalSpeeches = row ? row.total_speeches : 0;
      
      db.run(`INSERT OR REPLACE INTO cache_status 
        (id, meps_last_updated, speeches_last_updated, total_speeches) 
        VALUES (1, ?, ?, ?)`, [now, now, totalSpeeches]);
      
      console.log(`[REFRESH] Updated cache status - MEPs: ${new Date(now).toLocaleString()}, Speeches: ${totalSpeeches} (${newSpeechCount} new, ${contentCount} content fetched, ${langResult.updated} languages detected)`);
      
      console.log('[REFRESH] Data refresh completed successfully');
      res.json({ 
        success: true, 
        meps_count: meps.length,
        speeches_count: totalSpeeches,
        new_speeches_count: newSpeechCount,
        content_fetched_count: contentCount,
        language_detection_updated: langResult.updated,
        message: `Data refreshed successfully. Added ${newSpeechCount} new speeches, fetched content for ${contentCount} speeches, detected language for ${langResult.updated} speeches.`
      });
    });
  } catch (err) {
    console.error('[REFRESH] Error refreshing data:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// POST /api/refresh-speeches: refresh only speeches (incremental)
app.post('/api/refresh-speeches', async (req, res) => {
  try {
    console.log('[REFRESH] Starting perfect incremental refresh...');
    
    // Step 1: Check current database state
    const currentStats = await new Promise((resolve) => {
      db.get(`
        SELECT 
          COUNT(*) as total_sittings,
          COUNT(CASE WHEN LENGTH(content) > 100 THEN 1 END) as sittings_with_content,
          MAX(activity_date) as latest_date
        FROM sittings
      `, (err, row) => {
        resolve(row || { total_sittings: 0, sittings_with_content: 0, latest_date: null });
      });
    });
    
    console.log(`[REFRESH] Current state: ${currentStats.sittings_with_content} sittings with content, latest: ${currentStats.latest_date}`);
    
    // Get all existing sitting IDs to avoid duplicates
    const existingIds = await new Promise((resolve) => {
      db.all('SELECT id FROM sittings', (err, rows) => {
        resolve(rows ? rows.map(r => r.id) : []);
      });
    });
    console.log(`[REFRESH] Existing sitting IDs: ${existingIds.length}`);

    // Step 2: Fetch ALL speeches from API with pagination
    let allSpeeches = [];
    let offset = 0;
    const limit = 1000;
    let hasMore = true;
    let batchCount = 0;
    
    console.log('📡 [REFRESH] Starting API fetch with pagination...');
    
    while (hasMore) {
      batchCount++;
      console.log(`📡 [REFRESH] Fetching batch ${batchCount}: offset=${offset}, limit=${limit}`);
      
      try {
        const response = await axios.get('https://data.europarl.europa.eu/api/v2/speeches', {
          params: {
            format: 'application/ld+json',
            limit: limit,
            offset: offset,
            'search-language': 'EN'
          },
          headers: { 
            Accept: 'application/ld+json',
            'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)' 
          },
          timeout: 60000
        });
        
        const batchSpeeches = response.data.data || [];
        allSpeeches = allSpeeches.concat(batchSpeeches);
        
        console.log(`   [REFRESH] Batch ${batchCount}: ${batchSpeeches.length} speeches (total: ${allSpeeches.length})`);
        
        // Check if we got fewer speeches than requested (end of data)
        if (batchSpeeches.length < limit) {
          hasMore = false;
          console.log('   [REFRESH] Reached end of API data');
        } else {
          offset += limit;
          // Small delay to be respectful to the API
          await new Promise(resolve => setTimeout(resolve, 200));
        }
      } catch (error) {
        console.error(`   [REFRESH] Error in batch ${batchCount}:`, error.message);
        hasMore = false;
      }
    }
    
    console.log(`[REFRESH] API fetch completed: ${allSpeeches.length} total speeches`);

    // Step 3: Group speeches by date and filter for new ones
    const dateMap = new Map();
    allSpeeches.forEach(speech => {
      const date = speech.activity_date || speech.activity_start_date;
      if (!date) return;
      
      if (!dateMap.has(date)) {
        dateMap.set(date, []);
      }
      dateMap.get(date).push(speech);
    });
    
    console.log(`📅 [REFRESH] Found ${dateMap.size} unique dates in API data`);
    
    // Filter for dates we don't have yet
    const newDates = [];
    for (const [date, speeches] of dateMap) {
      // Check if we have any sitting for this date with content
      const hasContent = await new Promise((resolve) => {
        db.get(`
          SELECT COUNT(*) as count 
          FROM sittings 
          WHERE activity_date = ? AND LENGTH(content) > 100
        `, [date], (err, row) => {
          resolve(row ? row.count > 0 : false);
        });
      });
      
      if (!hasContent) {
        newDates.push({ date, speeches });
      }
    }
    
    console.log(`🆕 [REFRESH] Found ${newDates.length} dates with new content to fetch`);
    
    let fetchedCount = 0;
    let failedCount = 0;
    let parsedCount = 0;
    let totalSpeeches = 0;

    if (newDates.length > 0) {
      // Step 4: Fetch content for new dates
      console.log('📥 [REFRESH] Fetching content for new sittings...');
      
      for (const { date, speeches } of newDates) {
        console.log(`[REFRESH] Fetching content for ${date}...`);
        
        try {
          // Use the first speech ID as the sitting ID
          const sittingId = speeches[0].id;
          
          // Fetch content using the existing function
          const content = await fetchSpeechContentFromHTML(date, sittingId);
          
          if (content && content.length > 100) {
            // Store the sitting with proper API data structure
            await new Promise((resolve, reject) => {
              const stmt = db.prepare(`
                INSERT OR IGNORE INTO sittings
                (id, type, label, personId, activity_date, content, docIdentifier, notationId, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
              `);
              stmt.run(
                sittingId,
                speeches[0].type || 'def/ep-activities/PLENARY_DEBATE_SPEECH', // Use real API type
                speeches[0].label || `Parliamentary Sitting - ${date}`, // Use real API label
                speeches[0].personId || null,
                date,
                content,
                speeches[0].docIdentifier || '',
                speeches[0].notationId || '',
                Date.now()
              );
              stmt.finalize((err) => {
                if (err) reject(err);
                else resolve();
              });
            });
            
            fetchedCount++;
            console.log(`   [REFRESH] ${date}: ${content.length} chars stored`);
          } else {
            failedCount++;
            console.log(`   [REFRESH] ${date}: No content or too short`);
          }
        } catch (error) {
          failedCount++;
          console.log(`   [REFRESH] ${date}: ${error.message}`);
        }
        
        // Small delay between requests
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      // Step 5: Parse new sittings into individual speeches
      console.log('[REFRESH] Parsing new sittings into individual speeches...');
      
      // Get all sittings that need parsing (have content but no individual speeches)
      const sittingsToParse = await new Promise((resolve) => {
        db.all(`
          SELECT s.id, s.content, s.activity_date
          FROM sittings s
          LEFT JOIN individual_speeches i ON s.id = i.sitting_id
          WHERE LENGTH(s.content) > 100
          AND i.sitting_id IS NULL
          GROUP BY s.id
        `, (err, rows) => {
          resolve(rows || []);
        });
      });
      
      console.log(`[REFRESH] Found ${sittingsToParse.length} sittings to parse`);
      
      for (const sitting of sittingsToParse) {
        console.log(`[REFRESH] Parsing sitting ${sitting.id} (${sitting.activity_date})...`);
        
        try {
          const individualSpeeches = parseIndividualSpeeches(sitting.content, sitting.id);
          
          if (individualSpeeches.length > 0) {
            await storeIndividualSpeeches(db, individualSpeeches);
            parsedCount++;
            totalSpeeches += individualSpeeches.length;
            console.log(`   [REFRESH] Parsed ${individualSpeeches.length} individual speeches`);
          } else {
            console.log(`   [REFRESH] No individual speeches found`);
          }
        } catch (error) {
          console.log(`   [REFRESH] Error parsing: ${error.message}`);
        }
      }

      // Step 6: Link speeches to MEPs
      console.log('🔗 [REFRESH] Linking speeches to MEPs...');
      const linkedCount = await historicMeps.linkSpeechesToMeps(db);
      console.log(`[REFRESH] Linked ${linkedCount} speeches to MEPs`);
    }

    // Step 7: Check for and remove duplicates (FINAL STEP)
    console.log('[REFRESH] Final step: Checking for and removing duplicates...');
    const duplicateResult = await historicMeps.checkAndRemoveDuplicates(db);
    console.log(`[REFRESH] Duplicate cleanup completed - Removed ${duplicateResult.totalRemoved} duplicates`);

    // Step 8: Detect language for speeches that don't have it yet
    console.log('[REFRESH] Detecting language for speeches without language set...');
    const langResult = await runDetectionOnDb(db, { onlyNull: true, log: console.log });

    // Get final count after cleanup
    const finalCount = await new Promise((resolve) => {
      db.get('SELECT COUNT(*) as count FROM sittings WHERE LENGTH(content) > 100', (err, row) => {
        resolve(row ? row.count : 0);
      });
    });

    console.log(`[REFRESH] Perfect refresh completed - New dates: ${newDates.length}, Fetched: ${fetchedCount}, Parsed: ${parsedCount}, Total speeches: ${totalSpeeches}, Duplicates removed: ${duplicateResult.totalRemoved}, Languages updated: ${langResult.updated}, Final total: ${finalCount}`);
    res.json({
      success: true,
      sittings_count: finalCount,
      new_dates_count: newDates.length,
      content_fetched_count: fetchedCount,
      content_failed_count: failedCount,
      sittings_parsed_count: parsedCount,
      individual_speeches_count: totalSpeeches,
      duplicates_removed: duplicateResult.totalRemoved,
      language_detection_updated: langResult.updated,
      message: `Perfect refresh completed successfully. Added ${newDates.length} new dates, fetched content for ${fetchedCount} sittings, parsed ${parsedCount} sittings into ${totalSpeeches} individual speeches, removed ${duplicateResult.totalRemoved} duplicates, detected language for ${langResult.updated} speeches.`
    });
  } catch (err) {
    console.error('[REFRESH] Error in perfect refresh:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// POST /api/refresh-meps: refresh only MEPs
app.post('/api/refresh-meps', async (req, res) => {
  try {
    console.log('Starting MEP refresh...');
    const meps = await fetchAllMeps();
    
    db.run('DELETE FROM meps');
    const stmt = db.prepare(`INSERT OR REPLACE INTO meps 
      (id, label, givenName, familyName, sortLabel, country, politicalGroup, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const m of meps) {
      const pid = parseInt(m.identifier, 10);
      stmt.run(pid, m.label, m.givenName, m.familyName, m.sortLabel,
        m['api:country-of-representation'], m['api:political-group'], Date.now());
    }
    stmt.finalize();
    
    // Update cache status
    db.run(`INSERT OR REPLACE INTO cache_status 
      (id, meps_last_updated) 
      VALUES (1, ?)`, [Date.now()]);
    
    console.log('MEP refresh completed');
    res.json({ 
      success: true, 
      meps_count: meps.length,
      message: 'MEPs refreshed successfully'
    });
  } catch (err) {
    console.error('Error refreshing MEPs:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// POST /api/refresh-perfect: run the perfect fetch & parse script end-to-end
app.post('/api/refresh-perfect', async (req, res) => {
  try {
    const { execFile } = require('child_process');
    const path = require('path');

    const scriptPath = path.resolve(__dirname, 'perfect-fetch-parse.js');    const startDate = req.body.startDate || '2023-01-01';
    
    console.log(`[REFRESH PERFECT] Executing ${scriptPath} with start date: ${startDate}...`);

    const child = execFile('node', [scriptPath, startDate], { cwd: __dirname, timeout: 0 });
    
    // Stream stdout in real-time
    child.stdout.on('data', (data) => {
      console.log(data.toString());
    });
    
    // Stream stderr in real-time
    child.stderr.on('data', (data) => {
      console.error(data.toString());
    });
    
    child.on('close', (code) => {
      if (code === 0) {
        console.log('[REFRESH PERFECT] Completed successfully');
        res.json({ success: true, message: 'Perfect fetch & parse completed' });
      } else {
        console.error(`[REFRESH PERFECT] Script exited with code ${code}`);
        res.status(500).json({ success: false, error: `Script exited with code ${code}` });
      }
    });
    
    child.on('error', (error) => {
      console.error('[REFRESH PERFECT] Error:', error);
      res.status(500).json({ success: false, error: error.toString() });
    });
  } catch (err) {
    console.error('[REFRESH PERFECT] Unexpected error:', err);
    res.status(500).json({ success: false, error: err.toString() });
  }
});

// POST /api/refresh-speeches-full: force full refresh of speeches (clears and rebuilds)
app.post('/api/refresh-speeches-full', async (req, res) => {
  try {
    console.log('[REFRESH] Starting full speech refresh (clearing existing data)...');
    const speechCount = await speechesFetch.cacheAllSpeeches(db);
    
    console.log(`[REFRESH] Full speech refresh completed - Total: ${speechCount}`);
    res.json({ 
      success: true, 
      speeches_count: speechCount,
      message: `Full speech refresh completed. Total speeches: ${speechCount}`
    });
  } catch (err) {
    console.error('[REFRESH] Error in full speech refresh:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// Legacy endpoint for backward compatibility
app.post('/api/refresh-sittings', async (req, res) => {
  try {
    const all = await fetchAllSittingsFromRemote();
    db.run('INSERT INTO sittings_cache (data, last_updated) VALUES (?, ?)', JSON.stringify(all), Date.now());
    res.json({ success: true, count: all.length });
  } catch (err) {
    res.status(500).json({ error: err.toString() });
  }
});

// POST /api/refresh-mep-dataset: Full MEP dataset build (API upsert + link + historic one-per-person + group normalizer)
app.post('/api/refresh-mep-dataset', requireLocalRun, async (req, res) => {
  res.setTimeout(0);
  lastJobLogLine = 'Starting MEP dataset refresh...';
  try {
    console.log('[API] Starting full MEP dataset refresh...');
    const projectRoot = path.join(__dirname);
    const jobLog = (msg) => {
      lastJobLogLine = msg.replace(/\s+/g, ' ').trim();
      console.log(msg);
    };
    const results = await runRefreshMepDataset(db, { log: jobLog, projectRoot });
    db.run(`INSERT OR REPLACE INTO cache_status (id, meps_last_updated) VALUES (1, ?)`, [Date.now()], (err) => {
      if (err) console.error('[API] Error updating cache_status:', err);
    });
    console.log('[API] MEP dataset refresh completed:', results);
    res.json({
      success: true,
      ...results,
      message: `MEP dataset built: ${results.apiMeps} API MEPs, ${results.createdHistoric} historic created, ${results.linkedSpeeches} speeches linked. Group normalizer applied.`
    });
  } catch (err) {
    console.error('[API] Error in refresh-mep-dataset:', err);
    res.status(500).json({ success: false, error: err.message || err.toString() });
  }
});

// POST /api/link-historic-meps: Legacy — create historic MEPs and link (use refresh-mep-dataset for full build)
app.post('/api/link-historic-meps', async (req, res) => {
  try {
    console.log('🔗 [API] Starting historic MEP creation and speech linking...');
    const results = await historicMeps.createHistoricMepsAndLinkSpeeches(db);
    res.json({ 
      success: true, 
      ...results,
      message: `Created ${results.createdHistoricMeps} historic MEPs and linked ${results.linkedSpeeches} speeches`
    });
  } catch (err) {
    console.error('[API] Error in historic MEP linking:', err);
    res.status(500).json({ error: err.toString() });
  }
});

// POST /api/rebuild-database: Clear sittings/speeches and run full bulk pipeline (1999-07-20 → today)
app.post('/api/rebuild-database', requireLocalRun, async (req, res) => {
  res.setTimeout(0); // no timeout — rebuild can take hours
  lastJobLogLine = 'Rebuilding database (sittings + speeches from 1999)...';
  try {
    await new Promise((resolve, reject) => {
      db.run('DELETE FROM individual_speeches', (err) => (err ? reject(err) : resolve()));
    });
    await new Promise((resolve, reject) => {
      db.run('DELETE FROM sittings', (err) => (err ? reject(err) : resolve()));
    });
    const { runBulk } = require('./src/pipeline');
    const start = '1999-07-20';
    const end = new Date().toISOString().slice(0, 10);
    console.log('[REBUILD] Running bulk pipeline (single DB connection to avoid locks)...');
    const result = await runBulk({ startDate: start, endDate: end, skipExisting: false, log: console.log, db });
    console.log('[REBUILD] Running language detection on all speeches...');
    const langResult = await runDetectionOnDb(db, { onlyNull: false, log: console.log });
    res.json({
      success: true,
      processed: result.processed,
      failed: result.failed,
      language_detection: { updated: langResult.updated, total: langResult.total }
    });
  } catch (err) {
    console.error('[API] Rebuild database error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/refresh-languages: Re-run language detection on all speeches (overwrite existing)
app.post('/api/refresh-languages', requireLocalRun, async (req, res) => {
  res.setTimeout(0); // can take a while on large DBs
  lastJobLogLine = 'Refreshing languages for all speeches...';
  try {
    console.log('[REFRESH-LANGUAGES] Rebuilding language detection for all speeches...');
    const jobLog = (msg) => {
      lastJobLogLine = String(msg).replace(/\s+/g, ' ').trim();
      console.log(msg);
    };
    const langResult = await runDetectionOnDb(db, { onlyNull: false, log: jobLog });
    console.log(`[REFRESH-LANGUAGES] Done — updated ${langResult.updated} of ${langResult.total} speeches.`);
    res.json({
      success: true,
      updated: langResult.updated,
      total: langResult.total,
      byLang: langResult.byLang || {},
      message: `Language detection complete: ${langResult.updated} speeches updated (${langResult.total} total).`
    });
  } catch (err) {
    console.error('[API] Refresh languages error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/normalize-parties: Run political group normalizer on individual_speeches (fill political_group_std)
app.post('/api/normalize-parties', requireLocalRun, async (req, res) => {
  res.setTimeout(0);
  lastJobLogLine = 'Normalizing parties (political groups)...';
  const log = (msg) => {
    lastJobLogLine = String(msg).replace(/\s+/g, ' ').trim();
    console.log(msg);
  };
  try {
    const projectRoot = path.join(__dirname);
    await runGroupNormalizer(projectRoot, log);
    log('[NORMALIZE-PARTIES] Syncing MEP table from speeches...');
    const syncResult = await syncMepAffiliationsFromSpeeches(db, { log });
    log(`[NORMALIZE-PARTIES] Updated meps.politicalGroup for ${syncResult.updated} MEPs.`);
    log('[NORMALIZE-PARTIES] Done.');
    res.json({
      success: true,
      message: 'Political groups normalized and MEP table synced. Speeches and meps.politicalGroup now reflect the same organisations.',
      syncedMeps: syncResult.updated
    });
  } catch (err) {
    console.error('[API] Normalize parties error:', err);
    res.status(500).json({ success: false, error: err.message || err.toString() });
  }
});

// POST /api/normalize-macro-topics: AI suggests unification rules, then apply them to the DB
app.post('/api/normalize-macro-topics', requireLocalRun, async (req, res) => {
  res.setTimeout(0);
  lastJobLogLine = 'Normalizing macro topics...';
  const log = (msg) => {
    lastJobLogLine = String(msg).replace(/\s+/g, ' ').trim();
    console.log(msg);
  };
  try {
    log('[NORMALIZE] Fetching distinct macro topics...');
    const topicsWithCounts = await getDistinctTopics(db);
    if (topicsWithCounts.length === 0) {
      return res.json({ success: true, rules: 0, updated: 0, message: 'No macro topics in database.' });
    }
    const rules = await suggestRules(topicsWithCounts, log);
    if (rules.length === 0) {
      return res.json({ success: true, rules: 0, updated: 0, message: 'No normalization rules produced.' });
    }
    saveRules(rules);
    log('[NORMALIZE] Rules saved; applying to database...');
    const { updated, byRule } = await applyRules(db, rules, log);
    analyticsCache.data = null;
    analyticsCache.lastUpdated = null;
    log(`[NORMALIZE] Done — ${updated} speeches updated.`);
    res.json({
      success: true,
      rules: rules.length,
      updated,
      byRule,
      message: `Normalized macro topics: ${rules.length} rule(s) applied, ${updated} speeches updated.`
    });
  } catch (err) {
    console.error('[API] Normalize macro topics error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/test-pipeline: Refresh — run bulk from last fully processed sitting onward
// POST /api/generate-analytics: Generate pre-computed analytics database
app.post('/api/generate-analytics', requireLocalRun, async (req, res) => {
  res.setTimeout(0); // no timeout — generation can take 1-5 minutes
  lastJobLogLine = 'Generating analytics database...';
  try {
    console.log('[ANALYTICS] Starting analytics database generation...');
    const startTime = Date.now();
    
    // Generate analytics database
    await generateAnalyticsDatabase(db, console.log);
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[ANALYTICS] Analytics database generated successfully in ${duration} seconds`);
    
    // Clear the in-memory cache so it reloads from the new database
    analyticsCache.data = null;
    analyticsCache.lastUpdated = null;
    
    res.json({
      success: true,
      duration: `${duration}s`,
      message: 'Analytics database generated successfully'
    });
  } catch (err) {
    console.error('[ANALYTICS] Error generating analytics database:', err);
    res.status(500).json({
      success: false,
      error: err.message || 'Failed to generate analytics database'
    });
  }
});

app.post('/api/test-pipeline', requireLocalRun, async (req, res) => {
  res.setTimeout(0); // no timeout — refresh can run for a while
  lastJobLogLine = 'Checking new sittings...';
  const logs = [];
  const log = (msg) => {
    logs.push(msg);
    lastJobLogLine = String(msg).replace(/\s+/g, ' ').trim();
    console.log(msg);
  };
  try {
    log('[REFRESH] Refresh data: fetch new sittings, store them, then detect language for any speech missing it. A speech is complete when it has content + detected language (not default English).');
    const { runRefresh } = require('./src/pipeline');
    const result = await runRefresh({ log, db });
    log(`[REFRESH] Pipeline finished — sittings stored: ${result.processed}, failed: ${result.failed}, fetch skipped: ${result.fetchSkipped}, AI failed: ${result.aiFailed}.`);
    if (result.processed > 0 || result.failed > 0) {
      log('[REFRESH] Running language detection on speeches without language set...');
      const langResult = await runDetectionOnDb(db, { onlyNull: true, log });
      log(`[REFRESH] Language detection done — updated ${langResult.updated} speeches.`);
      res.json({
        success: result.processed >= 0,
        processed: result.processed,
        failed: result.failed,
        fetchSkipped: result.fetchSkipped,
        aiFailed: result.aiFailed,
        language_detection_updated: langResult.updated,
        logs,
        message: result.failed > 0
          ? `Stored ${result.processed} sittings; ${result.failed} store(s) failed (check logs). Language detection: ${langResult.updated} speeches updated.`
          : `Stored ${result.processed} new sittings. Language detection: ${langResult.updated} speeches updated.`
      });
    } else {
      log('[REFRESH] No new sittings to store. Running language detection on any speeches without language set...');
      const langResult = await runDetectionOnDb(db, { onlyNull: true, log });
      log(`[REFRESH] Language detection done — updated ${langResult.updated} speeches.`);
      res.json({
        success: true,
        processed: 0,
        failed: 0,
        fetchSkipped: result.fetchSkipped,
        aiFailed: result.aiFailed,
        language_detection_updated: langResult.updated,
        logs,
        message: result.fetchSkipped > 0
          ? `No new sittings stored (${result.fetchSkipped} dates skipped or already done). Language detection: ${langResult.updated} speeches updated.`
          : `No new sittings to process. Language detection: ${langResult.updated} speeches updated.`
      });
    }
  } catch (err) {
    log(`[REFRESH] Error: ${err.message}`);
    console.error('[API] Test pipeline error:', err);
    res.status(500).json({ success: false, error: err.message, logs });
  }
});