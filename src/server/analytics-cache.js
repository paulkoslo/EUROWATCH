/**
 * In-memory analytics cache and warm function. Used by /api/analytics/* routes.
 */
const { loadAnalyticsFromDatabase } = require('../core/analytics-db');

const analyticsCache = {
  data: null,
  lastUpdated: null,
  isWarming: false,
  progress: { stage: '', percent: 0, message: '' }
};

// Helper function to normalize topic names (remove HTML entities, normalize dashes)
function normalizeTopic(topic) {
  if (!topic) return topic;
  let normalized = topic
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
  
  // Normalize ALL types of dashes, hyphens, and minus signs to regular hyphen-minus
  // U+2010 to U+2015: various dashes
  // U+2011: non-breaking hyphen  
  // U+2013: en-dash
  // U+2014: em-dash (the —)
  // U+2212: minus sign
  normalized = normalized.replace(/[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]/g, '-');
  
  return normalized.trim();
};

async function warmAnalyticsCache(db) {
  if (analyticsCache.isWarming) {
    console.log('Cache warming already in progress');
    return;
  }

  analyticsCache.isWarming = true;
  analyticsCache.progress = { stage: 'Starting', percent: 0, message: 'Loading analytics...' };
  console.log('[CACHE] Starting analytics cache loading...');
  
  try {
    // Try to load from pre-computed analytics database first (FAST!)
    try {
      const cacheData = await loadAnalyticsFromDatabase(console.log);
      analyticsCache.data = cacheData;
      analyticsCache.lastUpdated = new Date().toISOString();
      analyticsCache.progress = { stage: 'complete', percent: 100, message: 'Cache ready!' };
      analyticsCache.isWarming = false;
      console.log('[CACHE] Analytics loaded from pre-computed database (instant!)');
      console.log(`[CACHE] Loaded: ${cacheData.allTopics.length} topics, ${cacheData.timeseries_month.labels.length} periods`);
      return;
    } catch (loadError) {
      // Analytics database doesn't exist yet - fall back to computing
      console.log('[CACHE] Pre-computed analytics database not found, computing from scratch...');
      console.log('💡 [CACHE] Tip: Run "node src/scripts/generate-analytics.js" to pre-compute analytics for instant loading');
    }
    
    // Fallback: Compute analytics on the fly (SLOW but works)
    analyticsCache.progress = { stage: 'Starting', percent: 0, message: 'Computing analytics...' };
    console.log('[CACHE] Computing analytics from main database (this may take a minute)...');
    
    const cacheData = {};
    // Step 1: Get all normalized topics (10%)
    analyticsCache.progress = { stage: 'topics', percent: 10, message: 'Loading topics...' };
    console.log('[CACHE] Step 1/6: Loading topics');
    
    const allTopicsRows = await new Promise((resolve, reject) => {
      // Optimized: Use index on macro_topic first, then filter
      db.all(`
        SELECT DISTINCT i.macro_topic AS topic
        FROM individual_speeches i
        INNER JOIN sittings s ON s.id = i.sitting_id
        WHERE i.macro_topic IS NOT NULL AND TRIM(i.macro_topic)<>''
          AND s.activity_date IS NOT NULL
      `, [], (err, rows) => err ? reject(err) : resolve(rows));
    });
    
    const rawTopics = allTopicsRows.map(r => r.topic);
    const normalizedMap = new Map();
    rawTopics.forEach(topic => {
      const normalized = normalizeTopic(topic);
      if (!normalizedMap.has(normalized)) {
        normalizedMap.set(normalized, [topic]);
      } else {
        normalizedMap.get(normalized).push(topic);
      }
    });
    const allTopics = Array.from(normalizedMap.keys());
    cacheData.allTopics = allTopics;
    cacheData.topicVariants = normalizedMap;
    console.log(`[CACHE] Found ${allTopics.length} unique topics`);
    
    // Step 2: Pre-compute time series for ALL topics (monthly and quarterly) (40%)
    analyticsCache.progress = { stage: 'timeseries', percent: 25, message: 'Computing time series data...' };
    console.log('[CACHE] Step 2/6: Computing time series');
    
    for (const interval of ['month', 'quarter']) {
      const periodExpr = interval === 'month' 
        ? `substr(s.activity_date,1,7)` 
        : `substr(s.activity_date,1,4) || '-Q' || ((cast(substr(s.activity_date,6,2) as integer)+2)/3)`;
      
      // Get all variants for SQL
      const allVariants = Array.from(normalizedMap.values()).flat();
      const placeholders = allVariants.map(() => '?').join(',');
      
      const [dataRows, periodRows] = await Promise.all([
        new Promise((resolve, reject) => {
          // Optimized: Use covering index idx_speeches_sitting_topic for faster JOIN
          // Filter on macro_topic first, then join only needed rows
          db.all(`
            SELECT ${periodExpr} AS period, i.macro_topic AS topic, COUNT(*) AS cnt
            FROM individual_speeches i
            INNER JOIN sittings s ON s.id = i.sitting_id
            WHERE i.macro_topic IN (${placeholders})
              AND s.activity_date IS NOT NULL
              AND TRIM(i.macro_topic) <> ''
            GROUP BY period, i.macro_topic
            ORDER BY period ASC
          `, allVariants, (err, rows) => err ? reject(err) : resolve(rows));
        }),
        new Promise((resolve, reject) => {
          // Optimized: Get distinct periods directly from sittings (faster)
          db.all(`
            SELECT DISTINCT ${periodExpr} AS period
            FROM sittings s
            WHERE s.activity_date IS NOT NULL
              AND EXISTS (SELECT 1 FROM individual_speeches i WHERE i.sitting_id = s.id)
            ORDER BY period ASC
          `, [], (err, rows) => err ? reject(err) : resolve(rows));
        })
      ]);
      
      const labels = periodRows.map(r => r.period);
      
      // OPTIMIZATION: Pre-index data by topic|period for O(1) lookups
      const dataIndex = new Map();
      dataRows.forEach(row => {
        const key = `${row.topic}|${row.period}`;
        dataIndex.set(key, (dataIndex.get(key) || 0) + row.cnt);
      });
      
      // Now this is FAST - O(1) lookups instead of O(n) filters!
      const datasets = allTopics.map(normalizedTopic => {
        const variants = normalizedMap.get(normalizedTopic) || [];
        return {
          label: normalizedTopic,
          data: labels.map(p => {
            // Sum counts for all variants using O(1) Map lookups
            return variants.reduce((sum, variant) => {
              const key = `${variant}|${p}`;
              return sum + (dataIndex.get(key) || 0);
            }, 0);
          })
        };
      });
      
      cacheData[`timeseries_${interval}`] = { labels, datasets, topics: allTopics };
      console.log(`[CACHE] Computed ${interval} time series: ${labels.length} periods, ${allTopics.length} topics`);
    }
    
    analyticsCache.progress = { stage: 'timeseries', percent: 40, message: 'Time series computed' };
    
    // Step 3: Pre-compute by-group data (60%)
    analyticsCache.progress = { stage: 'groups', percent: 50, message: 'Computing political groups data...' };
    console.log('[CACHE] Step 3/6: Computing by-group');
    
    // Use ALL topics (not just top 10) for comprehensive filtering
    const topTopicsForGroups = allTopics;
    
    const [groups, groupRows] = await Promise.all([
      new Promise((resolve, reject) => {
        db.all(`
          SELECT COALESCE(political_group_std, political_group) AS grp, COUNT(*) AS cnt
          FROM individual_speeches
          WHERE COALESCE(political_group_std, political_group) IS NOT NULL 
            AND TRIM(COALESCE(political_group_std, political_group))<>''
          GROUP BY grp
          ORDER BY cnt DESC LIMIT 10
        `, [], (err, rows) => err ? reject(err) : resolve(rows));
      })
    ]);
    
    const groupsList = groups.map(r => r.grp);
    
    const allTopicVariantsForGroups = Array.from(new Set(
      topTopicsForGroups.flatMap(t => normalizedMap.get(t) || [])
    ));
    
    const groupDataRows = await new Promise((resolve, reject) => {
      const pT = allTopicVariantsForGroups.map(() => '?').join(',');
      const pG = groupsList.map(() => '?').join(',');
      db.all(`
        SELECT i.macro_topic AS topic, COALESCE(i.political_group_std, i.political_group) AS grp, COUNT(*) AS cnt
        FROM individual_speeches i
        WHERE i.macro_topic IN (${pT})
          AND COALESCE(i.political_group_std, i.political_group) IN (${pG})
        GROUP BY i.macro_topic, COALESCE(i.political_group_std, i.political_group)
      `, [...allTopicVariantsForGroups, ...groupsList], (err, rows) => err ? reject(err) : resolve(rows));
    });
    
    // Normalize the rows to use normalized topic names for easier filtering
    const normalizedGroupRows = groupDataRows.map(row => ({
      ...row,
      topic: normalizeTopic(row.topic)
    }));
    
    cacheData.byGroup = { topics: topTopicsForGroups, groups: groupsList, rows: normalizedGroupRows, topicVariants: normalizedMap };
    console.log(`[CACHE] Computed by-group: ${topTopicsForGroups.length} topics × ${groupsList.length} groups`);
    analyticsCache.progress = { stage: 'groups', percent: 60, message: 'Political groups computed' };
    
    // Step 4: Pre-compute by-language data (75%)
    analyticsCache.progress = { stage: 'languages', percent: 65, message: 'Computing macro topics × language...' };
    console.log('[CACHE] Step 4/6: Computing by-language');
    
    const topTopicsForLanguages = allTopics;
    
    const [languages] = await Promise.all([
      new Promise((resolve, reject) => {
        db.all(`
          SELECT UPPER(COALESCE(language,'UNK')) AS language, COUNT(*) AS cnt
          FROM individual_speeches
          GROUP BY UPPER(COALESCE(language,'UNK'))
          ORDER BY cnt DESC LIMIT 24
        `, [], (err, rows) => err ? reject(err) : resolve(rows));
      })
    ]);
    
    const languagesList = languages.map(r => r.language).filter(Boolean);
    
    const allTopicVariantsForLanguages = Array.from(new Set(
      topTopicsForLanguages.flatMap(t => normalizedMap.get(t) || [])
    ));
    
    const languageDataRows = await new Promise((resolve, reject) => {
      const pT = allTopicVariantsForLanguages.map(() => '?').join(',');
      const pL = languagesList.map(() => '?').join(',');
      db.all(`
        SELECT i.macro_topic AS topic, UPPER(COALESCE(i.language,'UNK')) AS language, COUNT(*) AS cnt
        FROM individual_speeches i
        WHERE i.macro_topic IN (${pT})
          AND UPPER(COALESCE(i.language,'UNK')) IN (${pL})
        GROUP BY i.macro_topic, UPPER(COALESCE(i.language,'UNK'))
      `, [...allTopicVariantsForLanguages, ...languagesList], (err, rows) => err ? reject(err) : resolve(rows));
    });
    
    const normalizedLanguageRows = languageDataRows.map(row => ({
      ...row,
      topic: normalizeTopic(row.topic)
    }));
    
    cacheData.byLanguage = { topics: topTopicsForLanguages, languages: languagesList, rows: normalizedLanguageRows, topicVariants: normalizedMap };
    console.log(`[CACHE] Computed by-language: ${topTopicsForLanguages.length} topics × ${languagesList.length} languages`);
    analyticsCache.progress = { stage: 'languages', percent: 75, message: 'Macro × language computed' };
    
    // Step 5: Pre-compute languages (85%)
    analyticsCache.progress = { stage: 'languages', percent: 80, message: 'Computing languages...' };
    console.log('[CACHE] Step 5/6: Computing languages');
    
    const languageRows = await new Promise((resolve, reject) => {
      db.all(`
        SELECT UPPER(COALESCE(language,'UNK')) AS language, COUNT(*) AS cnt
        FROM individual_speeches
        GROUP BY UPPER(COALESCE(language,'UNK'))
        ORDER BY cnt DESC
      `, [], (err, rows) => err ? reject(err) : resolve(rows));
    });
    
    cacheData.languages = { rows: languageRows };
    console.log(`[CACHE] Computed languages: ${languageRows.length} languages`);
    analyticsCache.progress = { stage: 'languages', percent: 85, message: 'Languages computed' };
    
    // Step 6: Pre-compute overview data (95%)
    analyticsCache.progress = { stage: 'overview', percent: 90, message: 'Computing overview...' };
    console.log('[CACHE] Step 6/6: Computing overview');
    
    const [coverage, macroTopics, specificFocus] = await Promise.all([
      new Promise((resolve, reject) => {
        db.get(`
          SELECT 
            COUNT(*) AS total,
            SUM(CASE WHEN macro_topic IS NOT NULL AND TRIM(macro_topic) <> '' THEN 1 ELSE 0 END) AS with_macro
          FROM individual_speeches
        `, [], (err, row) => err ? reject(err) : resolve(row));
      }),
      new Promise((resolve, reject) => {
        db.all(`
          SELECT macro_topic AS topic, COUNT(*) AS count
          FROM individual_speeches
          WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
          GROUP BY macro_topic
          ORDER BY count DESC LIMIT 20
        `, [], (err, rows) => err ? reject(err) : resolve(rows));
      }),
      new Promise((resolve, reject) => {
        db.all(`
          SELECT macro_topic AS topic, macro_specific_focus AS focus, COUNT(*) AS count
          FROM individual_speeches
          WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
            AND macro_specific_focus IS NOT NULL AND TRIM(macro_specific_focus) <> ''
          GROUP BY macro_topic, macro_specific_focus
          ORDER BY count DESC LIMIT 20
        `, [], (err, rows) => err ? reject(err) : resolve(rows));
      })
    ]);
    
    const total = coverage?.total || 0;
    const withMacro = coverage?.with_macro || 0;
    const pct = total ? Math.round((withMacro / total) * 1000) / 10 : 0;
    
    cacheData.overview = {
      coverage: { total, with_macro: withMacro, pct_with_macro: pct },
      macroTopicDistribution: macroTopics,
      topSpecificFocus: specificFocus
    };
    
    console.log(`[CACHE] Computed overview`);
    
    // Done!
    analyticsCache.data = cacheData;
    analyticsCache.lastUpdated = new Date().toISOString();
    analyticsCache.progress = { stage: 'complete', percent: 100, message: 'Cache ready!' };
    analyticsCache.isWarming = false;
    
    console.log('[CACHE] Analytics cache warming completed successfully!');
    console.log(`[CACHE] Cached: ${allTopics.length} topics, ${cacheData.timeseries_month.labels.length} periods`);
    
  } catch (error) {
    console.error('[CACHE] Error warming cache:', error);
    analyticsCache.isWarming = false;
    analyticsCache.progress = { stage: 'error', percent: 0, message: 'Cache warming failed: ' + error.message };
  }
}

module.exports = { analyticsCache, warmAnalyticsCache, normalizeTopic };
