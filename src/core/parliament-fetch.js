/**
 * EU Parliament sitting HTML fetch & discovery
 * HTML scraping only — no EU Parliament API calls
 */

const axios = require('axios');

const USER_AGENT = 'Mozilla/5.0 (compatible; EUROWATCH/1.0)';

/**
 * Get session number for URL based on date (EP terms 1–10)
 */
function getSessionNumber(date) {
  if (!date) return 10;
  if (date >= '2024-07-16') return 10; // 10th term: 2024-07-16 → present
  if (date >= '2019-07-02') return 9;  // 9th term: 2019-07-02 → 2024-07-15
  if (date >= '2014-07-01') return 8;  // 8th term: 2014-07-01 → 2019-07-01
  if (date >= '2009-07-14') return 7;  // 7th term: 2009-07-14 → 2014-06-30
  if (date >= '2004-07-20') return 6;  // 6th term: 2004-07-20 → 2009-07-13
  if (date >= '1999-07-20') return 5;  // 5th term: 1999-07-20 → 2004-07-19
  if (date >= '1994-07-19') return 4;  // 4th term: 1994-07-19 → 1999-07-19
  if (date >= '1989-07-25') return 3;  // 3rd term: 1989-07-25 → 1994-07-18
  if (date >= '1984-07-24') return 2;  // 2nd term: 1984-07-24 → 1989-07-24
  if (date >= '1979-07-17') return 1;  // 1st term: 1979-07-17 → 1984-07-23
  return 1;
}

/**
 * Format date as YYYY-MM-DD
 */
function formatDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/**
 * Discover the most recent sitting date not in DB by iterating backwards from today.
 * Uses HTML scraping only — no EU API. Returns date string or null.
 */
async function discoverNextSittingDate(db, log = () => {}, maxDaysBack = 365) {
  const today = formatDate(new Date());
  let d = new Date();
  for (let i = 0; i < maxDaysBack; i++) {
    const dateStr = formatDate(d);
    const inDb = await new Promise((resolve, reject) => {
      db.get('SELECT 1 FROM sittings WHERE activity_date = ? AND LENGTH(content) > 100 LIMIT 1', [dateStr], (err, row) => resolve(!!row));
    });
    if (!inDb) {
      try {
        const html = await fetchSittingHTML(dateStr, 1);
        if (html && html.length > 500 && /<html|arrow_title_doc\.gif|<table|<td/i.test(html)) {
          log(`  Found sitting at ${dateStr} (HTML ${html.length} chars)`);
          return dateStr;
        }
      } catch (_) { /* no sitting this day */ }
    }
    d.setDate(d.getDate() - 1);
  }
  return null;
}

/**
 * Find the most recent sitting that is fully processed (has speeches, all with macro_topic).
 * Returns activity_date string or null.
 */
async function findMostRecentFullyProcessedSitting(db) {
  return new Promise((resolve, reject) => {
    db.get(
      `SELECT s.activity_date FROM sittings s
       WHERE EXISTS (SELECT 1 FROM individual_speeches i WHERE i.sitting_id = s.id)
       AND NOT EXISTS (SELECT 1 FROM individual_speeches i WHERE i.sitting_id = s.id AND (i.macro_topic IS NULL OR i.macro_topic = ''))
       ORDER BY s.activity_date DESC LIMIT 1`,
      (err, row) => (err ? reject(err) : resolve(row ? row.activity_date : null))
    );
  });
}

/**
 * Filter to dates that need processing: not in DB OR in DB but unclassified.
 */
async function filterDatesNeedingProcessing(dates, db) {
  if (!dates.length) return [];
  const placeholders = dates.map(() => '?').join(',');
  const [inDb, unclassified] = await Promise.all([
    new Promise((resolve, reject) => {
      db.all(
        `SELECT activity_date FROM sittings WHERE activity_date IN (${placeholders}) AND LENGTH(content) > 100`,
        dates,
        (err, rows) => (err ? reject(err) : resolve((rows || []).map(r => r.activity_date)))
      );
    }),
    new Promise((resolve, reject) => {
      db.all(
        `SELECT s.activity_date FROM sittings s
         WHERE s.activity_date IN (${placeholders}) AND s.content IS NOT NULL AND LENGTH(s.content) > 100
         AND (NOT EXISTS (SELECT 1 FROM individual_speeches i WHERE i.sitting_id = s.id)
              OR EXISTS (SELECT 1 FROM individual_speeches i WHERE i.sitting_id = s.id AND (i.macro_topic IS NULL OR i.macro_topic = '')))`,
        dates,
        (err, rows) => (err ? reject(err) : resolve((rows || []).map(r => r.activity_date)))
      );
    })
  ]);
  const inDbSet = new Set(inDb);
  const unclassifiedSet = new Set(unclassified);
  const notInDb = dates.filter(d => !inDbSet.has(d));
  const needsWork = [...new Set([...notInDb, ...unclassifiedSet])].sort();
  return needsWork;
}

/**
 * Get HTML for a date from DB if sitting exists, else null.
 */
async function getSittingHtmlFromDb(date, db) {
  return new Promise((resolve, reject) => {
    db.get(
      'SELECT content FROM sittings WHERE activity_date = ? AND LENGTH(content) > 100',
      [date],
      (err, row) => (err ? reject(err) : resolve(row ? row.content : null))
    );
  });
}

/**
 * Find the most recent sitting that exists in DB but has unclassified speeches
 * (macro_topic IS NULL). Returns { id, activity_date, content } or null.
 */
async function findMostRecentUnclassifiedSitting(db) {
  return new Promise((resolve, reject) => {
    db.get(
      `SELECT s.id, s.activity_date, s.content FROM sittings s
       WHERE s.content IS NOT NULL AND LENGTH(s.content) > 100
       AND (NOT EXISTS (SELECT 1 FROM individual_speeches i WHERE i.sitting_id = s.id)
            OR EXISTS (SELECT 1 FROM individual_speeches i WHERE i.sitting_id = s.id AND (i.macro_topic IS NULL OR i.macro_topic = '')))
       ORDER BY s.activity_date DESC LIMIT 1`,
      (err, row) => (err ? reject(err) : resolve(row || null))
    );
  });
}

/**
 * Fetch HTML content for a sitting date
 */
async function fetchSittingHTML(date, maxRetries = 3) {
  const session = getSessionNumber(date);
  const url = `https://www.europarl.europa.eu/doceo/document/CRE-${session}-${date}_EN.html`;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await axios.get(url, {
        headers: { 'User-Agent': USER_AGENT },
        timeout: 25000
      });
      return res.data;
    } catch (err) {
      if (attempt === maxRetries) throw err;
      await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 1000));
    }
  }
}

/**
 * Generate all dates in range [startDate, endDate] inclusive.
 * @param {string} startDate - YYYY-MM-DD
 * @param {string} endDate - YYYY-MM-DD
 * @returns {string[]}
 */
function listDatesInRange(startDate, endDate) {
  const start = new Date(startDate);
  const end = new Date(endDate);
  const dates = [];
  const d = new Date(start);
  while (d <= end) {
    dates.push(formatDate(d));
    d.setDate(d.getDate() + 1);
  }
  return dates;
}

/**
 * Filter to dates not yet in DB (no sitting with content).
 * @param {string[]} dates
 * @param {object} db - sqlite3 Database
 * @returns {Promise<string[]>}
 */
async function filterDatesNotInDb(dates, db) {
  if (!dates.length) return [];
  const placeholders = dates.map(() => '?').join(',');
  return new Promise((resolve, reject) => {
    db.all(
      `SELECT activity_date FROM sittings WHERE activity_date IN (${placeholders}) AND LENGTH(content) > 100`,
      dates,
      (err, rows) => {
        if (err) return reject(err);
        const inDb = new Set((rows || []).map(r => r.activity_date));
        resolve(dates.filter(d => !inDb.has(d)));
      }
    );
  });
}

module.exports = {
  getSessionNumber,
  formatDate,
  discoverNextSittingDate,
  findMostRecentFullyProcessedSitting,
  findMostRecentUnclassifiedSitting,
  filterDatesNeedingProcessing,
  getSittingHtmlFromDb,
  fetchSittingHTML,
  listDatesInRange,
  filterDatesNotInDb
};
