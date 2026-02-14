/**
 * EU Parliament speech language detection (CLD3 + franc + EU heuristics).
 * Used by refresh/rebuild to set `language` on individual_speeches (ISO 639-1, e.g. EN, FR).
 * Does not assume English; detects and stores the detected code or NULL when uncertain.
 */

const { loadModule } = require('cld3-asm');
const franc = require('franc').franc;
const langs = require('langs');

const TABLE = 'individual_speeches';
const ID_COL = 'id';
const TEXT_COL = 'speech_content';
const LANG_COL = 'language';

const CLD3_MIN_PROB = 0.60;
const CHUNK_SIZE = 600;
const MAX_TEXT = 50000;
const DEFAULT_WHEN_UNCERTAIN = null;

const EU_ISO2 = [
  'BG', 'CS', 'DA', 'DE', 'EL', 'EN', 'ES', 'ET', 'FI', 'FR', 'GA', 'HR', 'HU',
  'IT', 'LT', 'LV', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SL', 'SV'
];
const EU_SET = new Set(EU_ISO2);
const ISO2_TO_3 = {
  BG: 'bul', CS: 'ces', DA: 'dan', DE: 'deu', EL: 'ell', EN: 'eng', ES: 'spa',
  ET: 'est', FI: 'fin', FR: 'fra', GA: 'gle', HR: 'hrv', HU: 'hun', IT: 'ita',
  LT: 'lit', LV: 'lav', MT: 'mlt', NL: 'nld', PL: 'pol', PT: 'por', RO: 'ron',
  SK: 'slk', SL: 'slv', SV: 'swe'
};
const EU_3CODES = Object.values(ISO2_TO_3);

function toISO2Upper(s) {
  if (!s) return null;
  s = s.toLowerCase();
  const map = { he: 'iw', yi: 'ji' };
  const maybe = map[s] || s;
  const code = maybe.length === 2 ? maybe : (langs.where('3', maybe) || {})['1'];
  if (!code) return null;
  const iso2 = code.toUpperCase();
  return EU_SET.has(iso2) ? iso2 : null;
}

function iso3ToIso2Upper(iso3) {
  const hit = Object.entries(ISO2_TO_3).find(([, v]) => v === iso3);
  return hit ? hit[0] : null;
}

function scriptHeuristic(text) {
  if (!text) return null;
  const totalChars = text.replace(/\s/g, '').length;
  if (totalChars < 20) return null;
  const greekChars = (text.match(/[\u0370-\u03FF]/g) || []).length;
  if (greekChars > 0 && (greekChars / totalChars) >= 0.30) {
    return { lang: 'EL', conf: 0.999, via: 'script' };
  }
  const cyrillicChars = (text.match(/[\u0400-\u04FF]/g) || []).length;
  if (cyrillicChars > 0 && (cyrillicChars / totalChars) >= 0.30) {
    return { lang: 'BG', conf: 0.995, via: 'script' };
  }
  return null;
}

function chunksOf(text, size = CHUNK_SIZE) {
  const t = text.length > MAX_TEXT ? text.slice(0, MAX_TEXT) : text;
  const parts = [];
  for (let i = 0; i < t.length; i += size) parts.push(t.slice(i, i + size));
  return parts;
}

function clean(text) {
  if (!text) return '';
  return String(text)
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function detectWithCLD3(cld3, text) {
  const res = cld3.findLanguage(text);
  if (!res || !res.language) return null;
  const iso2 = toISO2Upper(res.language);
  if (!iso2) return null;
  if (res.isReliable && res.probability >= CLD3_MIN_PROB) {
    return { lang: iso2, conf: res.probability, via: 'cld3' };
  }
  return { lang: iso2, conf: res.probability || 0, via: 'cld3-weak' };
}

function voteCLD3(cld3, text) {
  const votes = new Map();
  const parts = chunksOf(text);
  for (const part of parts) {
    const r = detectWithCLD3(cld3, part);
    if (!r || !r.lang) continue;
    const prev = votes.get(r.lang) || { score: 0, n: 0 };
    votes.set(r.lang, { score: prev.score + (r.conf || 0.5), n: prev.n + 1 });
  }
  if (!votes.size) return null;
  let best = null; let bestAvg = -1;
  for (const [lang, { score, n }] of votes.entries()) {
    const avg = score / n;
    if (avg > bestAvg) { bestAvg = avg; best = lang; }
  }
  return { lang: best, conf: bestAvg, via: 'cld3-vote' };
}

function detectWithFranc(text) {
  const lang3 = franc(text, { whitelist: EU_3CODES, minLength: 20 });
  if (!lang3 || lang3 === 'und') return null;
  const iso2 = iso3ToIso2Upper(lang3);
  if (!iso2) return null;
  return { lang: iso2, conf: 0.75, via: 'franc' };
}

function decideLanguage(detectors, raw) {
  const text = clean(raw);
  if (!text) return null;

  const script = scriptHeuristic(text);
  if (script) return script;

  const cldOne = detectWithCLD3(detectors.cld3, text);
  if (cldOne && cldOne.conf >= CLD3_MIN_PROB) return cldOne;

  const cldVote = voteCLD3(detectors.cld3, text);
  if (cldVote && cldVote.conf >= 0.72) return cldVote;

  const francRes = detectWithFranc(text);
  const candidates = [cldOne, cldVote, francRes].filter(Boolean);
  if (!candidates.length) return null;

  const byLang = new Map();
  for (const c of candidates) byLang.set(c.lang, (byLang.get(c.lang) || 0) + 1);
  let agreed = null; let maxVotes = 0;
  for (const [lang, n] of byLang.entries()) if (n > maxVotes) { maxVotes = n; agreed = lang; }
  if (maxVotes >= 2) return candidates.find(c => c.lang === agreed);

  const pref = candidates.find(c => c.via.startsWith('cld3'))
    || candidates.find(c => c.via === 'franc');
  if (francRes && (francRes.lang === 'EL' || francRes.lang === 'MT') && (!pref || pref.conf < 0.7)) {
    return francRes;
  }
  return pref || candidates[0];
}

/**
 * Ensure individual_speeches has a language column. Idempotent.
 * @param {object} db - sqlite3 Database instance
 * @returns {Promise<void>}
 */
function ensureLanguageColumn(db) {
  return new Promise((resolve, reject) => {
    db.run(`ALTER TABLE ${TABLE} ADD COLUMN ${LANG_COL} TEXT`, (err) => {
      if (err && !String(err.message).includes('duplicate column name')) return reject(err);
      resolve();
    });
  });
}

/**
 * Create detector (load CLD3). Call once before processing many texts.
 * @returns {Promise<{ cld3: object }>}
 */
async function createDetector() {
  const cldFactory = await loadModule();
  const cld3 = cldFactory.create();
  return { cld3 };
}

/**
 * Detect language for a single text. Use createDetector() once, then call this for each text.
 * @param {object} detectors - from createDetector()
 * @param {string} text - raw speech content
 * @returns {string|null} - ISO 639-1 code (e.g. 'EN', 'FR') or null
 */
function detectLanguage(detectors, text) {
  const decision = decideLanguage(detectors, text);
  return decision ? decision.lang : DEFAULT_WHEN_UNCERTAIN;
}

/**
 * Run language detection on the database and update the language column.
 * @param {object} db - sqlite3 Database instance
 * @param {object} options
 * @param {boolean} [options.onlyNull=true] - if true, only update rows where language IS NULL
 * @param {function} [options.log] - log function (default no-op)
 * @param {number} [options.batchSize=500] - rows per transaction
 * @returns {Promise<{ updated: number, total: number, byLang: object }>}
 */
function runDetectionOnDb(db, options = {}) {
  const log = options.log || (() => {});
  const onlyNull = options.onlyNull !== false;
  const batchSize = options.batchSize || 500;

  return ensureLanguageColumn(db).then(() => new Promise((resolve, reject) => {
    const where = onlyNull ? ` WHERE ${LANG_COL} IS NULL` : '';
    db.all(
      `SELECT ${ID_COL} AS id, ${TEXT_COL} AS text FROM ${TABLE}${where} ORDER BY ${ID_COL}`,
      [],
      async (err, rows) => {
        if (err) return reject(err);
        const total = rows.length;
        if (total === 0) {
          log('[LANG] No speeches to detect (all have language set)');
          return resolve({ updated: 0, total: 0, byLang: {} });
        }

        log(`[LANG] Detecting language for ${total} speeches (CLD3 + franc, EU-constrained)...`);
        let detectors;
        try {
          detectors = await createDetector();
        } catch (e) {
          return reject(e);
        }

        const updateStmt = db.prepare(`UPDATE ${TABLE} SET ${LANG_COL} = ? WHERE ${ID_COL} = ?`);
        const tallies = Object.create(null);
        let processed = 0;

        function runBatch(offset, callback) {
          const batch = rows.slice(offset, offset + batchSize);
          if (batch.length === 0) return callback(null);

          db.run('BEGIN', (beginErr) => {
            if (beginErr) return callback(beginErr);
            let done = 0;
            for (const row of batch) {
              const lang = detectLanguage(detectors, row.text);
              updateStmt.run(lang, row.id, (runErr) => {
                if (!runErr && lang) tallies[lang] = (tallies[lang] || 0) + 1;
                processed++;
                if (processed % 2000 === 0) process.stdout.write(`\r[LANG] ${processed}/${total}...`);
                done++;
                if (done === batch.length) {
                  db.run('COMMIT', (commitErr) => {
                    if (commitErr) return callback(commitErr);
                    if (offset + batch.length < rows.length) {
                      runBatch(offset + batch.length, callback);
                    } else {
                      updateStmt.finalize(() => callback(null));
                    }
                  });
                }
              });
            }
          });
        }

        runBatch(0, (runErr) => {
          if (runErr) return reject(runErr);
          if (processed > 0 && processed % 2000 !== 0) log('');
          log(`[LANG] Done. Updated ${processed} speeches.`);
          const sorted = Object.entries(tallies).sort((a, b) => b[1] - a[1]);
          if (sorted.length > 0) {
            log('[LANG] Counts: ' + sorted.map(([lang, n]) => `${lang}: ${n}`).join(', '));
          }
          resolve({ updated: processed, total, byLang: tallies });
        });
      }
    );
  }));
}

module.exports = {
  ensureLanguageColumn,
  createDetector,
  detectLanguage,
  decideLanguage,
  runDetectionOnDb,
  TABLE,
  LANG_COL,
  TEXT_COL,
  ID_COL
};
