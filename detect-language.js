/*
 * EU Parliament Speech Language Detection — Robust v2
 *
 * What this script does (preserving original intent):
 *  - Ensures there is a TEXT column `language` on `individual_speeches`.
 *  - Detects the language for each speech and writes a **two‑letter ISO‑639‑1 uppercase** code (e.g., "EN", "FR").
 *  - Uses a high‑accuracy, multi‑stage detector with EU‑language constraints to maximize correctness.
 *  - Keeps the DB work fast with a prepared UPDATE and batched transaction.
 *
 * Usage:
 *  npm i sqlite3 cld3-asm franc langs
 *  node detect-language-robust.js
 *
 * Notes:
 *  - Primary detector: CLD3 (neural, fast, reliable for long texts).
 *  - Fallback: franc (n-gram) restricted to the 24 official EU languages.
 *  - Hard heuristics:
 *      • Greek and Cyrillic script short-circuits (EL and BG respectively) for very high precision.
 *      • Chunked majority vote when the text is long or mixed.
 *  - If confidence is low or disagreement persists, we set NULL (instead of forcing EN). You can change NULL to 'EN' if you prefer.
 */

const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const { loadModule } = require('cld3-asm');
const franc = require('franc').franc;
const langs = require('langs');

// --- Configuration ---------------------------------------------------------
const DB_FILE = path.join(__dirname, 'ep_data.db');
const TABLE = 'individual_speeches';
const ID_COL = 'id';
const TEXT_COL = 'speech_content';
const LANG_COL = 'language';

// Confidence thresholds
const CLD3_MIN_PROB = 0.60;   // Lower threshold = more analysis
const CHUNK_SIZE = 600;       // Smaller chunks = more votes
const MAX_TEXT = 50000;       // Don't truncate as much

// If you prefer to default to 'EN' instead of NULL when undecided, set here:
const DEFAULT_WHEN_UNCERTAIN = null; // e.g. 'EN' or null

// EU languages (ISO 639‑1) and mappings to 639‑3 for franc
const EU_ISO2 = [
  'BG','CS','DA','DE','EL','EN','ES','ET','FI','FR','GA','HR','HU','IT','LT','LV','MT','NL','PL','PT','RO','SK','SL','SV'
];
const EU_SET = new Set(EU_ISO2);
const ISO2_TO_3 = {
  BG:'bul', CS:'ces', DA:'dan', DE:'deu', EL:'ell', EN:'eng', ES:'spa', ET:'est', FI:'fin', FR:'fra', GA:'gle',
  HR:'hrv', HU:'hun', IT:'ita', LT:'lit', LV:'lav', MT:'mlt', NL:'nld', PL:'pol', PT:'por', RO:'ron', SK:'slk', SL:'slv', SV:'swe'
};
const EU_3CODES = Object.values(ISO2_TO_3);

function toISO2Upper(s) {
  if (!s) return null;
  s = s.toLowerCase();
  // CLD3 returns iso2 (mostly). Map a few common oddballs here if needed.
  // Normalize and uppercase.
  const map = { he:'iw', yi:'ji' }; // kept for completeness; not EU languages.
  const maybe = map[s] || s;
  const code = maybe.length === 2 ? maybe : (langs.where('3', maybe) || {})['1']; // '1' is iso2 in langs
  if (!code) return null;
  const iso2 = code.toUpperCase();
  return EU_SET.has(iso2) ? iso2 : null; // constrain to EU set
}

function iso3ToIso2Upper(iso3) {
  const hit = Object.entries(ISO2_TO_3).find(([, v]) => v === iso3);
  return hit ? hit[0] : null;
}

// Quick script short-circuits (very high precision in EU set)
// Quick script short-circuits (very high precision in EU set)
function scriptHeuristic(text) {
    if (!text) return null;
    
    // Count total non-whitespace characters
    const totalChars = text.replace(/\s/g, '').length;
    if (totalChars < 20) return null; // Need minimum text length
    
    // Greek script - require at least 30% Greek characters
    const greekChars = (text.match(/[\u0370-\u03FF]/g) || []).length;
    if (greekChars > 0 && (greekChars / totalChars) >= 0.30) {
      return { lang: 'EL', conf: 0.999, via: 'script' };
    }
    
    // Cyrillic script - require at least 30% Cyrillic characters (for Bulgarian)
    const cyrillicChars = (text.match(/[\u0400-\u04FF]/g) || []).length;
    if (cyrillicChars > 0 && (cyrillicChars / totalChars) >= 0.30) {
      return { lang: 'BG', conf: 0.995, via: 'script' };
    }
    
    return null;
  }

// Chunk text for majority vote
function chunksOf(text, size = CHUNK_SIZE) {
  const t = text.length > MAX_TEXT ? text.slice(0, MAX_TEXT) : text;
  const parts = [];
  for (let i = 0; i < t.length; i += size) parts.push(t.slice(i, i + size));
  return parts;
}

function clean(text) {
  if (!text) return '';
  return String(text)
    .replace(/<[^>]+>/g, ' ')      // strip HTML tags
    .replace(/\s+/g, ' ')         // collapse whitespace
    .trim();
}

// Single-shot CLD3 detection (synchronous once initialized)
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

// Majority vote using CLD3 across chunks
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
  // Pick lang with best average score (probability proxy)
  let best = null; let bestAvg = -1;
  for (const [lang, { score, n }] of votes.entries()) {
    const avg = score / n;
    if (avg > bestAvg) { bestAvg = avg; best = lang; }
  }
  return { lang: best, conf: bestAvg, via: 'cld3-vote' };
}

// franc fallback, restricted to EU languages
function detectWithFranc(text) {
  // franc needs enough letters to be confident; set minLength modestly
  const lang3 = franc(text, { whitelist: EU_3CODES, minLength: 20 });
  if (!lang3 || lang3 === 'und') return null;
  const iso2 = iso3ToIso2Upper(lang3);
  if (!iso2) return null;
  return { lang: iso2, conf: 0.75, via: 'franc' }; // franc doesn't expose a calibrated probability
}

// Final decision logic combining all above
function decideLanguage(detectors, raw) {
  const text = clean(raw);
  if (!text) return null;

  // 1) Hard script signal
  const script = scriptHeuristic(text);
  if (script) return script;

  // 2) CLD3 one-shot
  const cldOne = detectWithCLD3(detectors.cld3, text);
  if (cldOne && cldOne.conf >= CLD3_MIN_PROB) return cldOne;

  // 3) CLD3 majority vote across chunks (helps with quotes/mixed snippets)
  const cldVote = voteCLD3(detectors.cld3, text);
  if (cldVote && cldVote.conf >= 0.72) return cldVote;

  // 4) franc fallback (EU‑only)
  const francRes = detectWithFranc(text);

  // 5) Tie‑break logic:
  //    - If CLD3 and franc agree, keep it.
  //    - If only one exists, keep it.
  //    - Else prefer CLD3 unless franc suggests EL or MT (which CLD3 sometimes underdetects for short texts).
  const candidates = [cldOne, cldVote, francRes].filter(Boolean);
  if (!candidates.length) return null;

  // Check agreement
  const byLang = new Map();
  for (const c of candidates) byLang.set(c.lang, (byLang.get(c.lang) || 0) + 1);
  let agreed = null; let maxVotes = 0;
  for (const [lang, n] of byLang.entries()) if (n > maxVotes) { maxVotes = n; agreed = lang; }
  if (maxVotes >= 2) return candidates.find(c => c.lang === agreed);

  // Preference rule
  const pref = candidates.find(c => c.via.startsWith('cld3'))
           || candidates.find(c => c.via === 'franc');

  // Special nudge for languages CLD3 can under-detect
  if (francRes && (francRes.lang === 'EL' || francRes.lang === 'MT') && (!pref || pref.conf < 0.7)) {
    return francRes;
  }
  return pref || candidates[0];
}

// --- DB helpers ------------------------------------------------------------
const db = new sqlite3.Database(DB_FILE);

function addLanguageColumn() {
  return new Promise((resolve, reject) => {
    db.run(`ALTER TABLE ${TABLE} ADD COLUMN ${LANG_COL} TEXT`, (err) => {
      if (err && !String(err.message).includes('duplicate column name')) return reject(err);
      resolve();
    });
  });
}

function clearLanguageValues() {
  return new Promise((resolve, reject) => {
    db.run(`UPDATE ${TABLE} SET ${LANG_COL} = NULL`, (err) => err ? reject(err) : resolve());
  });
}

function withTransaction(run) {
  return new Promise((resolve, reject) => {
    db.exec('BEGIN', (err) => {
      if (err) return reject(err);
      run((runErr) => {
        if (runErr) return db.exec('ROLLBACK', () => reject(runErr));
        db.exec('COMMIT', (commitErr) => commitErr ? reject(commitErr) : resolve());
      });
    });
  });
}

async function processAllRows(detectors) {
  console.log('▶️  Scanning speeches...');
  const update = db.prepare(`UPDATE ${TABLE} SET ${LANG_COL} = ? WHERE ${ID_COL} = ?`);
  let processed = 0;
  const tallies = Object.create(null);

  await withTransaction((done) => {
    db.each(
      `SELECT ${ID_COL} AS id, ${TEXT_COL} AS text FROM ${TABLE} ORDER BY ${ID_COL}`,
      (err, row) => {
        if (err) return done(err);
        const decision = decideLanguage(detectors, row.text);
        const lang = decision?.lang || DEFAULT_WHEN_UNCERTAIN;
        update.run(lang, row.id);
        processed += 1;
        if (lang) tallies[lang] = (tallies[lang] || 0) + 1;
        if (processed % 1000 === 0) process.stdout.write(`\r   Processed ${processed} speeches...`);
      },
      (finalErr, count) => {
        if (finalErr) return done(finalErr);
        update.finalize((e) => done(e));
      }
    );
  });

  console.log(`\n✅ Done. Updated ${processed} speeches.`);
  console.log('📊 Language counts (non‑NULL):');
  const sorted = Object.entries(tallies).sort((a,b) => b[1]-a[1]);
  for (const [lang, n] of sorted) console.log(`  ${lang}: ${n}`);
}

async function main() {
  console.log(' EU Language Detection (CLD3 + franc, EU‑constrained)');
  
  // Init CLD3 with correct import
  const cldFactory = await loadModule();
  const cld3 = cldFactory.create();
  const detectors = { cld3 };

  await addLanguageColumn();
  await clearLanguageValues();
  await processAllRows(detectors);

  db.close();
}

main().catch((err) => {
  console.error('\n❌ Failure:', err && err.stack ? err.stack : err);
  try { db.close(); } catch(_) {}
  process.exit(1);
});