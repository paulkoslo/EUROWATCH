/*
  Read-only topic mapping script (with optional apply flag)

  What it does:
  - Fetches a CRE sitting HTML (default: 2025-07-10) and parses agenda headers
  - Extracts canonical topic titles and document identifiers from the headers
  - Joins topics to existing DB rows using docIdentifier (primary) or label similarity (fallback)
  - Additionally maps topics directly to individual speeches by locating each speech snippet within the appropriate HTML section
  - Ensures a new column `topic` exists on `individual_speeches`
  - Dry-run by default: prints a mapping summary and writes a JSON file
  - When --apply is provided, updates ONLY `individual_speeches.topic` for matched sittings

  Usage:
    node map-topics-for-sitting.js --date 2025-07-10            # dry run
    node map-topics-for-sitting.js --date 2025-07-10 --apply    # write topics to DB
    node map-topics-for-sitting.js --all                        # process all dates in DB (dry run)
    node map-topics-for-sitting.js --all --apply                # process all dates and write topics
    node map-topics-for-sitting.js --all --apply --resume       # resume: only process sittings with 0 topics
*/

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');
const sqlite3 = require('sqlite3');

// Config
const WORKSPACE_ROOT = __dirname;
const DB_PATH = path.join(WORKSPACE_ROOT, 'ep_data.db');

function getArg(flag, defaultValue = null) {
  const idx = process.argv.indexOf(flag);
  if (idx !== -1 && idx + 1 < process.argv.length) return process.argv[idx + 1];
  return defaultValue;
}

const dateArg = getArg('--date', '2025-07-10');
const APPLY = process.argv.includes('--apply');
const RUN_ALL = process.argv.includes('--all');
const RESUME = process.argv.includes('--resume');

function normalizeText(text) {
  return (text || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenSet(text) {
  const norm = normalizeText(text);
  return new Set(norm.split(' ').filter(Boolean));
}

function jaccardSimilarity(aText, bText) {
  const a = tokenSet(aText);
  const b = tokenSet(bText);
  const intersection = new Set([...a].filter(x => b.has(x)));
  const unionSize = new Set([...a, ...b]).size || 1;
  return intersection.size / unionSize;
}

function createProgressBar(current, total, width = 28) {
  const ratio = Math.min(Math.max(current / Math.max(total, 1), 0), 1);
  const filled = Math.round(ratio * width);
  const bar = '█'.repeat(filled) + '░'.repeat(width - filled);
  const pct = (ratio * 100).toFixed(1).padStart(5, ' ');
  return `[${bar}] ${pct}% (${current}/${total})`;
}

function getSessionNumber(date) {
  if (!date) return 10;
  if (date >= '2024-07-16') return 10; // 10th term
  if (date >= '2019-07-02' && date < '2024-07-16') return 9; // 9th term
  if (date >= '2014-07-01' && date < '2019-07-02') return 8; // 8th term
  return 7; // older fallback
}

async function fetchSittingHTML(date, db) {
  // Prefer cached HTML content from DB if present
  const fromDb = await new Promise((resolve) => {
    db.get(
      `SELECT content FROM sittings WHERE activity_date = ? LIMIT 1`,
      [date],
      (err, row) => {
        if (err || !row || !row.content) return resolve(null);
        resolve(row.content);
      }
    );
  });
  // If DB content looks like full HTML, use it; otherwise fetch the HTML page
  if (fromDb && /<html|arrow_title_doc\.gif|<table|<td|<a\s+name=/i.test(fromDb)) {
    console.log(`🗄️  Using cached HTML from DB for ${date}`);
    return fromDb;
  }

  const session = getSessionNumber(date);
  const url = `https://www.europarl.europa.eu/doceo/document/CRE-${session}-${date}_EN.html`;
  console.log(`🌐 Fetching HTML (${session}) for ${date}: ${url}`);
  const res = await axios.get(url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; EUROWATCH/1.0)' },
    timeout: 20000
  });
  return res.data;
}

function parseTopicsFromHTML(html) {
  const $ = cheerio.load(html);
  const topics = [];

  // Strategy: find the header rows that have the arrow_title_doc.gif image, then extract
  // - ordinal number (e.g., "11.2")
  // - title text (strip ordinal and trailing brackets)
  // - docIdentifier from any link inside the header cell (href contains '/doceo/document/XXXX_EN.html')
  $('td.doc_title:has(img[src*="arrow_title_doc.gif"])').each((_, cell) => {
    const td = $(cell);
    if (!td || td.length === 0) return;

    const raw = td.text().replace(/\s+/g, ' ').trim();
    if (!raw) return;

    // Extract ordinal prefix like "11." or "11.2."
    const ordinalMatch = raw.match(/^(\d+(?:\.\d+)*)\s*\./);
    const ordinal = ordinalMatch ? ordinalMatch[1] : null;

    // Extract docIdentifier from href
    let docIdentifier = null;
    const link = td.find('a[href*="/doceo/document/"]').filter((i, a) => /_EN\.html$/i.test($(a).attr('href') || ''))[0];
    if (link) {
      const href = $(link).attr('href');
      const idMatch = href && href.match(/\/doceo\/document\/([^/_]+(?:-[^/_]+)*)_EN\.html/i);
      if (idMatch) docIdentifier = idMatch[1];
    }

    // Clean title: remove ordinal prefix and any trailing bracketed refs
    let title = raw;
    if (ordinal) title = title.replace(new RegExp('^' + ordinal.replace('.', '\\.') + '\\.'), '').trim();
    title = title.replace(/\s*\([^)]*\)\s*$/, '').trim();

    // Skip empty titles
    if (!title) return;

    topics.push({ ordinal, title, docIdentifier, raw });
  });

  // De-duplicate by docIdentifier/title combo
  const seen = new Set();
  const deduped = [];
  for (const t of topics) {
    const key = `${t.docIdentifier || ''}::${normalizeText(t.title)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(t);
  }
  return deduped;
}

function splitHtmlIntoSections(html) {
  const $ = cheerio.load(html);
  const headers = $('td.doc_title:has(img[src*="arrow_title_doc.gif"])').toArray();
  const sections = [];
  if (headers.length === 0) return sections;

  // Build sections using raw HTML slice positions for robustness
  const rawHtml = $.root().html();
  let searchFrom = 0;
  const headerSpans = headers.map((el) => {
    const tdHtml = $.html(el);
    const start = rawHtml.indexOf(tdHtml, searchFrom);
    if (start !== -1) searchFrom = start + tdHtml.length;
    const rawText = $(el).text().replace(/\s+/g, ' ').trim();
    let title = rawText.replace(/^(\d+(?:\.\d+)*)\s*\./, '').trim();
    title = title.replace(/\s*\([^)]*\)\s*$/, '').trim();
    let docIdentifier = null;
    const link = $(el).find('a[href*="/doceo/document/"]').filter((i, a) => /_EN\.html$/i.test($(a).attr('href') || ''))[0];
    if (link) {
      const href = $(link).attr('href');
      const idMatch = href && href.match(/\/doceo\/document\/([^/_]+(?:-[^/_]+)*)_EN\.html/i);
      if (idMatch) docIdentifier = idMatch[1];
    }
    return { start, title, docIdentifier };
  }).filter(h => h.start !== -1);

  for (let i = 0; i < headerSpans.length; i++) {
    const start = headerSpans[i].start;
    const end = i + 1 < headerSpans.length ? headerSpans[i + 1].start : rawHtml.length;
    const slice = rawHtml.slice(start, end);
    const text = cheerio.load(slice)('body').text().replace(/\s+/g, ' ').trim();
    sections.push({ title: headerSpans[i].title, rawTitle: headerSpans[i].title, docIdentifier: headerSpans[i].docIdentifier, text });
  }
  return sections;
}

function normalizeForSearch(text) {
  return (text || '')
    .toLowerCase()
    .replace(/[\u00A0]/g, ' ')        // nbsp → space
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')  // strip diacritics
    .replace(/[^a-z0-9\s]/g, ' ')      // remove punctuation
    .replace(/\s+/g, ' ')              // collapse spaces
    .trim();
}

function findHeaderOffsetsInPageText(topics, pageTextNorm) {
  const offsets = [];
  for (const t of topics) {
    const key = normalizeForSearch(t.raw);
    const idx = key ? pageTextNorm.indexOf(key) : -1;
    if (idx !== -1) offsets.push({ start: idx, title: t.title, docIdentifier: t.docIdentifier || null });
  }
  // sort by start asc
  offsets.sort((a, b) => a.start - b.start);
  return offsets;
}

function findSpeechIndexInPageText(speechContent, pageTextNorm) {
  const text = normalizeForSearch(speechContent);
  if (!text || text.length < 40) return -1;
  const candidates = [];
  const starts = [0, 30, 60, 90];
  for (const s of starts) {
    const snippet = text.slice(s, s + 140);
    if (snippet.length < 60) continue;
    candidates.push(snippet);
  }
  for (const snip of candidates) {
    const idx = pageTextNorm.indexOf(snip);
    if (idx !== -1) return idx;
  }
  return -1;
}

function openDb() {
  return new sqlite3.Database(DB_PATH);
}

function ensureTopicColumn(db) {
  return new Promise((resolve, reject) => {
    db.all('PRAGMA table_info(individual_speeches)', (err, columns) => {
      if (err) return reject(err);
      const hasTopic = columns.some(c => c.name === 'topic');
      if (hasTopic) return resolve(false);
      db.run('ALTER TABLE individual_speeches ADD COLUMN topic TEXT', err2 => {
        if (err2) return reject(err2);
        resolve(true);
      });
    });
  });
}

function getSittingsForDate(db, date) {
  return new Promise((resolve, reject) => {
    db.all(
      `SELECT id, label, docIdentifier, notationId, activity_date FROM sittings WHERE activity_date = ?`,
      [date],
      (err, rows) => (err ? reject(err) : resolve(rows || []))
    );
  });
}

function getIndividualSpeechCounts(db, sittingIds) {
  if (sittingIds.length === 0) return Promise.resolve([]);
  const placeholders = sittingIds.map(() => '?').join(',');
  return new Promise((resolve, reject) => {
    db.all(
      `SELECT sitting_id, COUNT(*) as cnt FROM individual_speeches WHERE sitting_id IN (${placeholders}) GROUP BY sitting_id`,
      sittingIds,
      (err, rows) => (err ? reject(err) : resolve(rows || []))
    );
  });
}

function checkSittingHasTopics(db, date) {
  return new Promise((resolve, reject) => {
    db.get(
      `SELECT 
        COUNT(i.id) as total_speeches,
        COUNT(i.topic) as speeches_with_topics
       FROM sittings s
       LEFT JOIN individual_speeches i ON s.id = i.sitting_id
       WHERE s.activity_date = ?
       GROUP BY s.activity_date`,
      [date],
      (err, row) => {
        if (err) return reject(err);
        if (!row) return resolve({ hasTopics: false, totalSpeeches: 0, speechesWithTopics: 0 });
        resolve({
          hasTopics: row.speeches_with_topics > 0,
          totalSpeeches: row.total_speeches,
          speechesWithTopics: row.speeches_with_topics
        });
      }
    );
  });
}

function updateTopicsForSitting(db, sittingId, topic) {
  return new Promise((resolve, reject) => {
    db.run(
      `UPDATE individual_speeches SET topic = ? WHERE sitting_id = ?`,
      [topic, sittingId],
      function(err) {
        if (err) return reject(err);
        resolve(this.changes || 0);
      }
    );
  });
}

async function processOneDate(db, d) {
  console.log(`\n🔎 Mapping topics for date ${d} (apply=${APPLY ? 'yes' : 'no'})`);

  // Check if this sitting already has topics (resume mode)
  if (RESUME) {
    const topicCheck = await checkSittingHasTopics(db, d);
    if (topicCheck.hasTopics) {
      console.log(`⏭️  Skipping ${d}: already has ${topicCheck.speechesWithTopics}/${topicCheck.totalSpeeches} speeches with topics`);
      return { matchedSpeeches: 0, appliedIndividual: 0, appliedBySitting: 0 };
    } else {
      console.log(`🔄 Processing ${d}: ${topicCheck.totalSpeeches} speeches, 0 with topics`);
    }
  }

  const html = await fetchSittingHTML(d, db);
  const topics = parseTopicsFromHTML(html);
  console.log(`📄 Parsed ${topics.length} topics from HTML`);

  // 2) Read sittings from DB for the date
  const addedCol = await ensureTopicColumn(db).catch(err => { throw new Error('Failed to ensure topic column: ' + err.message); });
  if (addedCol) console.log('🆕 Added column individual_speeches.topic');
  const sittings = await getSittingsForDate(db, d);
  console.log(`🗄️ Found ${sittings.length} sittings in DB for ${d}`);

  // 3) Join topics → sittings
  const matches = [];
  for (const s of sittings) {
    let match = null;
    if (s.docIdentifier) {
      match = topics.find(t => t.docIdentifier && t.docIdentifier.toLowerCase() === s.docIdentifier.toLowerCase());
    }
    if (!match) {
      // Fallback: label similarity
      let best = null;
      let bestScore = 0;
      for (const t of topics) {
        const score = jaccardSimilarity(s.label || '', t.title || '');
        if (score > bestScore) {
          bestScore = score;
          best = t;
        }
      }
      if (best && bestScore >= 0.6) match = best;
    }
    if (match) {
      matches.push({
        sitting_id: s.id,
        date: s.activity_date,
        db_label: s.label,
        docIdentifier: s.docIdentifier || null,
        matched_title: match.title,
        matched_docIdentifier: match.docIdentifier || null,
        matched_by: (s.docIdentifier && match.docIdentifier && normalizeText(s.docIdentifier) === normalizeText(match.docIdentifier)) ? 'docIdentifier' : 'label-similarity'
      });
    } else {
      matches.push({
        sitting_id: s.id,
        date: s.activity_date,
        db_label: s.label,
        docIdentifier: s.docIdentifier || null,
        matched_title: null,
        matched_docIdentifier: null,
        matched_by: 'unmatched'
      });
    }
  }

  // 4) Summarize and write mapping file
  const outDir = path.join(WORKSPACE_ROOT, 'analysis');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `topic-mapping-${d}.json`);
  fs.writeFileSync(outPath, JSON.stringify({ date: d, matches }, null, 2));
  const matchedCount = matches.filter(m => m.matched_title).length;
  console.log(`📝 Wrote mapping file: ${outPath}`);
  console.log(`✅ Sitting-level header matches: ${matchedCount}/${matches.length}`);

  // 5) Attempt direct mapping for individual speeches via HTML sections (dry-run report + optional apply)
  const sections = splitHtmlIntoSections(html);
  // Use DOM-derived sections to avoid picking up the top "Index" TOC
  const rangedSections = sections.map(s => ({
    title: s.title,
    docIdentifier: s.docIdentifier,
    textNorm: normalizeForSearch(s.text || '')
  }));
  const speechRows = await new Promise((resolve, reject) => {
    db.all(
      `SELECT i.id, i.sitting_id, i.speaker_name, i.speech_order, i.title, i.speech_content
       FROM individual_speeches i
       JOIN sittings s ON s.id = i.sitting_id
       WHERE s.activity_date = ?
       ORDER BY i.speech_order ASC`,
      [d],
      (err, rows) => (err ? reject(err) : resolve(rows || []))
    );
  });

  function bestSectionForSpeech(speech) {
    const content = speech.speech_content || '';
    const speechNorm = normalizeForSearch(content);
    if (!speechNorm || speechNorm.length < 50) return null;

    // Strategy 1: try direct inclusion of a few speech snippets in each section
    const pageMatches = [];
    const candidates = [];
    const rawNorm = normalizeForSearch(content);
    const starts = [0, 40, 80, 120];
    for (const s of starts) {
      const snip = rawNorm.slice(s, s + 160);
      if (snip.length >= 80) candidates.push(snip);
    }
    for (let idx = 0; idx < rangedSections.length; idx++) {
      const sec = rangedSections[idx];
      if (!sec.textNorm || sec.textNorm.length < 100) continue;
      const hit = candidates.some(sn => sec.textNorm.indexOf(sn) !== -1);
      if (hit) return { section: { title: sec.title, docIdentifier: sec.docIdentifier }, score: 1.0, index: -1 };
    }

    // Strategy 2: coverage score against ranged sections
    const speechTokens = speechNorm.split(' ').filter(Boolean);
    const speechSet = new Set(speechTokens);
    let best = null;
    let bestScore = 0;
    for (const sec of rangedSections) {
      if (!sec.textNorm) continue;
      let hit = 0;
      for (const tok of speechSet) {
        if (tok.length <= 3) continue;
        if (sec.textNorm.indexOf(tok) !== -1) hit++;
      }
      const score = hit / Math.max(1, speechSet.size);
      if (score > bestScore) {
        bestScore = score;
        best = sec;
      }
    }
    if (!best || bestScore < 0.08) return null;
    return { section: { title: best.title, docIdentifier: best.docIdentifier }, score: bestScore, index: -1 };
  }

  const perSpeechResults = [];
  for (const sp of speechRows) {
    const res = bestSectionForSpeech(sp);
    if (res) {
      perSpeechResults.push({
        speech_id: sp.id,
        speech_order: sp.speech_order,
        speaker: sp.speaker_name,
        topic: res.section.title,
        docIdentifier: res.section.docIdentifier || null,
        score: Number(res.score.toFixed(3)),
        index: res.index
      });
    } else {
      perSpeechResults.push({
        speech_id: sp.id,
        speech_order: sp.speech_order,
        speaker: sp.speaker_name,
        topic: null,
        docIdentifier: null,
        score: 0
      });
    }
  }

  // Only write debug JSON in dry-run
  const matchedSpeeches = perSpeechResults.filter(r => r.topic).length;
  console.log(`🎯 Individual speech topics matched: ${matchedSpeeches}/${perSpeechResults.length}`);
  if (!APPLY) {
    const perSpeechOut = path.join(outDir, `topic-mapping-speeches-${d}.json`);
    fs.writeFileSync(perSpeechOut, JSON.stringify({ date: d, results: perSpeechResults }, null, 2));
    console.log(`📝 Wrote per-speech mapping file: ${perSpeechOut}`);
  }

  let appliedIndividual = 0;
  if (APPLY && matchedSpeeches > 0) {
    let total = 0;
    for (const r of perSpeechResults) {
      if (!r.topic) continue;
      await new Promise((resolve, reject) => {
        db.run('UPDATE individual_speeches SET topic = ? WHERE id = ?', [r.topic, r.speech_id], function(err) {
          if (err) return reject(err);
          total += this.changes || 0;
          resolve();
        });
      });
    }
    appliedIndividual = total;
    console.log(`💾 Applied individual topics: ${total}`);
  } else {
    console.log('🔒 Per-speech mapping dry-run complete. Pass --apply to write topics.');
  }

  // 5) Apply updates if requested
  let appliedBySitting = 0;
  if (APPLY) {
    for (const m of matches) {
      if (!m.matched_title) continue;
      const changed = await updateTopicsForSitting(db, m.sitting_id, m.matched_title).catch(err => {
        console.error(`❌ Failed to update sitting ${m.sitting_id}:`, err.message);
        return 0;
      });
      appliedBySitting += changed;
    }
    console.log(`💾 Applied by sitting header: ${appliedBySitting}`);
  } else {
    console.log('🔒 Dry-run only. Pass --apply to write topics into individual_speeches.topic');
  }

  return { matchedSpeeches, appliedIndividual, appliedBySitting };
}

async function main() {
  const db = openDb();
  await new Promise(r => db.serialize(r));

  if (RUN_ALL) {
    const dates = await new Promise((resolve, reject) => {
      db.all(`SELECT DISTINCT activity_date AS d FROM sittings WHERE activity_date IS NOT NULL ORDER BY d DESC`, [], (err, rows) => {
        if (err) return reject(err);
        resolve(rows.map(r => r.d));
      });
    });
    console.log(`📅 Processing ${dates.length} dates...`);
    let totalMatched = 0;
    let totalApplied = 0;
    const start = Date.now();
    for (let i = 0; i < dates.length; i++) {
      const d = dates[i];
      try {
        const { matchedSpeeches, appliedIndividual, appliedBySitting } = await processOneDate(db, d);
        totalMatched += matchedSpeeches;
        totalApplied += appliedIndividual + appliedBySitting;
        const elapsed = (Date.now() - start) / 1000;
        const rate = (i + 1) / Math.max(elapsed, 0.1);
        const remaining = dates.length - (i + 1);
        const etaSec = Math.round(remaining / Math.max(rate, 0.001));
        process.stdout.write(`\r${createProgressBar(i + 1, dates.length)} | matched ${totalMatched} | applied ${totalApplied} | ETA ${etaSec}s   `);
      } catch (e) {
        console.error(`❌ Error on ${d}:`, e.message);
      }
    }
    console.log(`\n🎉 Finished all dates. Total matched speeches: ${totalMatched}. Total rows updated: ${totalApplied}`);
    db.close();
    return;
  }

  await processOneDate(db, dateArg);
  db.close();
}

main().catch(err => {
  console.error('❌ Error:', err.stack || err.message);
  process.exit(1);
});


