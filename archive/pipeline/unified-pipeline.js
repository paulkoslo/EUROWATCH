#!/usr/bin/env node
/**
 * Unified Pipeline: Fetch → Parse → Extract Topics → Agent Classify → Store
 * Tests on the most recent sitting date NOT already in DB.
 * Can be run via CLI or required as module for API.
 */

require('dotenv').config();
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const cheerio = require('cheerio');
const { discoverNextSittingDate, fetchSittingHTML } = require('../../core/parliament-fetch');
const { classifyTopics } = require('../../core/topic-agent');
const { DB_PATH } = require('../../core/db');

// --- Normalize helpers (from map-topics-for-sitting) ---
function normalizeText(text) {
  return (text || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeForSearch(text) {
  return (text || '')
    .toLowerCase()
    .replace(/[\u00A0]/g, ' ')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// --- Parse topics from HTML (from map-topics-for-sitting) ---
function parseTopicsFromHTML(html) {
  const $ = cheerio.load(html);
  const topics = [];
  $('td.doc_title:has(img[src*="arrow_title_doc.gif"])').each((_, cell) => {
    const td = $(cell);
    const raw = td.text().replace(/\s+/g, ' ').trim();
    if (!raw) return;
    const ordinalMatch = raw.match(/^(\d+(?:\.\d+)*)\s*\./);
    const ordinal = ordinalMatch ? ordinalMatch[1] : null;
    let docIdentifier = null;
    const link = td.find('a[href*="/doceo/document/"]').filter((i, a) => /_EN\.html$/i.test($(a).attr('href') || ''))[0];
    if (link) {
      const href = $(link).attr('href');
      const idMatch = href && href.match(/\/doceo\/document\/([^/_]+(?:-[^/_]+)*)_EN\.html/i);
      if (idMatch) docIdentifier = idMatch[1];
    }
    let title = raw;
    if (ordinal) title = title.replace(new RegExp('^' + ordinal.replace('.', '\\.') + '\\.'), '').trim();
    title = title.replace(/\s*\([^)]*\)\s*$/, '').trim();
    if (!title) return;
    topics.push({ ordinal, title, docIdentifier, raw });
  });
  const seen = new Set();
  return topics.filter(t => {
    const key = `${t.docIdentifier || ''}::${normalizeText(t.title)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function splitHtmlIntoSections(html) {
  const $ = cheerio.load(html);
  const headers = $('td.doc_title:has(img[src*="arrow_title_doc.gif"])').toArray();
  const sections = [];
  if (headers.length === 0) return sections;
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
    sections.push({ title: headerSpans[i].title, docIdentifier: headerSpans[i].docIdentifier, text });
  }
  return sections;
}

// --- Extract plain text from HTML for speech parsing ---
function extractTextFromHTML(html) {
  const $ = cheerio.load(html);
  const main = $('.doc-content, .ep_text, .content, #content, .main-content').first();
  if (main.length > 0 && main.text().trim().length > 100) return main.text().trim();
  const body = $('body').text().trim();
  if (body.length > 100) return body;
  return $('p').map((i, el) => $(el).text()).get().join('\n').trim();
}

// --- Parse individual speeches (from reparse-with-parentheses) ---
function parseIndividualSpeeches(content, sittingId, log = () => {}) {
  const speeches = [];
  const lines = content.split('\n');
  let currentSpeech = null;
  let speechOrder = 0;

  const partyPattern = /^(PPE|S&D|ECR|Renew|Verts\/ALE|ID|The Left|NI|ALDE)$/i;
  const partyIndicators = ['on behalf of', 'au nom de', 'a nome del', 'en nombre del', 'im Namen der', 'fraktion', 'gruppo', 'grupo', 'group', 'groupe', 'εξ ονόματος', 'namens', 'w imieniu', 'în numele', 'for ', 'för ', 'thar ceann', 'u ime', 'za skupinu', 'em nome', "(PPE)", "(S&D)", "(ECR)", "(Renew)", "(Verts/ALE)", "(ID)", "(The Left)", "(NI)", "(ALDE)"];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    let speechMatch = line.match(/^([^,]+),\s*(.+?)\.\s*–\s*(.+)$/);
    let speakerName, roleInfo, speechContent;

    if (speechMatch) {
      speakerName = speechMatch[1].trim();
      roleInfo = speechMatch[2].trim();
      speechContent = speechMatch[3].trim();
    } else {
      speechMatch = line.match(/^([^(]+)\s*\(([^)]+)\)\.\s*–\s*(.+)$/);
      if (speechMatch) {
        speakerName = speechMatch[1].trim();
        roleInfo = speechMatch[2].trim();
        speechContent = speechMatch[3].trim();
      } else {
        speechMatch = line.match(/^([^(]+)\s*\(([^)]+)\),\s*(.+?)\.\s*–\s*(.+)$/);
        if (speechMatch) {
          speakerName = speechMatch[1].trim();
          roleInfo = speechMatch[3].trim();
          speechContent = speechMatch[4].trim();
        } else {
          speechMatch = line.match(/^([^.]+)\.\s*–\s*(.+)$/);
          if (speechMatch) {
            speakerName = speechMatch[1].trim();
            roleInfo = '';
            speechContent = speechMatch[2].trim();
          }
        }
      }
    }

    if (speechMatch) {
      if (currentSpeech) speeches.push(currentSpeech);

      let politicalGroup = null;
      let title = null;
      const nameWithPartyMatch = speakerName.match(/^(.+?)\s*\(([^)]+)\)$/);
      if (nameWithPartyMatch && partyPattern.test(nameWithPartyMatch[2].trim())) {
        politicalGroup = nameWithPartyMatch[2].trim();
        speakerName = nameWithPartyMatch[1].trim();
      }
      if (!politicalGroup && roleInfo && partyPattern.test(roleInfo)) politicalGroup = roleInfo;
      else if (roleInfo && partyIndicators.some(p => roleInfo.includes(p))) politicalGroup = roleInfo;
      else if (roleInfo) title = roleInfo;

      currentSpeech = {
        sitting_id: sittingId,
        speaker_name: speakerName,
        political_group: politicalGroup,
        title,
        speech_content: speechContent,
        speech_order: ++speechOrder,
        mep_id: null
      };
    } else if (currentSpeech) {
      currentSpeech.speech_content += ' ' + line;
    }
  }
  if (currentSpeech) speeches.push(currentSpeech);
  return speeches;
}

// --- Map speech to section (topic) by content overlap ---
function bestSectionForSpeech(speech, rangedSections) {
  const content = speech.speech_content || '';
  const speechNorm = normalizeForSearch(content);
  if (!speechNorm || speechNorm.length < 50) return null;

  const candidates = [];
  for (const s of [0, 40, 80, 120]) {
    const snip = speechNorm.slice(s, s + 160);
    if (snip.length >= 80) candidates.push(snip);
  }
  for (const sec of rangedSections) {
    if (!sec.textNorm || sec.textNorm.length < 100) continue;
    if (candidates.some(sn => sec.textNorm.indexOf(sn) !== -1))
      return { section: { title: sec.title, docIdentifier: sec.docIdentifier }, score: 1.0 };
  }

  const speechTokens = speechNorm.split(' ').filter(Boolean);
  const speechSet = new Set(speechTokens);
  let best = null, bestScore = 0;
  for (const sec of rangedSections) {
    if (!sec.textNorm) continue;
    let hit = 0;
    for (const tok of speechSet) {
      if (tok.length <= 3) continue;
      if (sec.textNorm.indexOf(tok) !== -1) hit++;
    }
    const score = hit / Math.max(1, speechSet.size);
    if (score > bestScore) { bestScore = score; best = sec; }
  }
  if (!best || bestScore < 0.08) return null;
  return { section: { title: best.title, docIdentifier: best.docIdentifier }, score: bestScore };
}

// --- MEP linking (FIX: use id not identifier) ---
async function linkSpeechesToMeps(db, log) {
  return new Promise((resolve, reject) => {
    db.all(`SELECT DISTINCT speaker_name FROM individual_speeches WHERE speaker_name IS NOT NULL AND mep_id IS NULL`, async (err, speakers) => {
      if (err) return reject(err);
      if (!speakers || speakers.length === 0) return resolve(0);
      log(`  Linking ${speakers.length} unlinked speakers to MEPs...`);

      let linkedCount = 0;
      for (const s of speakers) {
        const mep = await new Promise(r => {
          db.get(`SELECT id FROM meps WHERE label LIKE ? OR label LIKE ?`,
            [`%${s.speaker_name}%`, `%${s.speaker_name.split(' ').reverse().join(' ')}%`], (e, row) => r(row));
        });
        if (mep) {
          await new Promise(r => {
            db.run(`UPDATE individual_speeches SET mep_id = ? WHERE speaker_name = ? AND mep_id IS NULL`, [mep.id, s.speaker_name], function(er) {
              if (!er && this.changes > 0) linkedCount++;
              r();
            });
          });
        }
      }
      resolve(linkedCount);
    });
  });
}

/**
 * Run the full pipeline for one date (or discover latest)
 */
async function runPipeline(options = {}) {
  const log = options.log || console.log;
  const targetDate = options.date || null; // if set, use this date; else find latest not in DB

  const db = new sqlite3.Database(DB_PATH);
  const run = async () => {
    try {
      log('═══════════════════════════════════════════════════════════════');
      log('  EUROWATCH UNIFIED PIPELINE — Fetch → Parse → Topics → Store');
      log('═══════════════════════════════════════════════════════════════');

      // 1. Determine target date
      let date = targetDate;
      if (!date) {
        log('\n[STEP 1] Discovering latest sitting date not in DB...');
        const speeches = await fetchRecentSpeeches(500);
        const dateSet = new Set();
        for (const s of speeches) {
          const d = s.activity_date || s.activity_start_date;
          if (d) dateSet.add(d);
        }
        const sortedDates = [...dateSet].sort().reverse();

        const existingDates = await new Promise((resolve, reject) => {
          db.all('SELECT DISTINCT activity_date FROM sittings WHERE LENGTH(content) > 100', [], (err, rows) => {
            if (err) return reject(err);
            resolve(new Set((rows || []).map(r => r.activity_date)));
          });
        });

        date = sortedDates.find(d => !existingDates.has(d));
        if (!date) {
          log('  No new dates found. All recent sittings already in DB.');
          return { success: false, message: 'No new sitting dates to process' };
        }
        log(`  Target date: ${date} (first date not in DB)`);
      } else {
        log(`\n[STEP 1] Using provided date: ${date}`);
      }

      // 2. Fetch HTML
      log('\n[STEP 2] Fetching sitting HTML...');
      const html = await fetchSittingHTML(date);
      if (!html || html.length < 500) {
        log('  Failed to fetch HTML or content too short.');
        return { success: false, message: 'HTML fetch failed' };
      }
      log(`  Fetched ${html.length} chars`);

      // 3. Parse topics from HTML
      log('\n[STEP 3] Parsing topics from HTML agenda...');
      const rawTopics = parseTopicsFromHTML(html);
      log(`  Found ${rawTopics.length} agenda topics`);

      // 4. Agent: classify topics to macro/micro
      let topicMap = {}; // title -> { macro_topic, specific_focus, confidence }
      if (rawTopics.length > 0 && process.env.OPENAI_API_KEY) {
        log('\n[STEP 4] Agent: Classifying topics to macro/micro taxonomy...');
        const classified = await classifyTopics(rawTopics.map(t => t.title), log);
        for (const c of classified) {
          topicMap[c.topic] = { macro_topic: c.macro_topic, specific_focus: c.specific_focus, confidence: c.confidence };
        }
      } else {
        log('\n[STEP 4] Skipping AI classification (no OPENAI_API_KEY or no topics)');
      }

      // 5. Extract text and parse speeches
      log('\n[STEP 5] Parsing individual speeches...');
      const textContent = extractTextFromHTML(html);
      const sittingId = `sitting-${date}`;
      const speeches = parseIndividualSpeeches(textContent, sittingId, log);
      log(`  Parsed ${speeches.length} individual speeches`);

      if (speeches.length === 0) {
        log('  No speeches parsed. Storing sitting with raw content only.');
      }

      // 6. Insert sitting
      log('\n[STEP 6] Storing sitting...');
      await new Promise((resolve, reject) => {
        db.run(`INSERT OR REPLACE INTO sittings (id, type, label, activity_date, content, last_updated) VALUES (?, ?, ?, ?, ?, ?)`,
          [sittingId, 'PLENARY_DEBATE', `Parliamentary Sitting - ${date}`, date, html, Date.now()],
          err => err ? reject(err) : resolve());
      });
      log('  Sitting stored.');

      // 7. Ensure schema
      await new Promise(r => {
        db.exec(`
          ALTER TABLE individual_speeches ADD COLUMN topic TEXT;
          ALTER TABLE individual_speeches ADD COLUMN macro_topic TEXT;
          ALTER TABLE individual_speeches ADD COLUMN macro_specific_focus TEXT;
          ALTER TABLE individual_speeches ADD COLUMN macro_confidence REAL;
        `, () => r());
      });

      // 8. Map topics to speeches and insert
      log('\n[STEP 7] Mapping topics to speeches and storing...');
      const sections = splitHtmlIntoSections(html);
      const rangedSections = sections.map(s => ({ ...s, textNorm: normalizeForSearch(s.text || '') }));

      const stmt = db.prepare(`
        INSERT INTO individual_speeches (sitting_id, speaker_name, political_group, title, speech_content, speech_order, mep_id, topic, macro_topic, macro_specific_focus, macro_confidence)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);

      for (const sp of speeches) {
        const match = bestSectionForSpeech(sp, rangedSections);
        let topic = null, macro_topic = null, macro_specific_focus = null, macro_confidence = null;
        if (match) {
          topic = match.section.title;
          const meta = topicMap[topic];
          if (meta) {
            macro_topic = meta.macro_topic;
            macro_specific_focus = meta.specific_focus;
            macro_confidence = meta.confidence;
          }
        }
        stmt.run(sp.sitting_id, sp.speaker_name, sp.political_group, sp.title, sp.speech_content, sp.speech_order, sp.mep_id, topic, macro_topic, macro_specific_focus, macro_confidence);
      }
      stmt.finalize();
      log(`  Stored ${speeches.length} speeches`);

      // 9. Link to MEPs
      log('\n[STEP 8] Linking speeches to MEPs...');
      const linkedCount = await linkSpeechesToMeps(db, log);
      log(`  Linked ${linkedCount} speeches to MEPs`);

      log('\n═══════════════════════════════════════════════════════════════');
      log('  PIPELINE COMPLETE');
      log('═══════════════════════════════════════════════════════════════');
      log(`  Date: ${date}`);
      log(`  Speeches: ${speeches.length}`);
      log(`  Topics: ${rawTopics.length}`);
      log(`  MEPs linked: ${linkedCount}`);

      return { success: true, date, speechesCount: speeches.length, topicsCount: rawTopics.length, linkedCount };
    } catch (err) {
      log(`\n❌ Pipeline error: ${err.message}`);
      throw err;
    } finally {
      db.close();
    }
  };

  return run();
}

// CLI
if (require.main === module) {
  const dateArg = process.argv.indexOf('--date') !== -1 && process.argv[process.argv.indexOf('--date') + 1];
  runPipeline({ date: dateArg || null })
    .then(r => process.exit(r.success ? 0 : 1))
    .catch(() => process.exit(1));
}

module.exports = { runPipeline };
