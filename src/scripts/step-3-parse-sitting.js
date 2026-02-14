#!/usr/bin/env node
/**
 * Step 3: Parse sitting HTML → topics + speeches + sections (for topic mapping)
 *
 * Usage: node scripts/step-3-parse-sitting.js < html.txt
 *   or:  node scripts/step-3-parse-sitting.js --date YYYY-MM-DD (reads from step-2)
 *
 * Exports: parseSitting(html, sittingId) → { topics, speeches, sections }
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const {
  parseTopicsFromHTML,
  splitHtmlIntoSections,
  extractTextFromHTML,
  parseIndividualSpeeches,
  normalizeForSearch
} = require('../core/parse-helpers');
const { fetchSittingHTML } = require('../core/parliament-fetch');

function parseSitting(html, sittingId, log = () => {}) {
  log('  Parsing topics from HTML...');
  const topics = parseTopicsFromHTML(html);
  log(`  Found ${topics.length} agenda topics`);

  log('  Splitting HTML into sections...');
  const sections = splitHtmlIntoSections(html);
  const rangedSections = sections.map(s => ({ ...s, textNorm: normalizeForSearch(s.text || '') }));

  log('  Extracting text and parsing speeches...');
  const textContent = extractTextFromHTML(html);
  const speeches = parseIndividualSpeeches(textContent, sittingId);
  log(`  Parsed ${speeches.length} individual speeches`);

  return { topics, speeches, sections: rangedSections };
}

async function run(htmlOrDate, sittingId, options = {}) {
  const log = options.log || (() => {});
  let html = htmlOrDate;

  if (typeof htmlOrDate === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(htmlOrDate)) {
    const date = htmlOrDate;
    sittingId = sittingId || `sitting-${date}`;
    html = await fetchSittingHTML(date);
  } else if (typeof htmlOrDate === 'string') {
    sittingId = sittingId || `sitting-unknown`;
  } else {
    throw new Error('htmlOrDate must be HTML string or YYYY-MM-DD');
  }

  return parseSitting(html, sittingId, log);
}

if (require.main === module) {
  const dateIdx = process.argv.indexOf('--date');
  const date = dateIdx !== -1 && process.argv[dateIdx + 1] ? process.argv[dateIdx + 1] : null;
  const outIdx = process.argv.indexOf('--out');
  const outFile = outIdx !== -1 && process.argv[outIdx + 1] ? process.argv[outIdx + 1] : null;

  (async () => {
    let html;
    if (date) {
      html = await fetchSittingHTML(date);
    } else {
      html = fs.readFileSync(0, 'utf8');
    }
    const sittingId = date ? `sitting-${date}` : 'sitting-unknown';
    const result = parseSitting(html, sittingId, console.log);

    const output = JSON.stringify({
      sittingId,
      topicsCount: result.topics.length,
      speechesCount: result.speeches.length,
      topics: result.topics.map(t => ({ title: t.title, docIdentifier: t.docIdentifier })),
      speeches: result.speeches.map(s => ({
        speaker_name: s.speaker_name,
        political_group: s.political_group,
        speech_order: s.speech_order,
        content_preview: (s.speech_content || '').slice(0, 80) + '...'
      })),
      sections: result.sections.map(s => ({ title: s.title, textLen: (s.text || '').length }))
    }, null, 2);

    if (outFile) {
      fs.writeFileSync(outFile, output);
      console.log(`Written to ${outFile}`);
    } else {
      console.log(output);
    }
    process.exit(0);
  })().catch(err => {
    console.error('Error:', err.message);
    process.exit(1);
  });
}

module.exports = { parseSitting, run };
