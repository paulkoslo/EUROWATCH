#!/usr/bin/env node
/**
 * Full pipeline: runs all steps in sequence
 * Step 1: Discover date (HTML scrape, no API)
 * Step 2: Fetch HTML
 * Step 3: Parse sitting (topics + speeches + sections)
 * Step 4: Classify topics (AI agent)
 * Step 5: Store sitting + speeches + link MEPs
 *
 * Usage: node scripts/run-pipeline.js [--date YYYY-MM-DD]
 */

require('dotenv').config();
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const { discoverNextSittingDate, fetchSittingHTML } = require('../../core/parliament-fetch');
const { parseSitting } = require('../../scripts/step-3-parse-sitting');
const { run: classifyTopics } = require('../../scripts/step-4-classify-topics');
const { run: storeSitting } = require('../../scripts/step-5-store-sitting');
const { DB_PATH } = require('../../core/db');

async function runPipeline(options = {}) {
  const log = options.log || console.log;
  const quiet = !!options.quiet;
  const stepLog = quiet ? () => {} : log;
  const targetDate = options.date || null;

  const db = new sqlite3.Database(DB_PATH);

  try {
    if (!quiet) log('\nEUROWATCH — Refresh (newest sitting)\n');

    // Step 1: Discover the single newest sitting date not in DB
    let date = targetDate;
    if (!date) {
      stepLog('[1/5] Finding newest sitting not in DB...');
      date = await discoverNextSittingDate(db, stepLog);
      if (!date) {
        log('No new sittings found.');
        return { success: false, message: 'No new sitting dates to process' };
      }
    }
    if (!quiet) log(`→ ${date}`);

    // Step 2: Fetch HTML
    stepLog('[2/5] Fetching HTML...');
    const html = await fetchSittingHTML(date);
    if (!html || html.length < 500) {
      log('Failed to fetch HTML.');
      return { success: false, message: 'HTML fetch failed' };
    }

    // Step 3: Parse sitting
    stepLog('[3/5] Parsing sitting...');
    const sittingId = `sitting-${date}`;
    const { topics, speeches, sections } = parseSitting(html, sittingId, stepLog);

    // Step 4: Classify topics
    stepLog('[4/5] Classifying topics...');
    const topicTitles = topics.map(t => t.title);
    const topicMap = await classifyTopics(topicTitles, { log: stepLog });

    // Step 5: Store
    stepLog('[5/5] Storing sitting and speeches...');
    const storeResult = await storeSitting(
      { date, html, sittingId, topics, speeches, sections, topicMap },
      { log: stepLog, db }
    );

    log(`✓ ${date} — ${speeches.length} speeches, ${topics.length} topics, ${storeResult.linkedCount} MEPs linked`);

    return {
      success: true,
      date,
      speechesCount: speeches.length,
      topicsCount: topics.length,
      linkedCount: storeResult.linkedCount
    };
  } catch (err) {
    log(`\n❌ Pipeline error: ${err.message}`);
    throw err;
  } finally {
    db.close();
  }
}

if (require.main === module) {
  const dateIdx = process.argv.indexOf('--date');
  const date = dateIdx !== -1 && process.argv[dateIdx + 1] ? process.argv[dateIdx + 1] : null;

  runPipeline({ date })
    .then(r => process.exit(r.success ? 0 : 1))
    .catch(() => process.exit(1));
}

module.exports = { runPipeline };
