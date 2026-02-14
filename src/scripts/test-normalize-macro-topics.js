#!/usr/bin/env node
/**
 * Test script for macro topic normalization (dry-run, no DB writes).
 * Fetches distinct topics from the DB, asks the agent for rules, and prints
 * exactly which topics would be combined. Does NOT save rules or update the DB.
 *
 * Usage: node src/scripts/test-normalize-macro-topics.js
 * Requires: OPENAI_API_KEY in .env, and data/ep_data.db with macro_topic data.
 */

require('dotenv').config();
const sqlite3 = require('sqlite3').verbose();
const { DB_PATH } = require('../core/db');
const { getDistinctTopics, suggestRules } = require('../core/normalize-topics-agent');

function log(msg) {
  console.log(msg);
}

function main() {
  if (!process.env.OPENAI_API_KEY) {
    console.error('OPENAI_API_KEY is not set. Add it to .env to run the agent.');
    process.exit(1);
  }

  const db = new sqlite3.Database(DB_PATH, sqlite3.OPEN_READONLY, (err) => {
    if (err) {
      console.error('Could not open database (read-only):', err.message);
      process.exit(1);
    }
  });

  (async () => {
    try {
      log('--- Normalize Macro Topics (DRY RUN – no DB writes) ---\n');
      log('Fetching distinct macro topics from DB...');
      const topicsWithCounts = await getDistinctTopics(db);
      db.close();

      if (topicsWithCounts.length === 0) {
        log('No macro topics found in the database.');
        return;
      }

      const countByTopic = new Map(topicsWithCounts.map((t) => [t.topic, t.count]));
      log(`Found ${topicsWithCounts.length} distinct topics (${topicsWithCounts.reduce((s, t) => s + t.count, 0)} speeches total).\n`);
      log('Asking agent for normalization rules...\n');

      const rules = await suggestRules(topicsWithCounts, log);

      log('\n========== RULES (what would be applied) ==========\n');

      rules.forEach((rule, i) => {
        log(`--- Rule ${i + 1} [${rule.variants.length} → 1] ---`);
        log(`Canonical: "${rule.canonical}"`);
        rule.variants.forEach((v) => {
          const count = countByTopic.get(v) ?? 0;
          const same = v === rule.canonical ? ' (same)' : '';
          log(`  → "${v}" (${count} speeches)${same}`);
        });
        log('');
      });

      const speechesThatWouldChange = rules.reduce((sum, r) => {
        return sum + r.variants.filter((v) => v !== r.canonical).reduce((s, v) => s + (countByTopic.get(v) || 0), 0);
      }, 0);

      log('========== SUMMARY ==========');
      log(`Rules (merges only): ${rules.length}`);
      log(`Speeches that would be updated: ${speechesThatWouldChange}`);
      log('\nNo changes were written to the database. Run "Normalize Macro Topics" from the Data menu to apply.');
    } catch (err) {
      console.error('Error:', err.message);
      process.exit(1);
    }
  })();
}

main();
