#!/usr/bin/env node
/**
 * Step 4: Classify agenda topics to macro/micro taxonomy via AI agent
 *
 * Usage: node scripts/step-4-classify-topics.js < topics.json
 *   or:  node scripts/step-4-classify-topics.js --topics "Topic 1" "Topic 2"
 *
 * Input: array of topic title strings
 * Output: { topicMap: { "Topic": { macro_topic, specific_focus, confidence } } }
 */

require('dotenv').config();
const fs = require('fs');
const { classifyTopics } = require('../core/topic-agent');

async function run(topicTitles, options = {}) {
  const log = options.log || (() => {});
  if (!topicTitles || !Array.isArray(topicTitles) || topicTitles.length === 0) {
    return {};
  }

  if (!process.env.OPENAI_API_KEY) {
    log('  Skipping AI classification (no OPENAI_API_KEY)');
    return {};
  }

  log(`  Classifying ${topicTitles.length} topics...`);
  const classified = await classifyTopics(topicTitles, log);

  const topicMap = {};
  for (const c of classified) {
    topicMap[c.topic] = {
      macro_topic: c.macro_topic,
      specific_focus: c.specific_focus,
      confidence: c.confidence
    };
  }
  return topicMap;
}

if (require.main === module) {
  let topicTitles = [];

  const topicsIdx = process.argv.indexOf('--topics');
  if (topicsIdx !== -1) {
    topicTitles = process.argv.slice(topicsIdx + 1).filter(a => !a.startsWith('--'));
  } else {
    try {
      const stdin = fs.readFileSync(0, 'utf8');
      const parsed = JSON.parse(stdin);
      if (Array.isArray(parsed)) {
        topicTitles = parsed;
      } else if (parsed.topics && Array.isArray(parsed.topics)) {
        topicTitles = parsed.topics.map(t => typeof t === 'string' ? t : t.title);
      } else if (parsed.topicsCount !== undefined) {
        topicTitles = (parsed.topics || []).map(t => t.title || t);
      }
    } catch (_) {
      topicTitles = [];
    }
  }

  run(topicTitles, { log: console.log })
    .then(topicMap => {
      console.log(JSON.stringify(topicMap, null, 2));
      process.exit(0);
    })
    .catch(err => {
      console.error('Error:', err.message);
      process.exit(1);
    });
}

module.exports = { run };
