#!/usr/bin/env node

/*
  Macro Topic Classification for HTML Topics

  Classifies DISTINCT values of `individual_speeches.topic` into a controlled
  set of Main Topics, with an optional specific_focus, and propagates the
  result to all speeches that share that original topic.

  Usage:
    node classify-topics.js                 # classify all distinct topics
    node classify-topics.js 500             # classify first 500 distinct topics
    node classify-topics.js --dry-run       # no DB writes, print summary only
*/

require('dotenv').config();

const sqlite3 = require('sqlite3').verbose();
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');
const cliProgress = require('cli-progress');

const DB_FILE = path.join(__dirname, 'ep_data.db');
const MODEL = 'gpt-5-nano-2025-08-07';

// Rate limits & batching (mirror style from existing scripts)
const MAX_RPM = 5000; // requests per minute
const DEFAULT_BATCH_SIZE = 50; // faster default; override with --concurrency
const REQUEST_DELAY = (60 / MAX_RPM) * 1000; // ms between requests

// Pricing (keep consistent with other scripts)
const INPUT_COST_PER_1M = 0.05;
const OUTPUT_COST_PER_1M = 0.40;

// Controlled vocabulary embedded to keep deterministic behavior client-side
const CONTROLLED_VOCAB = [
  'Procedural & Parliamentary business',
  'Institutional affairs & governance',
  'EU budget & MFF',
  'Economy & industrial policy',
  'Single market, competition & consumer protection',
  'Trade & globalization',
  'Taxation & anti–money laundering',
  'Monetary & financial stability',
  'Digital policy & data protection',
  'Media, information & disinformation',
  'Energy & energy security',
  'Climate, environment & biodiversity',
  'Agriculture & fisheries',
  'Transport & mobility',
  'Health',
  'Research, innovation & space',
  'Education, culture & sport',
  'Social policy & employment',
  'Rule of law & fundamental rights',
  'Justice, security & policing',
  'Migration & asylum',
  'Security & defence',
  'Enlargement & neighbourhood policy',
  'Development & humanitarian aid',
  'Foreign policy — Europe & Eastern Neighbourhood',
  'Foreign policy — Middle East & North Africa',
  'Foreign policy — Sub‑Saharan Africa',
  'Foreign policy — Americas',
  'Foreign policy — Asia‑Pacific'
];

function buildSystemPrompt() {
  // Keep prompt concise but deterministic; aligns with prior message guidance
  return [
    'You map an EU Parliament HTML agenda header (topic) to exactly ONE Main topic from a fixed list.',
    'Return strict JSON: {"topic_input":"...","main_topic":"<one of list>","specific_focus":"<short or null>","confidence":<0..1>,"rationale_short":"<<=15 words>"}.',
    'Rules: prefer substantive policy; procedural items → Procedural & Parliamentary business; do not invent labels; use specific_focus for country/entity/program.',
    'Controlled vocabulary:',
    CONTROLLED_VOCAB.map(v => `- ${v}`).join('\n'),
    'Normalization: strip whitespace; ignore legislative IDs/citations in parentheses.',
    'Deterministic output. No extra text.'
  ].join('\n');
}

class TopicMacroClassifier {
  constructor(limit = null, dryRun = false, dateFilter = null, batchSize = DEFAULT_BATCH_SIZE) {
    this.db = new sqlite3.Database(DB_FILE);
    // Improve concurrent write reliability
    try {
      this.db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=10000;');
      if (typeof this.db.configure === 'function') {
        this.db.configure('busyTimeout', 10000);
      }
    } catch (_) {
      // ignore pragma/config errors
    }
    this.openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    this.limit = limit;
    this.dryRun = dryRun;
    this.dateFilter = dateFilter; // ISO date YYYY-MM-DD for filtering distinct topics
    this.batchSize = Math.max(1, Number(batchSize) || DEFAULT_BATCH_SIZE);
    this.progressBar = null;

    this.systemPrompt = buildSystemPrompt();

    // RL & accounting
    this.requestCount = 0;
    this.lastMinute = Math.floor(Date.now() / 60000);
    this.totalInputTokens = 0;
    this.totalOutputTokens = 0;
    this.totalCost = 0;
    this.totalRequests = 0;

    this.results = [];
  }

  async rateLimit() {
    const currentMinute = Math.floor(Date.now() / 60000);
    if (currentMinute !== this.lastMinute) {
      this.requestCount = 0;
      this.lastMinute = currentMinute;
    }
    if (this.requestCount >= MAX_RPM * 0.9) {
      const waitTime = 60000 - (Date.now() % 60000);
      await new Promise(r => setTimeout(r, waitTime));
      this.requestCount = 0;
    }
    await new Promise(r => setTimeout(r, REQUEST_DELAY));
  }

  async ensureSchema() {
    // Add macro classification columns if not present
    await new Promise(resolve => {
      this.db.exec(`
        ALTER TABLE individual_speeches ADD COLUMN macro_topic TEXT;
        ALTER TABLE individual_speeches ADD COLUMN macro_specific_focus TEXT;
        ALTER TABLE individual_speeches ADD COLUMN macro_confidence REAL;
        ALTER TABLE individual_speeches ADD COLUMN macro_classified_by TEXT;
        ALTER TABLE individual_speeches ADD COLUMN macro_classified_at INTEGER;
        ALTER TABLE individual_speeches ADD COLUMN macro_classification_cost REAL;
      `, () => resolve());
    });
  }

  async getDistinctTopics() {
    return new Promise((resolve, reject) => {
      let q;
      if (this.dateFilter) {
        q = `
          SELECT TRIM(i.topic) AS topic, COUNT(*) AS cnt
          FROM individual_speeches i
          JOIN sittings s ON s.id = i.sitting_id
          WHERE i.topic IS NOT NULL AND TRIM(i.topic) <> ''
            AND s.activity_date = ?
          GROUP BY TRIM(i.topic)
          ORDER BY cnt DESC
        `;
      } else {
        q = `
          SELECT TRIM(topic) AS topic, COUNT(*) AS cnt
          FROM individual_speeches
          WHERE topic IS NOT NULL AND TRIM(topic) <> ''
          GROUP BY TRIM(topic)
          ORDER BY cnt DESC
        `;
      }
      if (this.limit) {
        q += ` LIMIT ${this.limit}`;
      }
      const params = this.dateFilter ? [this.dateFilter] : [];
      this.db.all(q, params, (err, rows) => {
        if (err) return reject(err);
        resolve(rows.map(r => r.topic));
      });
    });
  }

  formatInput(topic) {
    return `Topic: ${topic}`;
  }

  async classifyOne(topic, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        await this.rateLimit();
        const response = await this.openai.chat.completions.create({
          model: MODEL,
          messages: [
            { role: 'system', content: this.systemPrompt },
            { role: 'user', content: this.formatInput(topic) }
          ]
        });

        this.requestCount++;
        this.totalRequests++;

        const inputTokens = response.usage?.prompt_tokens || 0;
        const outputTokens = response.usage?.completion_tokens || 0;
        this.totalInputTokens += inputTokens;
        this.totalOutputTokens += outputTokens;
        const cost = (inputTokens / 1_000_000) * INPUT_COST_PER_1M + (outputTokens / 1_000_000) * OUTPUT_COST_PER_1M;
        this.totalCost += cost;

        let content = response.choices?.[0]?.message?.content?.trim() || '';
        // Try to parse JSON; if it contains extra text, extract JSON block
        let parsed = null;
        try {
          parsed = JSON.parse(content);
        } catch (_) {
          const match = content.match(/\{[\s\S]*\}/);
          if (match) {
            parsed = JSON.parse(match[0]);
          }
        }
        if (!parsed || !parsed.main_topic) {
          throw new Error('Invalid JSON or missing main_topic');
        }

        return { topic, ...parsed, cost };
      } catch (err) {
        if (attempt === retries) {
          return { topic, error: err.message };
        }
        await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 1000));
      }
    }
  }

  updateProgress(current, total, currentTopic) {
    if (!this.progressBar) {
      this.progressBar = new cliProgress.SingleBar({
        format: '🚀 Macro Topic Classification |{bar}| {percentage}% | {value}/{total} | Current: {currentTopic} | Cost: ${cost}',
        barCompleteChar: '\u2588',
        barIncompleteChar: '\u2591',
        hideCursor: true
      });
      this.progressBar.start(total, 0, { currentTopic: 'Starting...', cost: '0.00' });
    }
    this.progressBar.update(current, { currentTopic: currentTopic?.slice(0, 50) || 'Processing...', cost: this.totalCost.toFixed(4) });
  }

  async applyResultToDatabase(result) {
    if (this.dryRun) return;
    const maxRetries = 6;
    const attemptRun = (attempt) => new Promise((resolve, reject) => {
      const stmt = this.db.prepare(`
        UPDATE individual_speeches
        SET macro_topic = ?,
            macro_specific_focus = ?,
            macro_confidence = ?,
            macro_classified_by = 'gpt-5-nano-2025-08-07',
            macro_classified_at = strftime('%s', 'now'),
            macro_classification_cost = COALESCE(macro_classification_cost, 0) + ?
        WHERE TRIM(topic) = TRIM(?)
      `);
      stmt.run([
        result.main_topic,
        result.specific_focus || null,
        typeof result.confidence === 'number' ? result.confidence : null,
        result.cost || 0,
        result.topic
      ], (err) => {
        if (err && (err.code === 'SQLITE_BUSY' || /database is locked/i.test(err.message || ''))) {
          stmt.finalize(() => {
            if (attempt < maxRetries) {
              const delay = Math.min(1600 * Math.pow(2, attempt), 10000);
              setTimeout(() => attemptRun(attempt + 1).then(resolve).catch(reject), delay);
            } else {
              reject(err);
            }
          });
          return;
        }
        if (err) {
          stmt.finalize(() => reject(err));
          return;
        }
        stmt.finalize(() => resolve());
      });
    });
    return attemptRun(0);
  }

  async run() {
    if (!process.env.OPENAI_API_KEY) {
      console.error('❌ OPENAI_API_KEY not set');
      process.exit(1);
    }

    console.log('🚀 EUROWATCH Macro Topic Classification (by HTML topics)');
    console.log(`⚙️  Model: ${MODEL} | Batch: ${this.batchSize} | Dry-run: ${this.dryRun ? 'yes' : 'no'}${this.dateFilter ? ` | Date: ${this.dateFilter}` : ''}`);

    await this.ensureSchema();
    const topics = await this.getDistinctTopics();
    if (topics.length === 0) {
      console.log('❌ No distinct topics found.');
      this.db.close();
      return;
    }
    console.log(`📥 Distinct topics to classify: ${topics.length}`);

    for (let i = 0; i < topics.length; i += this.batchSize) {
      const batch = topics.slice(i, i + this.batchSize);
      const promises = batch.map(t => this.classifyOne(t));
      const results = await Promise.all(promises);

      for (let j = 0; j < results.length; j++) {
        const r = results[j];
        this.updateProgress(Math.min(i + j + 1, topics.length), topics.length, r.topic);
        if (!r.error) {
          await this.applyResultToDatabase(r);
        }
        this.results.push(r);
      }
    }

    this.progressBar?.stop();

    // Summary
    const ok = this.results.filter(r => !r.error);
    const fail = this.results.filter(r => r.error);
    console.log('\n' + '='.repeat(80));
    console.log('📊 MACRO TOPIC CLASSIFICATION SUMMARY');
    console.log('='.repeat(80));
    console.log(`✅ Successful: ${ok.length}`);
    console.log(`❌ Failed: ${fail.length}`);
    if (fail.length) {
      console.log('Some errors (first 10):');
      fail.slice(0, 10).forEach(f => console.log(` - ${f.topic}: ${f.error}`));
    }
    console.log(`💰 Total cost: $${this.totalCost.toFixed(4)}`);
    console.log(`🔢 Tokens in/out: ${this.totalInputTokens.toLocaleString()} / ${this.totalOutputTokens.toLocaleString()}`);

    this.db.close();
  }
}

// CLI
const args = process.argv.slice(2);
const limitFlagIndex = args.findIndex(a => a === '--limit');
const limit = limitFlagIndex >= 0 && args[limitFlagIndex + 1] ? parseInt(args[limitFlagIndex + 1], 10) : null;
const dryRun = args.includes('--dry-run');
const dateFlagIndex = args.findIndex(a => a === '--date');
const dateFilter = dateFlagIndex >= 0 && args[dateFlagIndex + 1] ? args[dateFlagIndex + 1] : null;
const concurrencyFlagIndex = args.findIndex(a => a === '--concurrency');
const concurrency = concurrencyFlagIndex >= 0 && args[concurrencyFlagIndex + 1] ? parseInt(args[concurrencyFlagIndex + 1], 10) : DEFAULT_BATCH_SIZE;

const runner = new TopicMacroClassifier(limit, dryRun, dateFilter, concurrency);
runner.run().catch(err => {
  console.error('❌ Execution failed:', err);
  process.exit(1);
});


