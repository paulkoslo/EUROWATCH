/**
 * Topic Agent: Maps HTML-parsed agenda topics to macro/micro taxonomy
 * Macro topics are stored in data/macro-topics.json. The agent can add new ones when none fit.
 * Uses batch API calls (20 topics per request) with parallel workers.
 */

require('dotenv').config();
const path = require('path');
const { call: openaiCall } = require('./openai-call');
const { load: loadMacroTopics, addAllIfNew: addMacroTopics } = require('./macro-topics');
const { buildSystemPrompt: buildPromptFromModule } = require('./prompts/topic-macro-classification');
const BATCH_SIZE = parseInt(process.env.TOPIC_BATCH_SIZE || '20', 10);
const POOL_SIZE = parseInt(process.env.TOPIC_POOL_SIZE || '50', 10);

const PROMPT_PATH = path.join(__dirname, 'prompts', 'topic-macro-classification.js');

function buildSystemPrompt(existingTopics) {
  return buildPromptFromModule(existingTopics);
}

function buildUserMessage(topics) {
  return topics.map((t, i) => `${i}. ${t}`).join('\n');
}

function parseBatchResponse(content, topics, existingTopics, log) {
  let arr = null;
  try {
    arr = JSON.parse(content);
  } catch (_) {
    const match = content.match(/\[[\s\S]*\]/);
    if (match) arr = JSON.parse(match[0]);
  }

  const fallbackTopic = existingTopics[0] || 'Procedural & Parliamentary business';

  if (!Array.isArray(arr)) {
    log('⚠️ Agent: Expected JSON array in response');
    return { results: topics.map(t => ({ topic: t, macro_topic: fallbackTopic, specific_focus: null, confidence: 0.2, reason: 'parse_failed' })), newTopics: [] };
  }

  const newTopics = [];
  const results = topics.map((topic, i) => {
    const raw = arr[i];
    if (!raw || !raw.macro_topic) return { topic, macro_topic: fallbackTopic, specific_focus: null, confidence: 0.3, reason: 'parse_failed' };

    const macroTopic = String(raw.macro_topic).trim();
    const isNew = !!raw.is_new || !existingTopics.some(t => t.toLowerCase() === macroTopic.toLowerCase());
    if (isNew && macroTopic) newTopics.push(macroTopic);

    return {
      topic,
      macro_topic: macroTopic,
      specific_focus: raw.specific_focus || null,
      confidence: raw.confidence ?? 0.8,
      reason: raw.reason || ''
    };
  });

  return { results, newTopics };
}

async function classifyBatch(batchTopics, existingTopics, log = () => {}) {
  const systemPrompt = buildSystemPrompt(existingTopics);
  const userMessage = `Topics:\n${buildUserMessage(batchTopics)}`;

  try {
    const content = await openaiCall(systemPrompt, userMessage);
    const { results, newTopics } = parseBatchResponse(content, batchTopics, existingTopics, log);
    return { results, newTopics };
  } catch (err) {
    log(`❌ Batch error: ${err.message}`);
    const fallback = existingTopics[0] || 'Procedural & Parliamentary business';
    return { results: batchTopics.map(t => ({ topic: t, macro_topic: fallback, specific_focus: null, confidence: 0.2, reason: 'error' })), newTopics: [] };
  }
}

async function classifyTopic(topic, log = () => {}) {
  const existingTopics = loadMacroTopics();
  const { results, newTopics } = await classifyBatch([topic], existingTopics, log);
  if (newTopics.length > 0) await addMacroTopics(newTopics);
  return results[0];
}

async function classifyTopics(topics, log = () => {}) {
  const titles = topics.map(t => typeof t === 'string' ? t : (t.title || t.raw || t));
  if (titles.length === 0) return [];

  const existingCount = loadMacroTopics().length;
  log(`  Classifying ${titles.length} topics (${existingCount} macro topics in storage, batch ${BATCH_SIZE}, up to ${POOL_SIZE} parallel)...`);

  const batches = [];
  for (let offset = 0; offset < titles.length; offset += BATCH_SIZE) {
    batches.push(titles.slice(offset, offset + BATCH_SIZE));
  }

  const existingTopics = loadMacroTopics();
  const allNewTopics = new Set();

  const results = [];
  for (let i = 0; i < batches.length; i += POOL_SIZE) {
    const chunk = batches.slice(i, i + POOL_SIZE);
    const batchNum = Math.floor(i / POOL_SIZE) + 1;
    const totalChunks = Math.ceil(batches.length / POOL_SIZE);
    log(`  Batch group ${batchNum}/${totalChunks}: ${chunk.length} API call(s), ${chunk.reduce((s, b) => s + b.length, 0)} topics...`);

    const batchResults = await Promise.all(chunk.map(b => classifyBatch(b, existingTopics, log)));
    for (const { results: br, newTopics: nt } of batchResults) {
      results.push(...br);
      nt.forEach(t => allNewTopics.add(t));
    }
  }

  if (allNewTopics.size > 0) {
    const added = await addMacroTopics([...allNewTopics]);
    if (added > 0) log(`  + ${added} new macro topic(s) added: ${[...allNewTopics].slice(0, 3).join(', ')}${allNewTopics.size > 3 ? '...' : ''}`);
  }

  log(`  Done: ${results.length} topics classified (${loadMacroTopics().length} macro topics now in storage)`);
  return results;
}

module.exports = {
  BATCH_SIZE,
  POOL_SIZE,
  PROMPT_PATH,
  buildSystemPrompt: (topics) => buildSystemPrompt(topics || loadMacroTopics()),
  classifyTopic,
  classifyBatch,
  classifyTopics
};
