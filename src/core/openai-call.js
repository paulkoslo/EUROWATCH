/**
 * Reusable OpenAI API call. Sends exactly one request: one system prompt + one user message.
 * Use this for any OpenAI chat completion where you want a single prompt/response.
 */

require('dotenv').config();
const OpenAI = require('openai');

const DEFAULT_MODEL = process.env.OPENAI_MODEL || 'gpt-5-mini';

let _client = null;
function getClient() {
  if (!_client) {
    const key = process.env.OPENAI_API_KEY;
    if (!key) throw new Error('OPENAI_API_KEY not set in .env');
    _client = new OpenAI({ apiKey: key });
  }
  return _client;
}

/**
 * Send one API call: system prompt + user message.
 * @param {string} systemPrompt - System message (instructions, context)
 * @param {string} userMessage - User message (the actual input)
 * @param {object} options - { model?, temperature? }
 * @returns {Promise<string>} Assistant response content
 */
async function call(systemPrompt, userMessage, options = {}) {
  const client = getClient();
  const model = options.model || DEFAULT_MODEL;

  const response = await client.chat.completions.create({
    model,
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userMessage }
    ],
    ...(options.temperature != null && { temperature: options.temperature })
  });

  return response.choices?.[0]?.message?.content?.trim() || '';
}

module.exports = {
  call,
  getClient,
  DEFAULT_MODEL
};
