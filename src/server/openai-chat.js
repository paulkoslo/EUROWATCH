/**
 * OpenAI Chat Completion Helper
 * Handles chat completions with conversation history (multiple messages with different roles)
 */

require('dotenv').config();
const OpenAI = require('openai');

const DEFAULT_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';

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
 * Send a chat completion request with full message history
 * @param {Array<{role: string, content: string}>} messages - Array of message objects with role and content
 * @param {object} options - { model?, temperature? }
 * @returns {Promise<{content: string, usage: object}>} Assistant response content and token usage
 */
async function chatCompletion(messages, options = {}) {
  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    throw new Error('Messages array is required and must not be empty');
  }

  const client = getClient();
  const model = options.model || DEFAULT_MODEL;
  const temperature = options.temperature != null ? options.temperature : 0.2;

  const response = await client.chat.completions.create({
    model,
    messages,
    temperature
  });

  return {
    content: response.choices?.[0]?.message?.content?.trim() || '',
    usage: response.usage || {}
  };
}

module.exports = {
  chatCompletion,
  getClient,
  DEFAULT_MODEL
};
