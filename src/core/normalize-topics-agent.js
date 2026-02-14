/**
 * Normalize Macro Topics Agent
 * Fetches all distinct macro topics from the DB, asks the LLM for unification rules,
 * then (via normalize-topics-apply) applies those rules to update individual_speeches.
 */

const { call: openaiCall } = require('./openai-call');
const { buildSystemPrompt, buildUserMessage } = require('./prompts/normalize-macro-topics');

/**
 * @param {object} db - SQLite3 database
 * @returns {Promise<Array<{ topic: string, count: number }>>}
 */
function getDistinctTopics(db) {
  return new Promise((resolve, reject) => {
    db.all(
      `SELECT macro_topic AS topic, COUNT(*) AS count
       FROM individual_speeches
       WHERE macro_topic IS NOT NULL AND TRIM(macro_topic) <> ''
       GROUP BY macro_topic
       ORDER BY count DESC, macro_topic`,
      [],
      (err, rows) => {
        if (err) return reject(err);
        resolve(rows || []);
      }
    );
  });
}

/**
 * Parse LLM response into rules: [{ canonical, variants }, ...]
 * @param {string} content - Raw response text
 * @param {string[]} allTopics - All topic strings (to validate coverage)
 * @returns {{ rules: Array<{ canonical: string, variants: string[] }>, error?: string }}
 */
function parseRulesResponse(content, allTopics) {
  let arr = null;
  try {
    const trimmed = content.trim();
    const jsonMatch = trimmed.match(/\[[\s\S]*\]/);
    if (jsonMatch) arr = JSON.parse(jsonMatch[0]);
    else arr = JSON.parse(trimmed);
  } catch (e) {
    return { rules: [], error: `Invalid JSON: ${e.message}` };
  }

  if (!Array.isArray(arr)) {
    return { rules: [], error: 'Response was not a JSON array' };
  }

  const rules = [];
  const seen = new Set();

  for (const item of arr) {
    const canonical = item && (item.canonical ?? item.canonicalName);
    const variants = item && (Array.isArray(item.variants) ? item.variants : Array.isArray(item.variant) ? item.variant : []);
    if (!canonical || variants.length === 0) continue;

    const normalizedVariants = variants.map((v) => String(v).trim()).filter(Boolean);
    if (normalizedVariants.length === 0) continue;

    rules.push({ canonical: String(canonical).trim(), variants: normalizedVariants });
    normalizedVariants.forEach((v) => seen.add(v));
  }

  // Do not add identity rules for missing topics — leave them out so we only have real merges/renames

  // Post-process: drop "(alt)"-style canonicals, drop identity-only rules, deduplicate
  const cleaned = sanitizeRules(rules);
  return { rules: cleaned };
}

/**
 * Remove bad rules (e.g. canonical ending with "(alt)") and ensure each topic appears in only one rule.
 * @param {Array<{ canonical: string, variants: string[] }>} rules
 * @returns {Array<{ canonical: string, variants: string[] }>}
 */
function sanitizeRules(rules) {
  // 1) Drop rules whose canonical looks synthetic (e.g. "Something (alt)")
  const withoutAlt = rules.filter(
    (r) => !/\(\s*alt\s*\)$/i.test(r.canonical) && !/\(\s*other\s*\)$/i.test(r.canonical)
  );

  // 2) Drop identity-only rules (nothing actually changes)
  const withChanges = withoutAlt.filter((r) => {
    const hasChange = r.variants.some((v) => v !== r.canonical);
    return hasChange;
  });

  // 3) Deduplicate: each variant may appear only in the first rule that contains it
  const variantToRuleIndex = new Map();
  const result = [];
  for (let i = 0; i < withChanges.length; i++) {
    const rule = withChanges[i];
    const keptVariants = rule.variants.filter((v) => {
      if (variantToRuleIndex.has(v)) return false; // already in an earlier rule
      variantToRuleIndex.set(v, i);
      return true;
    });
    if (keptVariants.length === 0) continue;
    result.push({ canonical: rule.canonical, variants: keptVariants });
  }

  return result;
}

/**
 * Ask the LLM for normalization rules.
 * @param {Array<{ topic: string, count?: number }>} topicsWithCounts
 * @param {(msg: string) => void} log
 * @returns {Promise<Array<{ canonical: string, variants: string[] }>>}
 */
async function suggestRules(topicsWithCounts, log = () => {}) {
  const topicStrings = topicsWithCounts.map((t) => t.topic);
  if (topicStrings.length === 0) {
    log('[NORMALIZE] No macro topics in database.');
    return [];
  }

  log(`[NORMALIZE] Asking LLM for rules for ${topicStrings.length} distinct macro topics...`);
  const systemPrompt = buildSystemPrompt();
  const userMessage = buildUserMessage(topicsWithCounts);

  const content = await openaiCall(systemPrompt, userMessage);
  const { rules, error } = parseRulesResponse(content, topicStrings);
  if (error) {
    log(`[NORMALIZE] Parse warning: ${error}`);
  }
  log(`[NORMALIZE] Got ${rules.length} normalization rule(s).`);
  return rules;
}

module.exports = {
  getDistinctTopics,
  suggestRules,
  parseRulesResponse
};
