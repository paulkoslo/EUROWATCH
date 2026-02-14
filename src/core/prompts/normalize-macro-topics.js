/**
 * Prompt for the macro-topic normalization agent.
 * Given a list of distinct macro topics from the DB, the model suggests
 * rules to unify similar/duplicate labels (e.g. "Foreign Policy Cuba" and
 * "foreign policy central america" → one canonical "Foreign policy – Americas").
 */

/**
 * @returns {string} System prompt for normalization
 */
function buildSystemPrompt() {
  return `# Macro Topic Normalization — EU Parliament

You are given a list of macro topics that appear in the EU Parliament speech database. Many are the same or very similar but with different wording, casing, or regional variants (e.g. "Foreign Policy Cuba", "foreign policy central america", "Foreign policy – Latin America" should become one unified topic).

## Your task

Produce **normalization rules**: for each set of equivalent or near-equivalent topics, choose **one canonical label** and list **all variants** that should be mapped to it. Variants must be **exact strings** as they appear in the input list.

## Output format

Return **only** a JSON array. No markdown, no explanation. Each element:

\`\`\`json
{ "canonical": "Chosen label", "variants": ["exact input string 1", "exact input string 2", ...] }
\`\`\`

- \`canonical\`: the single label to use for this group (clear, consistent naming).
- \`variants\`: every topic string that should be rewritten to \`canonical\`. Include the preferred form (same as canonical) in \`variants\` if it appears in the input, plus all others that map to it.

## Good examples (do this)

**Regional merge — same policy area, different regions:** Unify foreign-policy regional variants into one canonical.
\`\`\`json
{ "canonical": "Foreign policy — Americas", "variants": ["Foreign policy — Americas", "Foreign policy — Latin America", "Foreign policy — Cuba", "Foreign policy — Central America", "Foreign policy — Caribbean", "Foreign policy — Brazil"] }
\`\`\`

**Typo / word-order / unicode:** Fix minor wording differences where the meaning is identical.
\`\`\`json
{ "canonical": "Procedural & Parliamentary business", "variants": ["Procedural & Parliamentary business", "Procedure & Parliamentary business"] }
{ "canonical": "Social policy & employment", "variants": ["Social policy & employment", "Employment & social policy"] }
{ "canonical": "Foreign policy — Asia-Pacific", "variants": ["Foreign policy — Asia-Pacific", "Foreign policy — Asia‑Pacific", "Foreign policy — Asia-Pacic"] }
\`\`\`

**Clear synonyms:** Merge when labels are different names for the same policy (e.g. transport modes).
\`\`\`json
{ "canonical": "Aviation policy", "variants": ["Aviation policy", "Air transport policy", "Civil aviation"] }
\`\`\`

## Bad example (do not do this)

**Do not merge distinct policy areas into one generic label.** Topics that are different policy domains (even if they share a word like "consumer protection") must stay separate. Only merge true duplicates or minor wording variants.
- BAD: Merging "Food safety & consumer protection", "Product safety & consumer protection", "Passenger rights & consumer protection", "Fraud prevention & consumer protection" into generic "Consumer protection" — these are distinct policy areas (food safety, product safety, passenger rights, fraud); merging loses important nuance.
- GOOD: Merging "Consumer protection" and "Consumer Protection" (casing only) into "Consumer protection" is fine.

## Rules

- **Only output rules where you are actually merging or renaming.** A rule must have at least one variant that is different from the canonical (so something actually changes). Do **not** output a rule for topics that stay unchanged — omit them entirely; they will be left as-is.
- Never list the same topic in two different rules.
- Use **exact strings** from the input; do not invent or rephrase when listing variants.
- **Canonical names**: Use clear, final labels only. Do not add synthetic suffixes like "(alt)" or "(other)" to canonical names.
- Group only topics that are **semantically the same or clearly the same policy area** (e.g. regional variants like Cuba/Central America/Latin America → "Foreign policy — Americas"; or "Agriculture" vs "Agriculture & fisheries" if they mean the same). Do **not** merge topics that are distinct policy domains (e.g. "Food safety & consumer protection" vs "Fraud prevention & consumer protection" are different — keep them separate).
- Prefer concise, consistent canonical names (e.g. "Foreign policy — Americas", "Agriculture & fisheries").
- Output valid JSON only.`;
}

/**
 * @param {Array<{ topic: string, count?: number }>} topicsWithCounts - Distinct topics (and optional counts)
 * @returns {string} User message with the list to normalize
 */
function buildUserMessage(topicsWithCounts) {
  const lines = topicsWithCounts.map((t, i) => {
    const count = t.count != null ? ` (${t.count} speeches)` : '';
    return `${i + 1}. ${t.topic}${count}`;
  });
  return `Normalize the following macro topics. Return a JSON array of { "canonical", "variants" } rules.\n\n${lines.join('\n')}`;
}

module.exports = {
  buildSystemPrompt,
  buildUserMessage
};
