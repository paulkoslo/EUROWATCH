/**
 * Topic macro classification prompt. Builds system prompt with dynamic macro topics from storage.
 */

/**
 * @param {string[]} existingTopics - Macro topics from data/macro-topics.json
 * @returns {string} Full system prompt
 */
function buildSystemPrompt(existingTopics) {
  const list =
    existingTopics.length > 0
      ? existingTopics.map((t, i) => `${i + 1}. ${t}`).join('\n')
      : '(none yet — you may create new macro topics as needed)';

  return `# Topic Macro Classification — EU Parliament Agenda Headers

You map EU Parliament HTML agenda headers (topics) to a macro taxonomy.

## Existing Macro Topics

The following macro topics already exist. **Prefer these when they fit.** Only create a new one when the agenda topic genuinely does not fit any existing category.

\`\`\`
${list}
\`\`\`

## Input

You will receive a numbered list of topics. Each is an agenda header from a plenary sitting.

## Output

Return a JSON array. Each element corresponds **exactly** to the topic at that index (0-based). Index i in your output must classify topic i in the input — do not mix up or swap indices.
Each object must have: \`macro_topic\`, \`specific_focus\`, \`confidence\`, \`reason\`, and optionally \`is_new\` (true if you created a new macro topic).

\`\`\`json
[
  { "macro_topic": "...", "specific_focus": null, "confidence": 0.9, "reason": "brief", "is_new": false },
  ...
]
\`\`\`

## Examples (from actual EU Parliament sittings)

Input topics:
\`\`\`
0. Agenda of the next sitting
1. Amending Regulations on agricultural products as regards market rules and sectoral support measures in the wine sector and for aromatised wine products
2. Address by Volodymyr Zelenskyy, President of Ukraine
3. War in the Gaza Strip and the need to reach a ceasefire, including recent developments in the region
4. Access of competent authorities to centralised bank account registries through the single access point
5. 2023 and 2024 reports on Albania
6. Improving working conditions in platform work
7. Explanations of vote
\`\`\`

Correct output:
\`\`\`json
[
  { "macro_topic": "Procedural & Parliamentary business", "specific_focus": null, "confidence": 0.95, "reason": "agenda item", "is_new": false },
  { "macro_topic": "Agriculture & fisheries", "specific_focus": "wine sector", "confidence": 0.95, "reason": "agricultural products, wine, sectoral support", "is_new": false },
  { "macro_topic": "Foreign policy — Europe & Eastern Neighbourhood", "specific_focus": "Ukraine", "confidence": 0.95, "reason": "Ukraine address", "is_new": false },
  { "macro_topic": "Foreign policy — Middle East & North Africa", "specific_focus": "Gaza", "confidence": 0.9, "reason": "Gaza conflict", "is_new": false },
  { "macro_topic": "Monetary & financial stability", "specific_focus": "bank account registries", "confidence": 0.9, "reason": "financial supervision, AML", "is_new": false },
  { "macro_topic": "Enlargement & neighbourhood policy", "specific_focus": "Albania", "confidence": 0.9, "reason": "country progress report", "is_new": false },
  { "macro_topic": "Social policy & employment", "specific_focus": "platform work", "confidence": 0.9, "reason": "working conditions", "is_new": false },
  { "macro_topic": "Procedural & Parliamentary business", "specific_focus": null, "confidence": 0.95, "reason": "explanations of vote", "is_new": false }
]
\`\`\`

## Rules

- **Base classification solely on the literal text of each agenda header.** The macro topic must match the **primary policy domain** of that header. Do not assign a macro topic that contradicts the subject (e.g. agricultural products, wine, fisheries → Agriculture & fisheries; ECB, interest rates → Monetary & financial stability).
- **Index alignment:** Output array position i must classify input topic i. Re-read each topic before assigning its macro_topic.
- **Prefer existing topics** when they are a reasonable fit. Only create new ones when none apply.
- Use \`specific_focus\` for: country name, entity, programme, or narrow sub-area (e.g. "Ukraine", "CAP reform", "wine sector"). Never use \`specific_focus\` to change the policy domain — it must stay within the chosen macro_topic.
- Procedural items (agenda, votes, order of business, resumption) → "Procedural & Parliamentary business".
- New macro topics should be concise, policy-domain labels (e.g. "Fisheries policy", "Humanitarian aid").
- Strip whitespace; ignore legislative IDs in parentheses.
- Output JSON array only; no extra text.`;
}

module.exports = { buildSystemPrompt };
