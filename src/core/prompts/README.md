# Topic Classification API Call

## What we send to the API

### Request structure

```json
{
  "model": "gpt-4o-mini",
  "messages": [
    {
      "role": "system",
      "content": "<output of core/prompts/topic-macro-classification.js buildSystemPrompt(existingTopics)>"
    },
    {
      "role": "user",
      "content": "Topics:\n0. Situation in Ukraine\n1. European Council and Commission statements\n2. ..."
    }
  ]
}
```

### System message

The output of `core/prompts/topic-macro-classification.js` → `buildSystemPrompt(existingTopics)` — instructions, existing macro topics (from `data/macro-topics.json`), examples, and output format.

### User message

A numbered list of topics (up to 20 per call):

```
Topics:
0. Situation in Ukraine
1. European Council and Commission statements
2. CAP reform – transitional provisions
...
```

### Expected response

A JSON array, one object per topic (same order):

```json
[
  { "macro_topic": "Foreign policy — Europe & Eastern Neighbourhood", "specific_focus": "Ukraine", "confidence": 0.95, "reason": "..." },
  { "macro_topic": "Institutional affairs & governance", "specific_focus": null, "confidence": 0.9, "reason": "..." },
  ...
]
```

## Macro topics storage

Macro topics are **not fixed**. They are stored in `data/macro-topics.json`. The agent:
- Receives the current list and prefers existing topics when they fit
- Can create new macro topics when none fit
- New topics are appended to `data/macro-topics.json` for future runs

## Batch size

- **BATCH_SIZE** (default 20): topics per API call
- **POOL_SIZE** (default 50): how many batch calls run in parallel

Example: 100 topics → 5 batches of 20 → 5 API calls (or up to 5 in parallel if POOL_SIZE ≥ 5).
