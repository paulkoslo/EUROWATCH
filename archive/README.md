# Archive

Old and superseded scripts, organized by category. The active codebase uses `scripts/` and `core/`.

## Structure

| Folder | Contents |
|--------|----------|
| `pipeline/` | Old pipeline implementations (superseded by `scripts/run-pipeline.js`, `scripts/bulk-pipeline.js`) |
| `parsing/` | Old HTML/reparse scripts (superseded by `core/parse-helpers.js`, step-3) |
| `classification/` | Old topic/session classification (superseded by `core/topic-agent.js`, step-4) |
| `analysis/` | Analysis and thesis scripts |
| `legacy/` | Misc utilities (export, language detection, etc.) |
| `docs/` | Old prompt docs, archive index |

## Replaced By

| Archived | Replaced By |
|----------|-------------|
| `pipeline/run-pipeline-newest.js` | `scripts/run-pipeline.js` (re-exports) |
| `pipeline/unified-pipeline.js` | `scripts/run-pipeline.js` |
| `pipeline/perfect-fetch-parse*.js`, `update.js` | `scripts/` step scripts + `core/parliament-fetch.js` |
| `parsing/reparse-*.js`, `map-topics-for-sitting.js` | `core/parse-helpers.js`, step-3, step-4 |
| `classification/classify-topics.js` etc. | `core/topic-agent.js`, `scripts/step-4-classify-topics.js` |
