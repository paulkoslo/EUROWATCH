# Archived Scripts

These scripts have been superseded by the new pipeline structure in `scripts/`:

| Archived Script | Replaced By |
|-----------------|-------------|
| `perfect-fetch-parse.js` | `scripts/run-pipeline.js` + step scripts |
| `perfect-fetch-parse-yearly.js` | `scripts/step-1-discover-date.js` + `step-2-fetch-html.js` (or run pipeline in a loop) |
| `update.js` | `scripts/run-pipeline.js` |
| `reparse-with-parentheses.js` | `core/parse-helpers.js` → `parseIndividualSpeeches` |
| `reparse-all-with-parentheses.js` | `core/parse-helpers.js` |
| `map-topics-for-sitting.js` | `core/parse-helpers.js` + `scripts/step-3-parse-sitting.js` + `step-4-classify-topics.js` |
| `scripts/unified-pipeline.js` | `scripts/run-pipeline.js` |

The new pipeline uses HTML scraping only (no EU API) and is split into discrete steps.
