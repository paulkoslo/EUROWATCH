# Project structure

The browsing service and offline collection tools are separate.

| Location | Role |
|---|---|
| `server.js` | Loads a prepared dataset, starts HTTP, handles shutdown. No initialization, scraping or AI. |
| `src/explorer/app.js` | Read-only HTTP routes, security headers, bounded cache and CSV streaming. |
| `src/explorer/params.js` | Shared input validation and bounded selections. |
| `src/explorer/queries.js` | Sitting/speaker queries, chronology, topic aggregation and coverage. |
| `src/data/sqlite.js` | Promise wrappers and SQLite backup. |
| `src/data/schema.js` | Explicit offline schema creation/migration and prepared-schema validation. |
| `src/data/dataset.js` | Copy, validate, derive analytics, hash, load and promote immutable releases. |
| `src/scripts/dataset.js` | CLI for `data:init`, `data:prepare`, `data:validate`, `data:promote`. |
| `public/index.html`, `public/app.js`, `public/style.css` | Browser UI with URL state, text-safe rendering, local SVG chart and table. |
| `test/` | Deterministic fixtures and Node regression tests. |
| `.github/workflows/ci.yml` | Install, lint, formatting, tests and production dependency audit; no deployment. |
| `src/core/`, `src/pipeline/` | Legacy offline research/ingestion code. Not imported by the public server. |
| `src/server/` | Legacy server/maintenance helpers still used by some offline tools. Not the current HTTP app. |
| `archive/` | Older research scripts, retained for reference. |

`data/releases/NAME/` contains `ep_data.db`, a matched `analytics.db`, and `manifest.json`. It is ignored by Git. `data/current.json` selects a release; promotion changes this pointer atomically, and a restart switches both database files together. `DATASET_DIR` can select a directory directly.

The original `data/ep_data.db`, legacy analytics and original demo are not public runtime inputs. Preparation reads the source using SQLite’s backup API, then migrates and indexes only the copy. It creates a speaker browsing projection that preserves inferred records and handles valid numeric unresolved IDs; blank IDs remain visible through their original sitting contributions.

The new analytics database contains normalized monthly topic counts, monthly corpus coverage, topic totals and matching release metadata. Month/quarter/year views aggregate these small tables using one filter contract. Neither a cache miss nor opening a chart triggers full-corpus rebuilding.

Public code has no dependency on Parliament network access or OpenAI. Keep future collection changes within the offline pipeline and promote a validated snapshot explicitly.
