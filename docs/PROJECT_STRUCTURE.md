# Project structure

All application code lives under **`src/`** so the repo has one clear place for code and the root stays for config, data, and entry points.

## Why the split?

| Folder | Role | Used by |
|--------|------|--------|
| **`src/core/`** | Foundational, reusable logic: DB path, parliament API fetch, parsing, analytics DB, language detection, topic/macro agents. No HTTP or CLI. | server, pipeline, scripts |
| **`src/server/`** | HTTP app glue: config, progress bars, speech fetch/parse wrappers, MEPs API, historic MEPs, init-db, analytics cache, CLI. Uses `core`; no duplication of parsing or fetch logic. | `server.js` only |
| **`src/pipeline/`** | CLI pipeline: refresh (newest sitting) and bulk (date range). Uses `core` + `scripts`. | `node src/pipeline` / npm scripts |
| **`src/scripts/`** | Standalone step scripts (discover date, fetch HTML, parse sitting, classify, store). Used by pipeline and sometimes run by hand. | pipeline, manual |

- **core** = shared primitives; **server** = app-specific glue for the Express app; **pipeline** = data-ingestion CLI; **scripts** = pipeline steps and one-off tools.
- `server.js` stays at repo root as the main entry; it only `require()`s from `src/core` and `src/server`, and starts the server.

## Root layout

- `server.js` — HTTP server entry point
- `package.json` — scripts point at `src/pipeline`, `src/scripts`
- `public/` — static frontend
- `data/` — SQLite DBs, macro-topics, etc.
- `docs/` — this file and other docs
- `archive/` — legacy / unused code
