# src/server/ — HTTP app glue

Shared logic for the Express server. **No duplication with `src/core/`:**

- **Parsing:** Uses `core/parse-helpers.parseIndividualSpeeches` (single source of truth). This folder only adds `storeIndividualSpeeches`, `parseRecentSpeeches`, `parseAllSpeechesWithContent`.
- **HTML fetch:** Uses `core/parliament-fetch.fetchSittingHTML` (correct session by term). `fetch-speech-html.js` only does content extraction from that HTML.
- **DB path:** Use `src/core/db` for `DB_PATH`; this folder does not define DB paths.
- **Config:** `config.js` holds server-only constants (PORT, API_BASE); core holds DB paths and analytics DB.

See `docs/PROJECT_STRUCTURE.md` for the full layout.
