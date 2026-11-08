# Reviewed Parliament briefings

The explorer links to `/briefings`. A reviewed edition includes shareable debate and sitting links, source quotations, official voting totals and adopted-text links when the official relationships are available. Edition links remain available as the archive advances, and each edition identifies its own data release.

## Rehearse and review

Use [the enrichment workflow](INSIGHTS_WORKFLOW.md) to build the March 9–12 pilot corpus, generate candidates and score a held-out sample. Review every selected quotation, speaker, fact and claim about the debate. An approval names `key`, the SHA-256 `candidateHash` of the exact candidate JSON, `reviewer`, `reviewedAt` and `approved: true`. Use `allowPartial: true` only after reviewing a briefing from a partial source window.

The CLI can generate a local HTML, RSS and newsletter preview from already reviewed candidates with `node src/insights/publication.js`. It writes private reviews outside the public artifact directory. Use `--corpus`, `--report`, `--approvals`, `--release-id`, `--week` (Monday), `--base-url`, `--output` and `--private-output`.

## Publish a reviewed edition

Run this explicit job with access to the private checkpoint and release storage. Do not include AI credentials in the web service.

```sh
node src/insights/release.js --corpus corpus.json --report pilot.json --approvals approvals.json --week 2026-03-09 --base-url https://YOUR-EUROWATCH-HOST
```

The job acquires the same writer lease as the transcript importer. It recalculates request keys against the current checkpoint and rejects stale or uncertain results. It keeps human reviews private. Reviewed speech-level labels update the analytics and CSV, and reviewed debate/sitting briefings travel with the verified download release. Exact publication replay preserves the release ID. Reports, approvals and the canonical checkpoint remain private. The public archive holds published editions only.

## Read and distribute

- `/briefings` and `/briefings/{publicationId}/{slug}` provide human-readable editions and source links.
- `/api/insights?q=research&limit=20` searches the latest reviewed edition.
- `/feed.xml` and `/newsletter.txt` provide feed and email-ready content.
- `/mcp` provides read-only Streamable HTTP tools for searching and retrieving briefings. Set `PUBLIC_SITE_URL` to the deployed site origin, especially when using a custom domain. Clients can call `search_briefings` and `get_briefing`. [MCP transport specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)

An optional private watchlist pilot is available at `/pilot/watchlists`. Set `WATCHLIST_PILOT_DB` to a persistent disk path and `WATCHLIST_PILOT_TOKEN_FILE` to a private JSON list of `{id,sha256}` entries containing tenant IDs and SHA-256 digests of random bearer tokens. Only configured pilot members can create and preview watchlists. The preview ledger is tenant-scoped and idempotent per publication. Newsletter delivery, customer billing and paid service launch require their own operational setup.
