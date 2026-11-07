# EUROWATCH

An independent explorer for European Parliament plenary speeches. Browse sittings, read contributions, find speakers and inspect topic trends against a prepared, read-only dataset.

The stabilization branch keeps the existing Node/Express/SQLite stack. It separates public browsing from offline maintenance, fixes filter and count consistency, and adds a small usable interface. It does not refresh or reinterpret the original research corpus automatically.

## Run locally

Use a supported Node.js LTS release (the checked-in `.nvmrc` selects Node 22).

```sh
nvm use
npm ci
npm run data:fixture
DATASET_DIR=data/fixture npm start
```

Open [localhost:3000](http://localhost:3000). The fixture is deliberately tiny and includes missing metadata, unresolved identities and text containing HTML. No API keys, upstream requests or large downloads are needed. `data:fixture` refuses to overwrite an existing fixture; select a new output with the preparation command below if needed.

For the full local corpus, first keep a verified backup outside the repository, then prepare a **new** release directory:

```sh
npm run data:prepare -- --source data/ep_data.db --output data/releases/baseline
npm run data:validate -- --dataset data/releases/baseline
npm run data:promote -- --dataset data/releases/baseline
npm start
```

Preparation uses SQLite’s backup API and changes only the copy. It computes matching topic analytics, data-quality counts and file hashes. Depending on disk speed, it may take several minutes and needs space for another full copy. Release directories cannot be overwritten. The original `data/ep_data.db` and legacy `analytics.db` are not changed.

Promotion atomically updates an ignored `data/current.json` pointer. Restart the server to select that release. Set `DATASET_DIR` to use a prepared directory directly. The public server deliberately ignores legacy `DB_PATH`, `ANALYTICS_DB_PATH`, `LOCALRUN` and automatic-ingestion settings; these must not point public browsing at a writable research database.

To prepare a smaller consistent corpus:

```sh
npm run data:prepare -- --source data/ep_data.db --output data/releases/since-2020 --from 2020-01-01
```

Do not use the old `data/demo-data` files as a release: the audited copy contains speeches whose sittings are absent. Preparation rejects sitting orphans in an unfiltered source. Subsets retain speeches only for selected sittings, and regenerate their analytics.

## What works

- **Sittings:** date filters, stable pagination, recorded contributions and links to the original Parliament record.
- **Speakers:** name search, record-source filters, chronological contributions and explicit inferred/unresolved identities.
- **Topics:** up to eight topics, month ranges and monthly/quarterly/yearly counts; a chart, accessible table, coverage information and matching CSV.
- **Data & About:** snapshot cutoff, processing limitations, provenance notes, thesis link and date/field-based speech CSV export.
- **Public service:** read-only databases, bounded requests/caches/exports, no maintenance or paid AI routes, predictable startup and health endpoints.

The local full corpus audited on 5 November 2026 contained 368,934 speeches and 1,512 sitting records, dated 20 July 1999–12 February 2026. These are corpus counts, not a guarantee of exhaustive coverage. The UI reads its figures from the served release.

Speaker records are not all verified MEPs. Some identities and many country/group fields are missing or inferred. Topics derive from agenda headings; languages are detected automatically. Data notes in the app explain these limitations. Blank chart periods mean no stored speeches, and aggregate periods report how many months have observations.

## Configuration

Copy `.env.example` if desired. Defaults are sufficient after promotion.

| Variable | Purpose |
|---|---|
| `DATASET_DIR` | Prepared directory containing `ep_data.db`, `analytics.db`, `manifest.json`. Defaults to the promoted release. |
| `HOST` | Defaults to `127.0.0.1`; defaults to `0.0.0.0` when `NODE_ENV=production`. |
| `PORT` | HTTP port; default 3000. |
| `TRUST_PROXY=1` | Trust exactly one reverse proxy. Enable only behind a controlled proxy such as the hosting service. |
| `ENABLE_FULL_DATASET_DOWNLOAD=0` | Optional host override to disable the complete `.db` download. Full downloads are enabled by default. |

No OpenAI key is used by the public application. Browser assets are served locally, without CDN scripts or remote fonts.

## API

Every `/api` response identifies the served dataset with `X-Dataset-ID`. API mutations return 405, including legacy maintenance routes.

| Route | Supported parameters |
|---|---|
| `GET /api/data-status` | Snapshot identity, counts, cutoff and analytics availability. |
| `GET /api/sittings` | `startDate`, `endDate` (`YYYY-MM-DD`), `limit`, `offset`. |
| `GET /api/sittings/:id` | Sitting metadata and source URL. |
| `GET /api/sittings/:id/speeches` | `limit`, `offset`. |
| `GET /api/speakers` | `q`, `source` (`api`, `historic`, `unresolved`), `country`, `limit`, `offset`. |
| `GET /api/speakers/:id` | Speaker metadata. |
| `GET /api/speakers/:id/speeches` | `startDate`, `endDate`, `limit`, `offset`. |
| `GET /api/topics` | Available normalized topic labels and corpus counts. |
| `GET /api/analytics/time-series` | `topics` (JSON string array), `from`, `to` (`YYYY-MM`), `interval` (`month`, `quarter`, `year`), `top` (default 3). |
| `GET /api/export/topics` | Exactly the same selection as time series; CSV includes corpus/period coverage. |
| `GET /api/export/speeches` | `startDate`, `endDate`, comma-separated `fields`, `countOnly=true`. |
| `GET /api/full-dataset/status` | Whether the complete snapshot download is enabled. |
| `GET /api/download/full-dataset` | Complete prepared SQLite download, enabled by default. |
| `GET /healthz`, `GET /readyz` | Process liveness, dataset readiness and analytics availability. |

List limits are 1–100; topic comparisons are limited to eight series. Speech CSV export is limited to 10,000 selected rows and two concurrent streams. Choose a narrower date range for larger selections. Downloads escape CSV quoting and prefix spreadsheet formula-like text with an apostrophe. The speech export follows its own date/field selection, not other views’ filters.

`/api/speeches?personId=...` remains a compatibility alias for **sittings containing that speaker’s contributions**. Old sitting/speaker HTML bookmarks redirect into the new explorer. Other unsupported legacy endpoints return 404; there is no public search/chat/refresh API.

## Checks and development

```sh
npm run check
npm audit --omit=dev --audit-level=high
```

Tests create disposable databases and HTTP servers. They cover backup/source preservation, subset integrity, missing profiles, read-only serving, mutation denial, pagination, date validation, cached filter parity, gaps, exports and mismatched analytics. They require no API keys or external services. CI runs these checks on development branches and pull requests; it does not deploy anything.

Public runtime dependencies are Express, sqlite3 and dotenv. Legacy collection/classification dependencies are development-only and never imported by the server. The CLD3 tooling dependency still carries an upstream Nano ID advisory: the loader uses a constant positive length, and this offline path is absent from the public runtime. Retire/update that legacy loader before making it network-accessible; do not use a forced downgrade that changes the detector silently.

See [project structure](docs/PROJECT_STRUCTURE.md), the [initial audit](docs/AUDIT_2026-11-05.md), [accepted stabilization scope](docs/STABILIZATION_PLAN.md) and [release notes and verification](docs/STABILIZATION_RELEASE.md).

## Offline maintenance

Use Node from `.nvmrc` and install the maintenance dependencies with `npm ci`. Preview an import without opening a database. March 8 was a Sunday and March 9 was a plenary sitting.

```sh
npm run data:import -- --dry-run --start 2026-03-08 --end 2026-03-09
```

To extend the historical corpus, create a staging copy of the original research database. Use a fresh staging filename for the backup, and reuse that staging file for subsequent imports.

```sh
sqlite3 -readonly data/ep_data.db '.backup data/staging.db'
npm run data:import -- --db data/staging.db --start 2026-02-13
npm run data:prepare -- --source data/staging.db --output data/releases/plenary-preview
npm run data:validate -- --dataset data/releases/plenary-preview
DATASET_DIR=data/releases/plenary-preview PORT=4318 npm start
```

For a small standalone dataset, use `npm run data:init -- --output data/staging.db` instead of the backup and pass a short date range to the importer. The `--end` date defaults to yesterday in Brussels. The [Parliament meetings API](https://data.europarl.europa.eu/en/developer-corner/opendata-api) provides the sitting dates and official transcript IDs. The importer tries the transcript HTML first, then Parliament’s multilingual DOCX if needed. It stores DOCX transcript text as HTML with a source marker. The per-sitting log identifies which source was used and the final summary counts imported, existing, pending and failed sittings.

Rerun with the same start date after a failure or publication delay. Each sitting commits independently and the command exits nonzero if any are pending or failed. A weekly job can use the same fixed start date so it also picks up late transcripts. Keep import, preparation and publication as separate steps. This repository does not configure a Render schedule.

The importer records agenda topics, detects speech language locally and links unambiguous names already present in the MEP table. AI macro topics remain empty on new rows until separately classified, so the speeches appear in the explorer before they contribute to macro-topic charts. Legacy collection and classification tools remain under `src/pipeline`, `src/core` and `src/scripts`.

## Deployment and rollback

The current work belongs on `codex/stabilize-eurowatch`. **Do not switch the existing Render service or merge into `main` until its prepared dataset has been provisioned.** An old writable database by itself is insufficient for the new server.

See [Render deployment notes](docs/RENDER_DEMO.md) for the proposed setup. Before selecting this code on a host, upload a validated release directory to persistent storage, configure `DATASET_DIR`, Node LTS, `NODE_ENV=production`, `HOST=0.0.0.0` and the host’s port. Build with `npm ci --omit=dev`; start with `npm start`. Test readiness and the three browsing flows on the intended host.

For a data rollback, promote a previous verified release and restart. For a code rollback, select the previous code commit **with its compatible dataset/configuration**. Retain the original code/data backups; never overwrite a release in place.

## Background and license

EUROWATCH grew out of Paul Gerrit Koslowsky’s master’s research. Read [What they say vs how they say it: rhetorical polarization in the European Parliament (2015–2025) from multilingual speech analysis](https://ciencia.ucp.pt/en/studentTheses/what-they-say-vs-how-they-say-it/).

[Portfolio and contact](https://www.koslowsky.net/#contact).

Source code is licensed under [MIT](LICENSE). Parliamentary source material retains its source terms; the code license does not relicense it. Source: [European Parliament open data](https://data.europarl.europa.eu/). EUROWATCH is not affiliated with the European Union or European Parliament.
