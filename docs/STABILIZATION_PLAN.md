# EUROWATCH: make the existing explorer dependable

Status: accepted for implementation. The user authorized a development branch, commits and development-branch pushes, while leaving `main` and its hosting untouched. See [release notes](STABILIZATION_RELEASE.md) for implementation and verification; the original findings are in the [audit](AUDIT_2026-11-05.md).

## The immediate goal

Make the database enjoyable and dependable to explore with the existing application. A visitor should be able to find a sitting, read contributions, browse a speaker’s speeches and inspect topic trends without broken filters, misleading totals or accidental maintenance operations.

The database is the product. The thesis is a background link. A larger public identity, research storytelling, richer analysis and an ongoing observatory can be considered once this foundation works.

**Recommendation: keep Express, SQLite and the existing HTML/CSS/JavaScript for this milestone.** Improve boundaries and replace the troublesome parts incrementally. A framework migration would add work without resolving the observed data and query bugs.

## Three supported exploration flows

| Flow | What someone can do | What must remain true |
|---|---|---|
| Browse sittings | Choose a date range, page through sittings, open a sitting, read its agenda and individual contributions, follow the original Parliament source. | Dates, counts and links agree. Opening a stored sitting does not need a new scrape. Back navigation restores the selection. |
| Browse speakers | Find a name, distinguish API and inferred records, open contributions and page through them in chronological order. | Missing identity metadata does not hide speech text. Inferred records are not represented as verified MEPs. Missing profiles fail gracefully. |
| Explore topics | Start with a small useful selection, choose topics/date range/monthly or quarterly or yearly intervals, inspect a chart and its table, then download the displayed series. | Filters mean the same thing before and after caching. Totals use the same population. Empty periods and missing data have defined meanings. |

Keep the existing date/field-based speech CSV export, but clearly label its scope. It should not imply it follows every chart or speaker filter. Exporting the displayed topic series is a small separate action; a universal cross-filter export system can wait.

Do not expand group and language analytics into new features now. Keep only views whose counts and stated filters can be verified. If a panel cannot support a date range or another control, remove that control from that panel or temporarily omit the panel. Do not leave a convincing-looking control that has no effect.

## A small, coherent UI

```text
EUROWATCH                   Sittings | Speakers | Topics | Data & About
Explore European Parliament plenary speeches

368,934 stored speeches  ·  Coverage: Jul 1999–Feb 2026
Snapshot information and known gaps                         [Details]

[ Browse sittings ]    [ Find a speaker ]    [ Explore topics ]

Active selection: date / topic / speaker where supported     [Reset]
Result count and loading / empty / error status
Readable results, with chart + table on the Topics view
Previous / next or load more, with a consistent count

Source links · Data notes · Thesis background · Project repository
```

The numbers above illustrate the current local full dataset; actual UI values must be generated from the served snapshot.

Use the current visual foundation with a consistent header, spacing, buttons, typography and responsive layouts. Give the entry screen a short explanation and direct routes into the three flows. Rename MEPs to Speakers where inferred identities are included. Avoid presenting all-time speaker counts as the composition of today’s Parliament.

Use URL state for the active view and its relevant filters, pagination and selection. Browser back/forward and a shared URL should restore the same exploration. No accounts, saved workspaces or elaborate dashboard builder are needed.

Add one concise Data & About page: what a sitting/speech/speaker record means, snapshot cutoff, coverage, missing metadata, how topics and languages were derived, original sources, project origin and the [master’s thesis](https://ciencia.ucp.pt/en/studentTheses/what-they-say-vs-how-they-say-it/). Do not call this live or comprehensive coverage.

## Work sequence

Each step should be a small reviewable change with its regression checks included. The sequence is designed to produce a useful result early and preserve the existing data.

### 1. Establish a recoverable baseline and development checks

- Create `codex/stabilize-eurowatch` after alignment. Preserve `main` and the existing history.
- Produce a consistent SQLite backup using the backup API or another SQLite-supported snapshot method; record hashes, table counts, schema and source date range. Keep backups outside Git. Verify a restore before any data migration.
- Record the starting dataset as a versioned baseline. No bulk reparse, relabeling or identity rebuild as a startup requirement.
- Add a small deterministic fixture that contains multiple sittings, known speakers, an inferred speaker, missing metadata and an unresolved speaker reference. Keep it tracked; never use the multi-GB dataset in ordinary CI.
- Add `npm test`, lint/format checks, a pinned supported Node LTS runtime and `.env.example` with harmless placeholders.
- Replace the unrelated Jekyll workflow with pull-request validation for the actual app. Keep deployment separate and disabled until explicitly chosen. CI uses `npm ci`, no API keys and no Parliament or OpenAI network calls.
- Review dependency advisories, remove unused dependencies and make compatible upgrades. Validate native SQLite/language modules on the selected runtime. Treat necessary major upgrades separately; no blind `npm audit fix --force`.

**Done when:** a clean clone can install reproducibly, start against the fixture and run checks without secrets or paid services. An original-data backup can be restored.

### 2. Make browsing safe and startup predictable

- Public mode opens a prepared database read-only and mounts only supported browsing endpoints. Offline maintenance commands own all writes.
- Remove legacy mutation routes from public mode. Centralize route policy rather than adding more scattered `LOCALRUN` checks. Local maintenance should bind to loopback if retained.
- Disable the public Ask AI widget and endpoint for this milestone. It is not needed to browse stored speeches.
- Make missing database/schema errors explicit. Public startup must not create an empty database, fetch MEPs, migrate tables, run `ANALYZE` or scrape data in the background.
- Add an explicit initialization/migration command for writable development and maintenance databases. Create tables, apply required columns and then build indexes, in that order. Validate before serving requests.
- Add liveness and readiness endpoints. Readiness confirms the database and supported schema are available; it does not require live access to upstream APIs.
- Put external text through safe text rendering. Sanitize only the limited HTML that the product deliberately supports. Pin/package browser dependencies so they change with a release.

**Done when:** normal browsing cannot alter corpus data, public mode exposes no maintenance or paid-compute route, and a missing/invalid database produces a clear setup error rather than a broken dashboard.

### 3. Serve one consistent dataset

- Use a validated snapshot of the full database as the initial source. Do not deploy the current broken demo.
- Attach a dataset ID to the main snapshot, analytics snapshot and API status. Rebuild analytics offline from that exact main snapshot; reject or explicitly report a mismatched analytics file.
- Measure and display source coverage and metadata coverage. Do not mistake a non-null speaker ID for a resolved identity.
- Preserve speeches with missing profiles: fall back to raw speaker labels and explicit “metadata unavailable.” Keep unknown country/group categories visible in totals where relevant.
- Keep API-sourced and inferred speaker records distinguishable. Move suspicious synthetic identities out of verified-MEP claims without deleting the underlying speeches. Full identity resolution is later work.
- Keep raw groups and speech-level labels intact. Do not globally rewrite small groups into `Other`. Do not claim a speaker’s all-time modal group is their current or historical membership on every date.
- If a smaller dataset is needed, build it by selecting sittings and their dependent speeches together, then validate joins and all totals. Treat the subset as a different dataset release.
- Document the existing classification limitations. Do not rerun all model labels or language detection simply to reach this milestone.

**Done when:** main and analytics data share an ID, sitting orphans are zero, speaker-reference gaps are measured and handled, and totals reconcile across the supported views. Known gaps can remain visible and documented; fabricated completeness cannot.

### 4. Fix the query contract and the three flows

- Define sitting lists separately from individual speech lists. Fix speaker filtering to use individual speech relationships; retire ambiguous legacy behavior deliberately.
- Validate supported query parameters centrally: dates, range ordering, interval, topic list, IDs, limit and offset. Bound limits, topic selections and expensive requests.
- Make cached and uncached analytics use identical topic/date/interval logic. Share normalization rules between queries and returned labels. Include dataset ID and the full applicable filter set in cache keys; bound cache memory.
- Choose and document whether each chart shows counts, shares or another measure. Initially keep counts. A monthly topic chart means recorded speeches assigned to that topic in that month; it does not measure public opinion, policy importance or ideological positions.
- Use `activity_date` for chronology, with a stable secondary sort. Derive page counts and rows from the same joins and filters.
- Define zero versus unavailable periods. Populate zero only where the dataset supports that interpretation; show coverage gaps explicitly.
- Ensure failures are real failures: frontend loaders check HTTP status, clear stale results and provide retry. Expired requests must not overwrite newer filter results.
- Bound and cancel exports on client disconnect. Verify CSV escaping, selected fields, dates and counts. Keep full-database download unavailable in the UI unless a consistent snapshot file actually exists.
- Return a clear unavailable state when precomputed analytics is missing. Avoid launching an unbounded full-corpus aggregation because a visitor opens a tab.

**Done when:** changing a filter reliably changes the intended result, cache state cannot change query meaning, pagination is stable, and every supported export describes its actual scope.

### 5. Finish the usability pass and verify the release candidate

- Add the simple entry screen, consistent navigation and Data & About page described above.
- Add speaker-name search over the existing speaker list or a bounded server endpoint. Do not build a multilingual full-text search engine yet.
- Default to a manageable topic selection, make active filters obvious, and provide reset. Avoid rendering hundreds of chart series at once.
- Give every supported flow distinct loading, empty, error and unavailable states. Missing values read as unknown, not blank success.
- Make results readable on mobile, keep long text within its container, provide visible keyboard focus and accessible form labels. Pair charts with a data table. Test keyboard navigation and back/forward behavior.
- Extract only the modules needed to keep these flows understandable: app startup, route groups, query validation and browser view state. Leave unrelated archive cleanup and broad rewrites alone.
- Update the README to the actual supported setup, endpoints and public/offline distinction. Remove stale instructions that require visitors to run maintenance actions. Resolve the missing LICENSE file with the intended license before publishing a release.

**Done when:** the acceptance checks below pass locally and on a staging-equivalent runtime using a representative dataset. Commit and push only the development branch. Do not merge, push to `main`, or change hosted deployment configuration.

## Acceptance checks

| Area | Required evidence |
|---|---|
| Data preservation | Public browsing leaves the main corpus unchanged; backup/restore works; data and analytics IDs match. |
| Startup | Fixture setup works from a clean clone; missing data and unsupported schemas fail clearly; public startup needs no upstream network. |
| Route safety | Public maintenance and AI routes are absent/denied, including all listed legacy variants. Normal browsing never initiates ingestion. |
| Query correctness | Fixture tests cover date boundaries, speaker joins, unknown metadata, topic normalization, warm/cold cache parity, zero results and invalid limits. |
| Counts and pagination | List counts match their underlying rows; pages do not duplicate or skip records; unresolved speaker profiles do not discard speeches. |
| Exports | Displayed series CSV agrees with its chart/table; speech CSV agrees with its stated date/field selection; aborted downloads stop work. |
| User journeys | Browser checks complete sitting-to-speech, speaker-to-contributions and topic-filter-to-table flows, plus back/forward and direct URLs. |
| Presentation | Keyboard and narrow-screen use work; charts have tables; failed APIs do not show fake empty results or stale charts. |
| Dependencies and CI | Clean install and automated checks pass on the chosen runtime. No unresolved exploitable high/critical advisory in exposed paths; remaining findings are triaged with evidence. |
| Operational behavior | Health/readiness behave correctly, stopping the process closes resources, logs identify failed requests, and the previous code+dataset release can be restored. |

Set performance budgets after measuring the existing full snapshot on the intended host. Provisional goals: ordinary cached requests under 500 ms and common uncached browsing under 2 seconds at the 95th percentile in a documented small-concurrency test; cap responses and chart series. These are targets, not current measurements or hosting guarantees. If the budget is missed, use query plans and measurement before changing storage engines.

## Working model after stabilization

```text
Offline maintenance (explicit command)
  -> staging database
  -> validation + analytics generation
  -> matched, versioned snapshot pair
  -> promote together, preserve previous release

Browser -> supported read-only API -> prepared SQLite snapshot
                |
          bounded analytics cache
```

No microservices, queue service, user accounts, Postgres migration or new hosting platform is required to establish this model. Retain the existing deployment approach provisionally; verify its persistence, resources and rollback behavior before publishing anything.

## Later, after the playground works

Keep this backlog deliberately uncommitted as product direction:

- Better full-text discovery across speeches, with explicit multilingual search behavior.
- Reliable historical speaker identities, country metadata and dated group memberships.
- Carefully designed comparisons and richer public-interest analysis, grounded in measured coverage.
- Automated ingestion with checkpoints, retry accounting, transactional writes and reviewed classification changes.
- A stronger public visual identity, curated stories and more shareable charts.
- A documented public API and independently hosted, versioned data downloads.

Revisit these using what people actually struggle with in the stabilized explorer. The immediate decision is the scope of the five steps above, not the final form of the project.

## Reference guidance

- [GitHub: building and testing Node.js](https://docs.github.com/en/actions/tutorials/build-and-test-code/nodejs) supports the proposed application CI setup.
- [W3C WCAG 2.2](https://www.w3.org/TR/WCAG22/) is the reference for the targeted accessibility work. Automated checks alone do not establish full conformance.
- [Master’s thesis record](https://ciencia.ucp.pt/en/studentTheses/what-they-say-vs-how-they-say-it/) provides project background and the requested thesis link.
