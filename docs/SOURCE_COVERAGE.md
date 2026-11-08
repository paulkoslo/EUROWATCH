# Source reconciliation and classification coverage

`npm run data:refresh -- --start 2026-10-06 --end 2026-10-22 --source docx` selects the official multilingual DOCX first and falls back to the reader HTML. `PLENARY_SOURCE=docx` selects the same policy for the forked preview worker. The existing default remains HTML-first; the hosted runner can pass `sourcePreference: "docx"`. Reader links always point to the public HTML page.

Each run report now includes the calendar's expected dates, its count of unlisted days, and a structured outcome for each expected sitting. Source attempts record format, official URL, duration and outcome. Categories distinguish source refusal (401/403/429), network failure, parser failure, unavailable distribution, and pending publication. Pending requires an unpublished calendar record and absence from both source paths; a refused source is not called pending. Successful imports include the raw payload checksum and retrieval timestamp. Reports contain no transcript payloads or upstream exception text from transcript fetching. Transport retries remain bounded by the existing client; source attempts describe format attempts, not individual HTTP retry counts.

The final result retains `items`, so a failed or pending date and its source diagnostics survive the final summary. Repeat the date range to retry: imported sittings are skipped and only unresolved sittings are fetched. Dates absent from the calendar are not a guarantee that historical upstream coverage is complete. The private hosted run view can consume these events without scraping messages.

## Read-only comparison

Run `node src/data/source-audit.js --start YYYY-MM-DD --end YYYY-MM-DD` to discover official dates and independently attempt HTML and DOCX. Its JSON Lines output compares speech counts and ordered hashes of text, speakers and agenda labels when both parse. It never writes to staging or publishes. Matching hashes alone cannot validate speaker identity: neither existing parser extracts official speaker links. Translated HTML and multilingual DOCX may legitimately differ.

Live probes on November 7–8, 2026:

| Sitting | Official DOCX parsed speeches | Reader HTML |
| --- | ---: | --- |
| 2026-10-06 | 481 | HTTP 403 |
| 2026-10-07 | 486 | HTTP 403 |
| 2026-10-08 | 198 | HTTP 403 |
| 2026-10-19 | 234 | HTTP 403 |
| 2026-10-20 | 489 | HTTP 403 |
| 2026-10-21 | 429 | HTTP 403 |
| 2026-10-22 | 179 | HTTP 403 |
| 2024-04-22 (term 9) | 197 | HTTP 403 |

The term-9 file exposed legacy Word `heading 1` / `Contents` styles and agenda numbers in TC fields. The parser now recognizes those structures and direct TOC runs, with a regression fixture preserving multilingual speech text. The older 2019-07-02 and 2014-07-01 calendar probes returned no records for those dates; direct document-identifier probes listed no matching multilingual DOCX and HTML returned 403. They were probes, not inferred verified source identities. No other machine-readable transcript distribution has been enabled. HTML refusal prevents a live equivalence claim, especially across older terms and speaker links; DOCX-first is an explicit operational choice supported by successful recent parsing, not a declaration of full historical parser coverage.

## Dated MEP membership groundwork

The existing `meps-api` utility merges term lists into one current record. `sync-mep-affiliations` collapses all speech affiliations into the most frequent group, while `historic-meps` can create inferred identities. None supplies effective-dated affiliation history.

The official detail endpoint `https://data.europarl.europa.eu/api/v2/meps/197490` was inspected: `hasMembership` includes `memberDuring.startDate`, optional `endDate`, organization identity, membership classification and parliamentary mandates. `src/data/mep-memberships.js` now normalizes only mandate and EU political-group records, fingerprints them without contact details, and resolves a date with explicit missing/ambiguous/outside-mandate outcomes. End dates are treated as inclusive; missing end dates remain open. Invalid periods remain unresolved. This helper does not overwrite speech affiliations or infer identities from names. Bulk roster ingestion, versioned canonical persistence, organization labels and importer wiring remain follow-up work; it is not yet an automatic roster sync.

## Public coverage

Topic series now compute classified counts from all macro-topic series, independently of topic selection. A period with stored but unclassified speeches has a null topic value; a period with classifications but none for the selected topic has zero. The table and CSV include stored count, classified count, classification rate and observed/classified months. Quarterly and yearly coverage is weighted by speech count. The table is still available when no topic is selected.

The home page shows the exact latest stored sitting date. `/api/data-status` exposes optional refresh metadata from `dataset.release.refresh` (`completedAt`, `pendingDates`, `failedDates`). Absent metadata is shown as unknown, never inferred from release preparation time. The hosted producer/loader must attach this metadata to the served release; this slice does not change schema, migration or hosting.
