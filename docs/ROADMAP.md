# EUROWATCH roadmap

EUROWATCH should answer **“What happened in Parliament on my issue, who said what, and what was decided?”** with links to the underlying record. The product builds on the multilingual speech archive and its full SQLite download. The first release adds dependable updates, trustworthy topic labels and short, source-backed briefings.

## Current state

The production audit found that the public site was still serving the old deployment. The newer attempts passed the SQLite build and failed at startup while looking for a prepared release. EUROWATCH-1 has a 15 GB disk at `/opt/render/project/src/data`. Identify the actual production archive and free space on that disk before planning the cutover. [Render service settings](https://dashboard.render.com/web/srv-d67iisd6ubrc73934dn0/settings)

**Check New Sittings** starts the same data runner as `npm run data:refresh`. It adds missing plenary sittings and their speeches to a local staging copy, records agenda headings, detects language and matches speakers against the existing roster. It then builds and verifies a preview release with updated counts, speaker index, topic analytics and download. Staging, the selected preview and run reports survive restarts. Existing sittings with speeches are skipped. MEP roster changes, revised transcripts, votes, adopted texts, AI topic labels and summaries are outside the button's current scope.

- The local full archive contains 368,934 speeches across 1,512 sittings through February 12, 2026 and occupies about 4.3 GiB. All 1,512 sitting keys use the legacy `sitting-YYYY-MM-DD` form. The sitting rows have zero populated `docIdentifier` or `notationId` values. Preserve those existing keys and reconcile official identifiers separately. The local preview imported 8,334 additional speeches across 28 sittings through October 5.
- All 8,334 added speeches had an agenda heading and zero had an AI macro-topic label. The topic chart queries macro labels. Its coverage uses all speeches, so a month with imported but entirely unclassified speeches was plotted as zero for every selected topic. The local refresh runs ingestion and rebuilds the preview. It never invokes the classifier, regardless of whether a key is configured.
- The preview kept a single progress message and replaced it with the final totals. Comparing the snapshot with 35 sitting dates from Parliament's calendar identified the seven missing sittings as October 6–8 and 19–22. A later read-only probe fetched and parsed the official DOCX for all seven. The original errors were lost when the status text was overwritten, so the first run's failure causes remain unknown.
- The same seven HTML URLs returned HTTP 403 from CloudFront to the programmatic client. Parliament's document API and DOCX downloads returned HTTP 200. A fetch of the March 25 reader page also showed Parliament's robot verification prompt. This is consistent with automated access filtering on the public site. The documents exist and the exact blocking rule remains unknown. [Parliament Open Data API](https://data.europarl.europa.eu/en/developer-corner/opendata-api) · [March 25 reader page](https://www.europarl.europa.eu/doceo/document/CRE-10-2026-03-25_EN.html) · [CloudFront 403 documentation](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/http-403-permission-denied.html)

## 1. Rehearse startup and plan the archive cutover

- Use the Debian 12 CI check and rehearse the app against a prepared release from a copy of the full archive. Record the source path, size, date coverage and counts. Verify the actual free space for a consistent backup, the new release and rollback. The legacy deployment remains the rollback until the new one passes its checks. [Render runbook](RENDER_DEMO.md)
- Use the verified full archive as the single production seed for the migration in step 2. If the archive already on Render is verified and accessible, use a consistent copy of it. When the migrated release is ready, set `DATASET_DIR` to the actual validated directory for cutover. Require `/readyz` to report the manifest's release ID and available analytics, then check the public API and download before accepting the cutover. [SQLite backup](https://www.sqlite.org/backup.html)

**Done when** a rehearsal proves the full archive can be prepared and served, and the production cutover has a verified source, sufficient space and a rollback path.

## 2. Separate source records, normalized data and enrichment

- **Official sources and revisions.** Store the provider, record type, official ID when available, source URL, retrieval time, payload reference and checksum. Keep revisions and point the normalized record to the source version it used. Mark legacy imports and uncertain matches explicitly.
- **Normalized records.** Keep stable keys for sittings and speeches, and map official identifiers to them as they become available. Preserve existing public identifiers and source links. Carry original text and language, speaker and agenda references, and source revision links. Handle ambiguous or missing identifiers explicitly so a correction does not silently create a duplicate speech.
- **AI enrichment.** Store existing labels as legacy results and store new topics and summaries against the relevant record and input revision, with model, prompt and taxonomy versions, evidence and outcome. Keep the source text independent of enrichment. Rebuild speaker indexes, analytics and the downloadable SQLite projection from the same versioned records.
- **Migration.** Create a versioned, repeatable migration and test it against a copy of the real archive. Compare sitting and speech counts by date, identifiers, text hashes, links, and representative API queries and downloads. Check integrity, orphan records, and duplicate keys. Run the migration twice on the same input and verify identical logical results. Record unresolved provenance and reconcile official identifiers from trusted sources where possible.

**Done when** a tested migration can seed production once from the verified archive, with a validated first release and a record of any gaps or exceptions.

## 3. Publish releases from one hosted data service

- Reuse the local runner for the private **Update Data** button on Render, the scheduled job and targeted retries. Detect changes for the first enabled source using its official identity and payload fingerprint. Process new or revised records, rebuild analytics and downloads from the result, and validate before publication. Make the historical AI backfill an explicit run. Keep the job short lived and use a single writer across both triggers.
- Use shared release storage between the job and web service. Store the private canonical checkpoint and the public release under versioned keys, with their manifests and checksums. Upload and validate the full release before conditionally advancing the current pointer. A no-op retains the current release. Render cron and one-off jobs run on separate compute and cannot access the web service's disk. [Render cron jobs](https://render.com/docs/cronjobs) · [Render one-off jobs](https://render.com/docs/one-off-jobs)
- Have the web app check the current pointer automatically, stage and verify the files, and switch the API and download together to the same release. Keep requests already in flight on their original release. Report the active release ID, and keep serving the previous verified copy when fetching or validation fails.
- Give the owner a private view with live progress, recent runs and **Retry failed items**. Use the same structured events in stdout and durable run reports. Include run ID, trigger, source or stage, attempts, outcome, counts, duration and release ID. Bound retention and keep credentials and transcript bodies out of logs. Give the web and data tasks distinct streams in Turborepo, with task caching disabled for data writes and publication. [Turborepo task UI](https://turborepo.dev/docs/crafting-your-repository/developing-applications)

### Hosted release gate

Seed once and run two successful updates that each publish a changed release on Render. Use the private button for one and the scheduler for the other. Verify both jobs complete with the same deployed code and no laptop upload, configuration change or code redeploy. For each update, check the release ID in the public API and download response, then compare the downloaded SQLite file's checksum with the manifest.

Replay the same inputs and verify zero duplicate records and the same published release ID. Force a fetch or validation failure and verify the previous good release still serves both API and download. Confirm that the failure appears in the run history and can be retried.

## 4. Reconcile sources and show accurate coverage

- Use Parliament's meetings calendar and official transcript identifiers for discovery. Compare the officially distributed DOCX and any documented machine-readable format against human-readable HTML across terms, languages, and difficult documents. Check counts, order, text, speaker links, and agenda boundaries. If a distribution meets those checks, use it as the scheduled job's primary input and retain the public HTML link for readers.
- Extend source version and fingerprint checks as new inputs are enabled. Distinguish a non-sitting day, a transcript pending publication, a source refusal, a network error, and a parser error. Reconcile every expected sitting after February 12 against imported data. Exercise the seven October dates as a targeted retry case. Add official MEP roster and affiliation records with effective dates so new speakers and historical affiliations can be linked accurately.
- Show the exact date of the latest stored sitting, the last successful refresh, and known gaps. On the topic chart and in its CSV, show how many speeches were stored and how many were classified per period. Use a gap for months with no topic classifications, a zero when classified data contains no selected topic, and a visible coverage indicator for partial classifications.

**Done when** the calendar, source outcomes, database, chart, and CSV agree about what's present and what's still missing.

## 5. Evaluate AI quality and ship a plenary week

- Build the March 9–12, 2026 preview around agenda items, debates, official vote results, and adopted texts. Connect the records using official identifiers where the source provides them. Show unavailable links explicitly and give each item a shareable URL.
- Audit the existing classifications using a reviewed sample of agenda items and speeches across terms, languages, and confidence levels. Include generic headings like *Question Time*, multi-topic debates, and procedural records. Score agenda assignment separately from topic quality so parser mistakes and classifier mistakes can be corrected at their source.
- Run the legacy classifier as a baseline on the same sample. Evaluate a versioned vocabulary that distinguishes policy themes from procedure and uses debate excerpts when titles are too vague. Use stable record IDs to match results to inputs. Validate the returned structure and treat failed, uncertain, or invalid responses as such. Store model, prompt and taxonomy versions, source references, and review status with the output.
- Create a short briefing per debate and a sitting digest. Cite transcript passages for arguments and attribute them to the right speakers. Use official structured records for vote totals and procedural outcomes. Review the pilot for missing arguments, misleading translations, invented facts, and citation errors. The current public explorer has no generated debate summaries.
- Configure the AI provider and a job-only credential before the pilot. Record whether the AI step ran, was skipped, or failed. Measure quality, coverage, cost, and runtime on the pilot before setting a budget and approving the historical run.

**Done when** a reviewer can compare old and new labels and verify the first week's claims, citations and structured outcomes against the source.

## 6. Reprocess the historical corpus

- Re-run enrichment across the full eligible archive after the pilot. Estimate work from the number of unique agenda items and debates. Reuse shared debate classifications where they apply and use speech text for contributions that need their own classification. Generate debate and sitting summaries as separate outputs.
- Process in resumable batches with spend limits and checkpoints. Key outputs by source version and enrichment version so successful work can be reused and revised transcripts can be reprocessed. Record the coverage and remaining exceptions by term and language. Preserve the original labels alongside the candidate outputs for comparison and rollback.
- Review a held-out sample and reconcile coverage before publishing new labels or rebuilding the charts. Then run the same validated enrichment on newly imported material.
- Reconcile at least two plenary weeks of unattended runs before treating the live feed as reliable.

**Done when** every eligible record has an auditable outcome, the new labels improve on the baseline in review, and the charts, exports and briefings reference the same published release.

## 7. Distribute the insights

Turn the reviewed plenary briefing into a weekly newsletter. Add procedure timelines, committee activity, and parliamentary questions as source coverage matures. Expose the same searchable, cited record through a documented API and a read-only MCP. Pilot paid watchlists, team alerts, and recurring exports with users who need ongoing monitoring.

[Parliament's Legislative Observatory](https://oeil.europarl.europa.eu/oeil/en/find-out-more) · [Parliament reuse terms](https://www.europarl.europa.eu/legal-notice/en/home)

## Review and deploy the stack

Review the PRs in order. Each PR builds on the branch above it.

1. [PR #3](https://github.com/paulkoslo/EUROWATCH/pull/3) verifies the Render build. [PR #4](https://github.com/paulkoslo/EUROWATCH/pull/4) extracts the local runner and preserves staging and logs. [PR #5](https://github.com/paulkoslo/EUROWATCH/pull/5) records the cutover plan.
2. [PR #6](https://github.com/paulkoslo/EUROWATCH/pull/6) migrates the full archive and rebuilds the public release. [PR #7](https://github.com/paulkoslo/EUROWATCH/pull/7) reports source outcomes and corrects classification coverage.
3. [PR #8](https://github.com/paulkoslo/EUROWATCH/pull/8) adds verified release storage and automatic pickup. [PR #9](https://github.com/paulkoslo/EUROWATCH/pull/9) adds the shared manual and scheduled hosted job. [PR #10](https://github.com/paulkoslo/EUROWATCH/pull/10) synchronizes official MEP profiles and membership dates.
4. [PR #11](https://github.com/paulkoslo/EUROWATCH/pull/11) adds the cited pilot, legacy baseline and resumable historical enrichment. [PR #12](https://github.com/paulkoslo/EUROWATCH/pull/12) publishes reviewed editions and adds the API, feed, read-only MCP and private watchlist pilot. The final context PR adds selected procedures, committee documents and questions.

The operator handles each merge and deployment. The archived input has been rehearsed locally. Follow the [hosted setup and acceptance checks](HOSTED_UPDATES.md) to verify the real disk, object storage, and first manual and scheduled releases. Use the [AI workflow](INSIGHTS_WORKFLOW.md) and [briefing runbook](PUBLIC_BRIEFINGS.md) for the live pilot, human evaluation and approved backfill. The public context feed starts with [selected official records](OFFICIAL_CONTEXT.md). Newsletter sending, billing and customer onboarding are operational follow-up after the pilot.

Automated transcript and roster updates are independent of paid enrichment. Enable and evaluate a provider with a budget before scheduling additional paid work.
