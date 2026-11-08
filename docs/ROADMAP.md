# EUROWATCH roadmap

EUROWATCH should answer **“What happened in Parliament on my issue, who said what, and what was decided?”** with links to the underlying record. The product builds on the multilingual speech archive and its full SQLite download. The first release adds dependable updates, trustworthy topic labels and short, source-backed briefings.

## Current state

**Check New Sittings** adds missing plenary sittings and their speeches to a temporary local copy. It records agenda headings, detects language and matches speakers against the existing roster. It then rebuilds the preview's overview counts, speaker index, topic analytics and download from that copy. Existing sittings with speeches are skipped. MEP roster changes, revised transcripts, votes, adopted texts, AI topic labels and summaries are outside the button's current scope. The preview copy lasts for the local session.

- The baseline contained 368,934 speeches across 1,512 sittings through February 12, 2026. The preview published 377,268 speeches across 1,540 sittings through October 5. The overview counts changed. The coverage card shows only the first and last years, so its display stayed at 1999–2026.
- All 8,334 added speeches had an agenda heading and zero had an AI macro-topic label. The topic chart queries macro labels. Its coverage uses all speeches, so a month with imported but entirely unclassified speeches was plotted as zero for every selected topic. The local refresh runs ingestion and rebuilds the preview. It never invokes the classifier, regardless of whether a key is configured.
- The preview kept a single progress message and replaced it with the final totals. Comparing the snapshot with 35 sitting dates from Parliament's calendar identified the seven missing sittings as October 6–8 and 19–22. A later read-only probe fetched and parsed the official DOCX for all seven. The original errors were lost when the status text was overwritten, so the first run's failure causes remain unknown.
- The same seven HTML URLs returned HTTP 403 from CloudFront to the programmatic client. Parliament's document API and DOCX downloads returned HTTP 200. A fetch of the March 25 reader page also showed Parliament's robot verification prompt. This is consistent with automated access filtering on the public site. The documents exist and the exact blocking rule remains unknown. [Parliament Open Data API](https://data.europarl.europa.eu/en/developer-corner/opendata-api) · [March 25 reader page](https://www.europarl.europa.eu/doceo/document/CRE-10-2026-03-25_EN.html) · [CloudFront 403 documentation](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/http-403-permission-denied.html)

## 1. Restore the Render deployment

- Check the latest failed deploy's logs. The SQLite source-build command is already configured. The dashboard audit confirmed a 15 GB disk attached to EUROWATCH-1 at `/opt/render/project/src/data`. Verify the free space and release files, place a validated release on the disk if needed, and set `DATASET_DIR` to its actual directory.
- Add a production install and startup smoke test on Debian 12 or the selected deployment image. Verify the deployed `/readyz` returns the intended release ID and `analyticsAvailable: true`, then check the explorer and download. Keep a validated rollback release. [Render settings and recovery steps](RENDER_DEMO.md)

**Done when** the current app boots on Render and serves the intended release.

## 2. Put every data action behind one service

- Create one `data` service that owns writes and publication. Its stages discover changes, fetch official records, parse and link them, run eligible AI enrichment, rebuild derived tables and exports, validate the release and publish it. Keep sources and stages independently retryable inside that service.
- Route the private **Update Data** button, CLI, scheduled job and targeted retries through the same runner. A normal update checks every enabled source and processes new or revised records. Preview and publish are explicit destinations. The historical AI backfill is an explicit run mode. Reuse successful results, lock publication to one writer, and keep the last good release if validation fails.
- Provide a private operator view with live progress, recent runs, and **Retry failed items**. Print the same events to stdout. Record the run ID, trigger, range, source or stage, document ID, attempt, outcome, duration, row counts and release ID as applicable. Include a short reason for skips and retain errors after the next run. Show AI usage and cost where available. Keep compact reports with bounded retention and omit credentials and transcript bodies from logs.
- Give the web app and data service distinct tasks in Turborepo so the data output has its own stream. Disable task caching for writes, fetches and publication. Run the production data job only on demand or on a schedule, then let it exit. Keep operator controls and run history off the public site. [Turborepo task UI](https://turborepo.dev/docs/crafting-your-repository/developing-applications)

**Done when** a button run and a CLI run use the same stages and report format, two simultaneous triggers cannot publish over each other, and an operator can retry a failed date after restarting the app.

## 3. Reconcile sources and show accurate coverage

- Use Parliament's meetings calendar and official transcript identifiers for discovery. Compare the officially distributed DOCX and any documented machine-readable format against human-readable HTML across terms, languages, and difficult documents. Check counts, order, text, speaker links, and agenda boundaries. If a distribution meets those checks, use it as the scheduled job's primary input and retain the public HTML link for readers.
- Track source versions and fingerprints so revisions can be detected and replayed. Distinguish a non-sitting day, a transcript pending publication, a source refusal, a network error, and a parser error. Reconcile every expected sitting after February 12 against imported data. Exercise the seven October dates as a targeted retry case. Add official MEP roster and affiliation records with effective dates so new speakers and historical affiliations can be linked accurately.
- Show the exact date of the latest stored sitting, the last successful refresh, and known gaps. On the topic chart and in its CSV, show how many speeches were stored and how many were classified per period. Use a gap for months with no topic classifications, a zero when classified data contains no selected topic, and a visible coverage indicator for partial classifications.

**Done when** the calendar, source outcomes, database, chart, and CSV agree about what's present and what's still missing.

## 4. Evaluate AI quality and ship a plenary week

- Build the March 9–12, 2026 preview around agenda items, debates, official vote results, and adopted texts. Connect the records using official identifiers where the source provides them. Show unavailable links explicitly and give each item a shareable URL.
- Audit the existing classifications using a reviewed sample of agenda items and speeches across terms, languages, and confidence levels. Include generic headings like *Question Time*, multi-topic debates, and procedural records. Score agenda assignment separately from topic quality so parser mistakes and classifier mistakes can be corrected at their source.
- Run the legacy classifier as a baseline on the same sample. Evaluate a versioned vocabulary that distinguishes policy themes from procedure and uses debate excerpts when titles are too vague. Use stable record IDs to match results to inputs. Validate the returned structure and treat failed, uncertain, or invalid responses as such. Store model, prompt and taxonomy versions, source references, and review status with the output.
- Create a short briefing per debate and a sitting digest. Cite transcript passages for arguments and attribute them to the right speakers. Use official structured records for vote totals and procedural outcomes. Review the pilot for missing arguments, misleading translations, invented facts, and citation errors. The current public explorer has no generated debate summaries.
- Configure the AI provider and a job-only credential before the pilot. Record whether the AI step ran, was skipped, or failed. Measure quality, coverage, cost, and runtime on the pilot before setting a budget and approving the historical run.

**Done when** a reviewer can compare old and new labels and verify the first week's claims, citations and structured outcomes against the source.

## 5. Reprocess the historical corpus and automate releases

- Re-run enrichment across the full eligible archive after the pilot. Estimate work from the number of unique agenda items and debates. Reuse shared debate classifications where they apply and use speech text for contributions that need their own classification. Generate debate and sitting summaries as separate outputs.
- Process in resumable batches with spend limits and checkpoints. Key outputs by source version and enrichment version so successful work can be reused and revised transcripts can be reprocessed. Record the coverage and remaining exceptions by term and language. Preserve the original labels alongside the candidate outputs for comparison and rollback.
- Review a held-out sample and reconcile coverage before publishing new labels or rebuilding the charts. Then run the same validated enrichment on newly imported material.
- Schedule daily checks using the same data service as the manual button. Use shared storage for release handoff to the web service. A Render cron job cannot access the web service's attached disk, so verify the handoff and rollback before relying on scheduled publication. Reconcile at least two plenary weeks of unattended runs before calling the feed reliable. [Render cron documentation](https://render.com/docs/cronjobs)

**Done when** every eligible record has an auditable outcome, the new labels improve on the baseline in review, and the charts, exports and briefings reference the same published release.

## 6. Distribute the insights

Turn the reviewed plenary briefing into a weekly newsletter. Add procedure timelines, committee activity, and parliamentary questions as source coverage matures. Expose the same searchable, cited record through a documented API and a read-only MCP. Pilot paid watchlists, team alerts, and recurring exports with users who need ongoing monitoring.

[Parliament's Legislative Observatory](https://oeil.europarl.europa.eu/oeil/en/find-out-more) · [Parliament reuse terms](https://www.europarl.europa.eu/legal-notice/en/home)
