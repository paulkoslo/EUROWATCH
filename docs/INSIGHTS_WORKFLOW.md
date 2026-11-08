# Cited insights and evaluation

This module produces **unreviewed candidate artifacts**, never changes the source archive, and never promotes a dataset. The hosted publisher must require explicit reviewed approval before projecting candidates into a release. Quotation/source checks establish traceability, not semantic truth: factuality, omitted arguments, translation and topic quality need human review.

## Input and integration contract

`{config, records, officialEvidence}` is a JSON snapshot exported from a consistent canonical checkpoint. `config` requires `model`, `promptVersion`, `taxonomyVersion`, and an explicit `topics` string array (include a procedural category). Each record needs:

- `id`: stable normalized key; `kind`: `speech`, `debate`, or `sitting`.
- `inputRevision`, `effectiveAt` (ISO timestamp), `term`, `language`, `agendaTitle`.
- `officialIds`: exact official relationship identifiers, or an empty array. Never synthesize these from dates, titles or legacy sitting keys.
- `passages`: `{id,text,speakerId,sourceUrl,sourceRevision}` from the source snapshot. URLs must be Parliament HTTPS URLs. Preserve original text/language and original speaker identity; use null for an unknown speaker.
- `legacyLabels`: existing labels and confidence, retained unchanged as the historical baseline. A separately rerun legacy classifier can be evaluated on the same sample, but must be versioned and identified as a new baseline, not represented as an original label.

Debates and sittings use their constituent source passages. Include speech records separately where debate-wide labels would obscure a contribution; do not blindly copy a debate label to all speeches. The upstream adapter must use normalized agenda membership, flag ambiguous groups, and build a full sitting record for a digest. Oversized inputs fail explicitly rather than silently truncating evidence.

`officialEvidence` items require `{id,kind,officialIds,sourceUrl,sourceRevision,effectiveAt}`. `kind` is `vote` or `adopted_text`; votes require nonnegative integer `totals: {for,against,abstention}` taken from structured official records. Fetch official vote/adopted-text records through the [Parliament Open Data API](https://data.europarl.europa.eu/en/developer-corner/opendata-api), retaining the original payload/revision in canonical storage. The adapter must map actual official relation IDs, not infer them from a sitting date or text similarity. No evidence is joined without an exact identifier intersection. Missing relationships are exported in `missingEvidence`; generated prose cannot supply official totals. Source authenticity and accurate payload extraction remain the upstream source adapter's responsibility.

The candidate key hashes the entire record, linked official evidence, taxonomy and model/prompt/schema versions. Corrections, language/text changes, revised evidence and enrichment changes all invalidate reuse. Results preserve input revision, effective time, generation time, legacy baseline and citation source revisions. Keep results private until reviewed; provider credentials belong only on the job.

## Pilot and review

Export directly from a read-only legacy archive or canonical schema v1 snapshot:

```
node src/insights/corpus.js --db data/ep_data.db --config insight-config.json --output corpus.json --from 2026-03-09 --to 2026-03-12
```

The config file contains the versioned model/prompt/taxonomy settings described above. Optional `--evidence official-evidence.json` accepts normalized official evidence from the trusted source adapter. `--kinds debate,sitting,speech`, `--max-chars 24000` and `--max-passages 80` control output. The exporter reads a consistent transaction and yields one sitting at a time through `streamCorpus(options)`; JSON writing is streaming. Existing pilot/backfill CLIs load their input JSON, so export bounded date ranges for large archives and reuse the same checkpoint across ranges. Do not run many paid whole-archive copies in parallel.

Legacy source URLs come only from the retained HTML canonical link; source gaps are explicit diagnostics, never invented URLs. Canonical exports use the exact provenance revision and known nonlegacy official identifiers. Contiguous agenda headings provide provisional debate membership, labelled `legacy_contiguous_heading`; repeated headings separated by other agenda items remain separate. These groups are not claimed to be official debate IDs. Transcript IDs are not copied to debate groups. Source text is never discarded to meet a token limit: long passages/debates/sittings are split, retaining their parent ID and explicit `scope` (part count, selected/total passages, missing empty text). Treat these as partial summaries until reviewed and assembled. Effective timestamps from sitting dates are explicitly day precision.

Offline adapter validation on the real legacy archive's 2026-02-12 sitting produced 212 records, zero source diagnostics and a largest evidence chunk of 23,947 characters. This validates the export contract for that sitting, not full-corpus quality or official agenda membership.

```
node src/insights/cli.js pilot --input corpus.json --output pilot-plan.json --from 2026-03-09 --to 2026-03-12
node src/insights/cli.js pilot --input corpus.json --output sample-plan.json --sample 2
```

With no provider, the CLI creates requests and explicit skipped outcomes without reading credentials. Sampling is deterministic across term, language, record kind, procedural headings and low legacy confidence. Review multi-topic debates separately and preserve a held-out set. The March pilot and the cross-corpus sample serve different purposes.

Offline responses map the exact request key to `{topics:[{label,confidence,passageIds}],claims:[{text,passageId,quote,speakerId}],uncertain}`. Quotations must be verbatim and match their passage/speaker. An empty result must explicitly be uncertain.

```
node src/insights/cli.js pilot --input corpus.json --output pilot.json --provider offline --responses responses.json
node src/insights/cli.js evaluate --input pilot.json --reviews reviews.json --output evaluation.json
```

Each review supplies `key`, `candidateHash`, `reviewer`, `reviewedAt`, `split` (`pilot` or `heldout`) and independent 0–1 scores for `agenda`, `candidateTopic`, `baselineTopic`, `factuality`, `citation`, `balance`. Agenda scoring isolates parser errors from topic classification errors. Include reviewer notes for invented facts, missing arguments and misleading translations. The report exposes coverage, scores and review evidence; it never auto-approves quality based on a mean. A failed/uncertain candidate must remain a failed/uncertain outcome rather than receiving a fallback policy label.

## Explicit live adapter

The adapter uses [OpenAI Chat Completions](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create) JSON mode with local structure and provenance validation, a request timeout, no automatic retries, and a completion-token cap. Select a compatible model explicitly and supply current prices; no model or price is assumed.

```
node src/insights/cli.js pilot --input corpus.json --output pilot.json --checkpoint data/insights/pilot-v1 --provider openai --budget 1 --input-price INPUT_USD_PER_MILLION --output-price OUTPUT_USD_PER_MILLION --max-output-tokens 1200
```

Only explicit `--provider openai` reads `OPENAI_API_KEY`. Budget reservations conservatively use UTF-8 input bytes plus protocol overhead and the maximum output tokens. Reported usage/cost is distinct from reserved cost; prices are operator-supplied estimates, so use a provider project spending limit as an additional billing control. No credential or paid-provider call is part of tests. No evaluation or live AI quality claim has been established by fixture tests.

## Historical backfill and recovery

Backfill is a separate explicit command, never triggered by a normal data refresh. Use the reviewed taxonomy/prompt/model configuration from the pilot and reconcile held-out quality before promotion:

```
node src/insights/cli.js backfill --input corpus.json --checkpoint data/insights/backfill-v1 --output backfill.json --provider offline --responses responses.json --max-items 100
```

Live mode uses the same explicit provider and price flags as the pilot. `--budget` is the lifetime reservation ceiling for that checkpoint, including all prior failed and interrupted attempts; it does not reset each invocation. Each response is validated and atomically persisted as a versioned object before the next item. The append-only, fsynced `events.jsonl` reserves spending before a call and retains compact attempt/outcome events. `latest-report.json` contains coverage and exceptions by term/language/kind. The configured batch limit bounds new attempts, while completed candidates and uncertain results are reused without a provider call. Review uncertain records explicitly; changing a version creates a separate candidate, preserving the original.

Rerun the same command to resume. A failure is retained until `--retry-failed true`; interrupted attempts without an output also require that flag. Both reserve additional budget. A process crash leaves `writer.lock`: verify that its `owner.json` PID/job is no longer running, then remove only that lock directory. A damaged ledger fails closed; recover it from trusted storage, never delete spend history to force a restart. Only one writer may use a checkpoint. Hosted jobs must hydrate and durably upload this private checkpoint using the hosted service's existing single-writer mechanism; ephemeral local checkpoints alone do not survive compute replacement.

Only the named input snapshot's result versions appear in a report; prior source/enrichment versions remain in `objects/`. The publisher can read a reviewed report's `results`, join `recordId` and `inputRevision` to canonical records, reject stale revisions, then rebuild labels, charts, CSV and briefings together. Never overwrite legacy labels or promote `publicationAllowed: false` automatically. Counts distinguish candidate, uncertain, failed, skipped and interrupted records; candidate coverage is not reviewed coverage. `reportedCostUsd` summarizes available usage for this snapshot, while `reservedUsd` includes unsuccessful/unknown-cost attempts across the entire checkpoint.

Run offline verification with `node --test test/insights-pilot.test.js test/insights-backfill.test.js`. The tests exercise replay, revision invalidation, budget exhaustion, failure retry, interruption, batch continuation, exclusive writers and corrupt-ledger rejection. A production pilot, human quality review, full-corpus spend/runtime estimate and unattended release verification remain operator gates.

If actual usage exceeds its reservation, the checkpoint records a spending halt before processing another item. It will not submit more paid calls on restart. Investigate the model and price configuration before choosing a new versioned checkpoint.


Use the Parliament evidence adapter to collect the week's structured decisions and adopted texts. It requires an exact calendar relationship to a transcript, uses official totals, and leaves debate-level relationships missing when the transcript does not identify the official activity.

```sh
node src/insights/parliament-evidence.js --from 2026-03-09 --to 2026-03-12 --output official-evidence.json
node src/insights/corpus.js --db archive.db --config insight-config.json --output sample.json --sample 2
node src/insights/corpus-backfill.js --db archive.db --config insight-config.json --checkpoint data/insights/backfill-v1 --output coverage.json
```

The last command walks the full archive in monthly batches. Supply the explicit provider and budget flags to make live calls. Use a stable, read-only SQLite backup with no pending WAL. Completed monthly reports are keyed to that snapshot, configuration and official evidence. It writes per-month reports, a corpus coverage report, and a shared spending ledger. Rerunning uses completed objects and bounds the number of new attempts across the invocation.


Rerun the original agenda-only prompt against the same sample using `node src/insights/legacy-baseline.js --input sample.json --taxonomy legacy-labels.json --output baseline.json`. The taxonomy file is a frozen JSON array of the original labels. Use a separate checkpoint, explicit provider, budget and prices for a paid baseline. Original archived labels remain available for comparison. For a live briefing pilot, export `--kinds debate,sitting` and set `--max-items` to cover your chosen sample size.
