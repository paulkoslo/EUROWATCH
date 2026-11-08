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

Each review supplies `key`, `reviewer`, `reviewedAt`, `split` (`pilot` or `heldout`) and independent 0–1 scores for `agenda`, `candidateTopic`, `baselineTopic`, `factuality`, `citation`, `balance`. Agenda scoring isolates parser errors from topic classification errors. Include reviewer notes for invented facts, missing arguments and misleading translations. The report exposes coverage, scores and review evidence; it never auto-approves quality based on a mean. A failed/uncertain candidate must remain a failed/uncertain outcome rather than receiving a fallback policy label.

## Explicit live adapter

The adapter uses [OpenAI Chat Completions](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create) JSON mode with local structure and provenance validation, a request timeout, no automatic retries, and a completion-token cap. Select a compatible model explicitly and supply current prices; no model or price is assumed.

```
node src/insights/cli.js pilot --input corpus.json --output pilot.json --provider openai --budget 1 --input-price INPUT_USD_PER_MILLION --output-price OUTPUT_USD_PER_MILLION --max-output-tokens 1200
```

Only explicit `--provider openai` reads `OPENAI_API_KEY`. Budget reservations conservatively use UTF-8 input bytes plus protocol overhead and the maximum output tokens. Reported usage/cost is distinct from reserved cost; prices are operator-supplied estimates, so use a provider project spending limit as an additional billing control. No credential or paid-provider call is part of tests. No evaluation or live AI quality claim has been established by fixture tests.
