# Official Parliament records

EUROWATCH can publish a selected set of legislative procedures and their timelines, committee documents, and parliamentary questions alongside the speech archive. The explorer offers `/parliament` and `/api/official-context`. Each entry identifies the corresponding Parliament record. The current release's full SQLite download contains the same edition.

```sh
node src/insights/official-context.js --procedures 2026-0801 --questions E-10-2026-000002 --committees AFCO-AD-592152 --output official-context.json
node src/insights/release.js --context official-context.json
```

The first command is read-only and accepts up to 20 official IDs per kind. Missing, refused, or mismatched responses appear in its issues list. Each timeline distinguishes a completed request from the collection limit. This is selected coverage, and committee documents are one view of committee activity. Record relationships use official IDs supplied by Parliament. The second command uses the same private checkpoint, writer lease and verified release publication as the reviewed briefing workflow. An identical edition replay leaves the release unchanged.

A live source probe on November 8, 2026 retrieved all three example records. See [the Parliament Open Data API](https://data.europarl.europa.eu/en/developer-corner/opendata-api) for the supported source resources.
