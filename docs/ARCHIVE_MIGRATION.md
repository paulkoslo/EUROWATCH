# Migrate the full archive

Use the verified research database as the one-time seed. Choose an output path on the same filesystem as its temporary files and leave space for the canonical copy, rollback archive and public release. Keep the original read-only.

```sh
npm run --silent data:migrate -- --source data/ep_data.db --output data/canonical/archive.db > migration-report.json
npm run data:prepare -- --source data/canonical/archive.db --output data/releases/initial-canonical
npm run data:validate -- --dataset data/releases/initial-canonical
```

The command takes a consistent SQLite backup, applies the versioned migration on that copy and makes the private checkpoint visible only after validation. The report records sitting and speech counts by date, row hashes for the legacy data, unresolved speakers and source IDs, duplicate order groups and migrated labels. An existing output is never replaced. Repeat with a different output path and compare the two reports to rehearse from the same archive.

The canonical checkpoint keeps normalized sitting, speech and speaker IDs, plus separate source identities, source revisions, provenance links and enrichment records. Legacy sitting HTML remains available in its original row. Its fingerprint describes the archived copy, and any legacy identifier is a candidate until verified against the official source. The public release rebuilds its legacy label columns and analytics from selected enrichment records, then removes the private checkpoint tables. Keep the checkpoint private and publish the validated release.

Confirm the active manifest ID with `/readyz`, `/api/data-status` and the `X-Dataset-ID` header on `/api/download/full-dataset`. Compare the checksum and check representative early and recent sittings, speaker routes and topic analytics. Retain the last working release for rollback.
