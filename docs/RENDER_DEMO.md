# Prepared datasets on Render

These are setup notes for a future development/staging deployment. The existing hosted service and `main` branch have not been changed.

## Prepare before changing the service

The stabilized server requires a release directory containing `ep_data.db`, `analytics.db` and `manifest.json`. It refuses legacy databases without this metadata. Generate the release locally from a verified source:

```sh
npm ci
npm run data:prepare -- --source data/ep_data.db --output data/releases/staging
npm run data:validate -- --dataset data/releases/staging
```

For a smaller snapshot, add `--from 2020-01-01` and a new output directory. Do not upload the previously generated `data/demo-data` directory; the audited copy is inconsistent.

## Proposed staging configuration

1. Use the development branch in a separate staging service, or another environment explicitly selected for testing. Leave the existing production branch on `main`.
2. Put the validated release directory on persistent storage. Do not commit or build the multi-GB corpus in GitHub Actions.
3. Set `DATASET_DIR` to the absolute uploaded directory, `NODE_ENV=production`, `HOST=0.0.0.0`, and a supported Node LTS version. Allow Render to supply `PORT`.
4. Build: `npm ci --omit=dev`. Start: `npm start`. Set `TRUST_PROXY=1` only when exactly one controlled proxy fronts the application.
5. Keep API keys out of the public service. The full `.db` download remains available by default, as in the previous app. An explicit `ENABLE_FULL_DATASET_DOWNLOAD=0` override can disable it on a particular host.
6. Verify `/readyz`, the dataset ID and cutoff, then exercise sittings, speakers, filtered topics and CSV downloads. Measure latency/memory on the actual instance before selecting its final size.

Disk capacity must cover the selected release and retained rollback release. Local preparation also needs room for a full source backup; a subset starts from a consistent full copy before pruning.

## Rollback

Keep a previous validated release and its compatible code/configuration. Change `DATASET_DIR` to that directory and restart, or use `data:promote` when the service uses the local pointer. Never replace an active database file in place. Reverting to the old `main` application also requires its original database/configuration, so retain those backups separately.

No hosted provisioning, deployment, domain change or production data update is part of this branch’s implementation.
