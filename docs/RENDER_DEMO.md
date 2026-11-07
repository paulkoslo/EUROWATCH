# Prepared datasets on Render

Use these settings for a Render web service running the prepared-dataset version of EUROWATCH.

## Prepare before changing the service

The stabilized server requires a release directory containing `ep_data.db`, `analytics.db` and `manifest.json`. It refuses legacy databases without this metadata. Generate the release locally from a verified source:

```sh
npm ci
npm run data:prepare -- --source data/ep_data.db --output data/releases/staging
npm run data:validate -- --dataset data/releases/staging
```

For a smaller snapshot, add `--from 2020-01-01` and a new output directory. Do not upload the previously generated `data/demo-data` directory; the audited copy is inconsistent.

## Render settings

1. Use the development branch in a separate staging service, or another environment explicitly selected for testing. Leave the existing production branch on `main`.
2. Put the validated release directory on persistent storage. Do not commit or build the multi-GB corpus in GitHub Actions.
3. Set `DATASET_DIR` to the absolute uploaded directory, `NODE_ENV=production`, `HOST=0.0.0.0`, and a supported Node LTS version. Allow Render to supply `PORT`.
4. In **Settings → Build**, set **Build Command** to `npm run build:render` once the commit that defines it is on the deployed branch. It compiles the locked SQLite addon against Render's Debian libraries and verifies it can load before uploading the build. The existing inline command below performs the same steps.
5. In **Settings → Deploy**, use `npm start` as **Start Command** and leave **Pre-Deploy Command** blank. Render's pre-deploy step uses a separate instance and does not carry file changes into the deployed service.
6. After saving the build command, use **Manual Deploy → Clear build cache & deploy**.
7. Verify `/readyz`, the dataset ID and cutoff, then exercise sittings, speakers, filtered topics and CSV downloads. Measure latency and memory on the actual instance before selecting its final size.

```sh
npm_config_build_from_source=sqlite3 npm ci --omit=dev && node -e "require('sqlite3')"
```

Set `TRUST_PROXY=1` only when exactly one controlled proxy fronts the application. AI provider credentials belong on the enrichment worker. The full `.db` download is enabled by default and can be disabled with `ENABLE_FULL_DATASET_DOWNLOAD=0`.

[Render deploy steps](https://render.com/docs/deploys#deploy-steps) · [SQLite source installation](https://github.com/TryGhost/node-sqlite3/tree/v6.0.1#source-install)

Disk capacity must cover the selected release and retained rollback release. Local preparation also needs room for a full source backup; a subset starts from a consistent full copy before pruning.

EUROWATCH-1's disk was confirmed at `/opt/render/project/src/data`. One release destination on that disk is `/opt/render/project/src/data/releases/production`. Set `DATASET_DIR` to the directory that actually contains `manifest.json`, `ep_data.db` and `analytics.db`. The legacy `DB_PATH` and `ANALYTICS_DB_PATH` variables describe individual files. The prepared release is selected as a directory.

GitHub Actions runs the build command on Debian 12 with Node 22 and exercises the production entry point using a small fixture. The Render readiness check verifies the dataset on the mounted disk.

## Rollback

Keep a previous validated release and its compatible code/configuration. Change `DATASET_DIR` to that directory and restart, or use `data:promote` when the service uses the local pointer. Never replace an active database file in place. Reverting to the old `main` application also requires its original database/configuration, so retain those backups separately.

For a prepared-dataset commit from before `build:render` was added, use the inline build command above. For the legacy app, restore the original deploy configuration and database paths together.
