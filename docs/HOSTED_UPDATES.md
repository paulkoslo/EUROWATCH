# Hosted data updates

The web service reads verified releases from the configured object store. A Render cron job and the private Update Data page use the same job command. Manual runs launch a Render one-off job from the cron service's build and configuration. The object store lease serializes writes across both triggers.

## Seed once

1. Use the verified full archive and the commands in [Archive migration](ARCHIVE_MIGRATION.md) to prepare the canonical checkpoint and public release. If those files are on Render already, seed from that host. Allow space for the working archive, output and temporary data.
2. Configure a private S3 bucket with the settings in [Release storage](RELEASE_STORAGE.md). Give the cron job write access to release, checkpoint, lease and run report keys. Give the web app read access to public release objects, the current pointer and the operator report and lease keys. Keep the bucket private.
3. With the storage credentials and settings available, publish the matching pair once.

```sh
npm run data:update:hosted -- --seed --dataset data/releases/initial-canonical --checkpoint data/canonical/archive.db
```

The seeder checks checksums and compares the archive rows in both files before uploading. It refuses to replace an existing pointer.

## Render settings

- Keep the web service build command as `npm run build:render` and start command as `npm start`. Use `/readyz` as the health check. Set the shared release configuration and put `RELEASE_CACHE_DIR` on the mounted disk.
- Create a cron job using the same branch and build command. Set its command to `npm run data:update:hosted` and schedule to `0 6 * * *` for 06:00 UTC. Set the shared release configuration on the job. Set `DATA_UPDATE_WORK_DIR` only if the default temporary filesystem lacks space. Verify actual scratch space before cutover. The job needs the checkpoint and rebuilt download on disk at once and checks capacity before transferring.
- Set `DATA_UPDATE_OPERATOR_PASSWORD` to a random value of at least 24 characters on the web service. Set `DATA_UPDATE_RENDER_CRON_ID` to the cron service ID and `DATA_UPDATE_RENDER_API_KEY` to its Render API credential on the web service. Visit `/operator/` and sign in as `operator`. Keep the API credential on the server.
- Set an object lifecycle policy for `private/runs/` of 90 days and abort incomplete multipart uploads. Keep checkpoint and release files referenced by the current pointer and the previous verified release. Apply any longer retention to release history independently.

The job checks the recent 14 days for corrections and revisits unresolved publication dates. An ambiguous correction is logged for review. A source failure blocks the entire candidate. Retrying a failed run replays its original range so previously successful imports in that batch are recovered too. An unchanged run records its outcome without changing the public release ID.

Turborepo shows separate web and data preview streams with `npm run dev`. With local object-store credentials configured, `npm run dev:update` shows the hosted job's data task by itself. Refresh and publish tasks run uncached.

## Hosted acceptance

With a single seed and the web and cron services on the same deployed code, publish one changed release with the private button and one from the schedule. Check the recorded trigger and release ID in the run history. Verify both public API and download identify each release and that the SQLite checksum matches its manifest. Replay the same source dates and confirm zero duplicate records and the same release ID. On a staging service, fail a source fetch or corrupt a candidate and confirm the previous verified API and download remain active. Check that the run history records the error and retry the range.

[Render cron jobs](https://render.com/docs/cronjobs) · [Render one-off jobs](https://render.com/docs/one-off-jobs)
