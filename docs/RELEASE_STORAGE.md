# Private release storage and automatic pickup

The worker and web service share a **private S3-compatible bucket**. No bucket provisioning or live configuration is performed by this code. `RELEASE_BUCKET` enables the adapter; `RELEASE_PREFIX` defaults to `eurowatch`, `RELEASE_REGION` to `us-east-1`. Set `RELEASE_ENDPOINT` for an HTTPS-compatible provider and `RELEASE_FORCE_PATH_STYLE=1` if required. The AWS SDK uses its standard credential chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`). Keep credentials in server-side environment configuration.

The provider must implement conditional `PutObject` and conditional `CompleteMultipartUpload`, including `If-Match` and `If-None-Match`, and return ETags. There is no unsafe unconditional fallback. Enable bucket public-access blocking and encryption, and configure a lifecycle rule to abort incomplete multipart uploads. The application never sets public ACLs. Grant the web identity only read access to `current.json` and `releases/*`; grant the worker the additional private checkpoint/report and multipart permissions. Never serve the bucket or the cache directory through a static route.

## Worker API

`createReleaseStorageFromEnv()` in `src/data/release-storage.js` returns `null` when no bucket is configured. Otherwise:

- `readCurrent()` returns `null` or `{value, etag}`. Save this ETag **before** starting a run.
- `uploadRelease(directory)` validates the complete manifest, both database sizes, SHA-256 hashes and matching DB metadata; uploads immutable files; and returns `{id, manifestSha256}`. The manifest is uploaded last. Never alter prepared files while uploading.
- `uploadCheckpoint(sourceFile, runId)` returns `{key, sha256, bytes}`. The source must be a closed, consistent standalone SQLite copy with no uncheckpointed WAL. Checkpoints use a separate private prefix.
- `publishRelease({...release, checkpoint}, {expectedETag})` verifies remote bytes and atomically publishes both references. Pass the saved ETag, or explicit `null` when creating the first pointer. A competing update fails with `error.code === "RELEASE_CONFLICT"`; retain its report and retry from the newly published checkpoint. Never catch that failure and retry with a newly fetched ETag without rerunning the work.
- `downloadCheckpoint(reference, destination)` streams and verifies bytes before replacing the destination. `downloadRelease(reference, destination)` requires a nonexistent destination, removes partial output on failure, and validates the downloaded DBs.
- `writeRunReport(runId, report, {expectedETag})` creates or conditionally updates a private JSON report; `readRunReport(runId)` returns `{value, etag}` or `null`. Reports are limited to 1 MiB. Callers must omit secrets and transcript bodies.
- `listRunReports({limit: 50, continuationToken})` returns `{items: [{runId, lastModified}], nextToken}` latest-modified first. Each call scans at most 1,000 report objects and fails with `REPORT_SCAN_LIMIT` if the history exceeds that bound. Pagination is for an operator view, not a transactional snapshot; concurrent report updates can move rows between pages. Use bucket lifecycle retention for old reports and unpublished checkpoints, with a retention policy that preserves published and rollback checkpoints.
- `readLease()` / `writeLease({runId, expiresAt}, {expectedETag})` expose a private conditional lease record. The runner owns expiry, heartbeat and lost-lease handling; storage does not infer lock ownership or delete expired leases.
- `inspectRelease(reference)` returns `{manifest, totalBytes}` for capacity planning; `close()` releases the SDK client.

File uploads buffer at most one 16 MiB part, support up to 10,000 parts (over 150 GiB), and use conditional completion. Failed multipart uploads are aborted; lifecycle cleanup covers interrupted processes. Retried release uploads accept existing objects only after reading and verifying their exact bytes. SHA-256 is calculated independently of provider ETags, which are opaque concurrency tokens, not content checksums. Publication verifies remote bytes before changing the pointer; an interrupted upload never becomes current. Stale publishers can leave immutable unreferenced objects, which must be retained or cleaned through an operator-managed lifecycle rather than by deleting live releases.

## Vendor references and validation limits

- [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Enforcing conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)
- [AWS SDK S3 client and multipart operations](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/s3/)

Tests use an in-memory S3 protocol double and real SQLite fixtures. They do not prove a particular vendor's conditional-write or multipart behavior, network performance, live permissions, or 4.3 GiB transfer throughput. Before enabling publication, exercise initial creation, stale ETag rejection, interrupted multipart cleanup and a full archive upload/download against the selected private provider.
