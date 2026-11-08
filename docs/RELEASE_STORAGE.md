# Private release storage and automatic pickup

The worker and web service share a **private S3-compatible bucket**. No bucket provisioning or live configuration is performed by this code. `RELEASE_BUCKET` enables the adapter; `RELEASE_PREFIX` defaults to `eurowatch`, `RELEASE_REGION` to `us-east-1`. Set `RELEASE_ENDPOINT` for an HTTPS-compatible provider and `RELEASE_FORCE_PATH_STYLE=1` if required. The AWS SDK uses its standard credential chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`). Keep credentials in server-side environment configuration. The client bounds connection establishment to 10 seconds and request/idle waits to 120 seconds, with at most two attempts; a stalled storage service does not block fallback indefinitely.

The provider must implement conditional `PutObject` and conditional `CompleteMultipartUpload`, including `If-Match` and `If-None-Match`, and return ETags. There is no unsafe unconditional fallback. Enable bucket public-access blocking and encryption, and configure a lifecycle rule to abort incomplete multipart uploads. The application never sets public ACLs. Grant the web identity only read access to `current.json` and `releases/*`; grant the worker the additional private checkpoint/report and multipart permissions. Never serve the bucket or the cache directory through a static route.

## Worker API

`createReleaseStorageFromEnv()` in `src/data/release-storage.js` returns `null` when no bucket is configured. Otherwise:

- `readCurrent()` returns `null` or `{value, etag}`. Save this ETag **before** starting a run.
- `uploadRelease(directory)` validates the complete manifest, both database sizes, SHA-256 hashes and matching DB metadata; uploads immutable files; and returns `{id, manifestSha256}`. The manifest is uploaded last. Never alter prepared files while uploading.
- `uploadCheckpoint(sourceFile, runId)` returns `{key, sha256, bytes}`. The source must be a closed, consistent standalone SQLite copy with no uncheckpointed WAL. Checkpoints use a separate private prefix.
- `publishRelease({...release, checkpoint}, {expectedETag})` verifies remote bytes and atomically publishes both references. Pass the saved ETag, or explicit `null` when creating the first pointer. A competing update fails with `error.code === "RELEASE_CONFLICT"`; retain its report and retry from the newly published checkpoint. Never catch that failure and retry with a newly fetched ETag without rerunning the work.
- `downloadCheckpoint(reference, destination)` streams and verifies bytes before replacing the destination. `downloadRelease(reference, destination)` requires a nonexistent destination, removes partial output on failure, and validates the downloaded DBs.
- `writeRunReport(runId, report, {expectedETag})` creates or conditionally updates a private JSON report; `readRunReport(runId)` returns `{value, etag}` or `null`. Reports are limited to 1 MiB. Callers must omit secrets and transcript bodies.
- `listRunReports({limit: 50, continuationToken})` returns `{items: [{runId, lastModified}], nextToken}` latest-modified first. Each call scans at most 1,000 report objects and fails with `REPORT_SCAN_LIMIT` if the history exceeds that bound. Pagination is for an operator view, not a transactional snapshot; concurrent report updates can move rows between pages. Use bucket lifecycle retention to keep report history under 1,000 entries. Checkpoint cleanup must explicitly preserve the current and rollback references; do not expire the entire checkpoint prefix by age.
- `readLease()` / `writeLease({runId, expiresAt}, {expectedETag})` expose a private conditional lease record. The runner owns expiry, heartbeat and lost-lease handling; storage does not infer lock ownership or delete expired leases.
- `inspectRelease(reference)` returns `{manifest, totalBytes}` for capacity planning; `close()` releases the SDK client.

File uploads buffer at most one 16 MiB part, support up to 10,000 parts (over 150 GiB), and use conditional completion. Failed multipart uploads are aborted; lifecycle cleanup covers interrupted processes. Retried release uploads accept existing objects only after reading and verifying their exact bytes. SHA-256 is calculated independently of provider ETags, which are opaque concurrency tokens, not content checksums. Publication verifies remote bytes before changing the pointer; an interrupted upload never becomes current. Stale publishers can leave immutable unreferenced objects, which must be retained or cleaned through an operator-managed lifecycle rather than by deleting live releases.

## Vendor references and validation limits

- [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Enforcing conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)
- [AWS SDK S3 client and multipart operations](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/s3/)

Tests use an in-memory S3 protocol double and real SQLite fixtures. They do not prove a particular vendor's conditional-write or multipart behavior, network performance, live permissions, or 4.3 GiB transfer throughput. Before enabling publication, exercise initial creation, stale ETag rejection, interrupted multipart cleanup and a full archive upload/download against the selected private provider.

## Web pickup and disk budget

When `RELEASE_BUCKET` is set, `server.js` polls the published pointer automatically. `RELEASE_CACHE_DIR` selects a **dedicated directory owned exclusively by this one web process** (default `data/release-cache`). Do not point it at the corpus, a shared worker directory, or a directory used by another server: unreferenced cache entries are cleaned on startup. A persistent cache allows startup during a storage outage; an ephemeral cache needs the bucket available on a cold start.

`RELEASE_POLL_INTERVAL_MS` defaults to 60,000 and must be at least 1,000. `RELEASE_CACHE_MAX_BYTES` defaults to 16 GiB. The cache retains the active and prior release and stages at most one additional release. The configured byte budget must accommodate all three during a normal update, and free disk space must cover the candidate plus 64 MiB. Existing corpus/worker files outside this cache also consume disk. An insufficient budget or disk leaves the current release serving; raise capacity rather than deleting the active files.

Every fetched release is fully validated before activation. The cache pointer is atomically replaced, then new requests acquire the new database. Each request keeps its dataset, headers, queries and download tied to that release until its response and asynchronous route work end. Topic-query caches include the release ID. Old handles stay open for in-flight requests. If the previous release still has such a request, further updates wait, bounding cache growth. The next poll installs the newest pointer; intermediate releases can be skipped. A pointer that changes during download causes the candidate to be discarded and retried on a later poll.

At startup, validated cached active/previous releases provide fallback when remote retrieval fails. If neither is valid, `DATASET_DIR` (or the existing local `data/current.json` directory) can supply an explicitly configured validated fallback. Without any valid release, startup fails. The next successful poll resumes automatic pickup. With no `RELEASE_BUCKET`, existing `DATASET_DIR`/local-pointer behavior is unchanged.

Rollback publishes a retained release reference with the current ETag, and its matching private checkpoint when future updates should continue from that source. The web service picks it up on the next poll. Do not change a release directory in place. Public API responses contain database release metadata only; storage pointers, checkpoint keys and credentials stay private.

On space-constrained disks, the cache may discard the older idle rollback copy before staging a candidate. The active verified release stays available throughout. Confirm room for at least two managed releases and any seed or fallback retained on the same disk.
