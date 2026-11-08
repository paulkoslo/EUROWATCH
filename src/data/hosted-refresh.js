const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const { parseArgs } = require("node:util");
const { loadDataset, prepare } = require("./dataset");
const { canonicalVersion } = require("./canonical-schema");
const { archiveSnapshot } = require("./archive-migration");
const { close, open } = require("./sqlite");
const { syncMepRoster } = require("./mep-roster");
const { importSittings, isDate, lastClosedDate } = require("./plenary-import");
const { createReleaseStorageFromEnv } = require("./release-storage");

const LEASE_MS = 120_000;
const HISTORY_DAYS = 14;
const MAX_EVENTS = 200;
const today = () => new Date().toISOString();
const shift = (date, days) =>
  new Date(Date.parse(date) + days * 86_400_000).toISOString().slice(0, 10);

async function claimLease(storage, runId) {
  const current = await storage.readLease();
  if (
    current &&
    (current.value.runId !== runId || current.value.phase !== "queued") &&
    Date.parse(current.value.expiresAt) > Date.now()
  )
    return null;
  const renewal = () => ({
    runId,
    expiresAt: new Date(Date.now() + LEASE_MS).toISOString(),
  });
  let lease;
  try {
    lease = await storage.writeLease(renewal(), {
      expectedETag: current?.etag ?? null,
    });
  } catch (error) {
    if (error.code === "RELEASE_CONFLICT") return null;
    throw error;
  }
  let error;
  let pending = Promise.resolve();
  function renew() {
    pending = pending
      .then(async () => {
        if (error || Date.parse(lease.value.expiresAt) <= Date.now())
          throw new Error("The update lost its writer lease.", {
            cause: error,
          });
        lease = await storage.writeLease(renewal(), {
          expectedETag: lease.etag,
        });
      })
      .catch((cause) => {
        error = cause;
      });
    return pending;
  }
  const timer = setInterval(renew, LEASE_MS / 4);
  timer.unref();
  return {
    async assertHeld() {
      await renew();
      if (
        error ||
        Date.parse(lease.value.expiresAt) - Date.now() < LEASE_MS / 2
      )
        throw new Error("The update lost its writer lease.", { cause: error });
    },
    async close() {
      clearInterval(timer);
      await pending;
      if (!error) {
        await storage.writeLease(
          { runId, expiresAt: new Date(0).toISOString() },
          {
            expectedETag: lease.etag,
          },
        );
      }
    },
  };
}

function runReporter(storage, report, log, previous) {
  let etag = previous?.etag ?? null;
  let pending = Promise.resolve();
  let error;
  return {
    event(stage, message, details = {}) {
      const event = {
        runId: report.runId,
        time: today(),
        stage,
        message,
        ...details,
      };
      report.events.push(event);
      if (report.events.length > MAX_EVENTS) report.events.shift();
      log(JSON.stringify(event));
    },
    persist() {
      const snapshot = structuredClone(report);
      pending = pending
        .then(async () => {
          if (error) return;
          etag = (
            await storage.writeRunReport(report.runId, snapshot, {
              expectedETag: etag,
            })
          ).etag;
        })
        .catch((cause) => {
          error = cause;
        });
      return pending;
    },
    async flush() {
      await pending;
      if (error) throw error;
    },
  };
}

async function verifySeed(directory, checkpoint) {
  const db = await open(checkpoint, { readonly: true });
  let release;
  try {
    if (!(await canonicalVersion(db)))
      throw new Error("The seed checkpoint must come from data:migrate.");
    release = await loadDataset(directory, { verifyHashes: true });
    if (release.release.subsetFrom)
      throw new Error("The hosted seed requires a full public release.");
    const original = await archiveSnapshot(db);
    const published = await archiveSnapshot(release.db);
    if (JSON.stringify(original) !== JSON.stringify(published))
      throw new Error(
        "The seed checkpoint and public release contain different archive data.",
      );
  } finally {
    try {
      await release?.close();
    } finally {
      await close(db);
    }
  }
}

async function seedHosted({ storage, directory, checkpoint }) {
  const lease = await claimLease(storage, randomUUID());
  if (!lease) throw new Error("Another data update is running.");
  try {
    if (await storage.readCurrent())
      throw new Error("Release storage is already seeded.");
    await verifySeed(directory, checkpoint);
    const [release, privateRef] = await Promise.all([
      storage.uploadRelease(directory),
      storage.uploadCheckpoint(checkpoint, randomUUID()),
    ]);
    return await storage.publishRelease(
      { ...release, checkpoint: privateRef },
      { expectedETag: null, beforeCommit: lease.assertHeld },
    );
  } finally {
    await lease.close();
  }
}

async function runHosted({
  storage,
  trigger = "scheduled",
  runId = randomUUID(),
  retryRunId,
  start,
  end = lastClosedDate(),
  request,
  importData = importSittings,
  rosterSync = syncMepRoster,
  workRoot = os.tmpdir(),
  log = console.log,
  sourcePreference = "docx",
}) {
  if (
    !isDate(end) ||
    end > lastClosedDate() ||
    (start && (!isDate(start) || start > end))
  )
    throw new Error("Invalid update date range.");
  const queued =
    trigger === "scheduled" ? null : await storage.readRunReport(runId);
  if (queued && queued.value.status !== "queued")
    throw new Error("This run ID has already started.");
  const report = {
    ...queued?.value,
    runId,
    trigger,
    retryRunId,
    startedAt: today(),
    status: "running",
    events: [],
    failedDates: [],
    pendingDates: [],
  };
  const reporter = runReporter(storage, report, log, queued);
  const event = (stage, message, details) => {
    reporter.event(stage, message, details);
    return reporter.persist();
  };
  let lease;
  let work;
  try {
    await event("start", "Starting Parliament data update.");
    lease = await claimLease(storage, runId);
    if (!lease) {
      report.status = "busy";
      await event("complete", "Another update already holds the writer lease.");
      return report;
    }
    const current = await storage.readCurrent();
    if (!current?.value?.checkpoint)
      throw new Error("Seed the release storage before running updates.");
    const { manifest } = await storage.inspectRelease(current.value);
    const cursor = await storage.readPendingDates();
    const previouslyPending = [
      ...new Set([
        ...(cursor?.value?.dates ?? []),
        ...(manifest.refresh?.pendingDates ?? []),
      ]),
    ].filter(isDate);
    report.previousReleaseId = manifest.id;
    report.releaseId = manifest.id;
    let dates;
    if (retryRunId) {
      const previous = (await storage.readRunReport(retryRunId))?.value;
      if (!previous) throw new Error("The requested run report has expired.");
      if (
        ["failed", "interrupted", "running"].includes(previous.status) &&
        isDate(previous.start) &&
        isDate(previous.end)
      ) {
        // A failed run never published its successful writes. Replay every
        // input from that run so a later sitting cannot hide older discarded work.
        ({ start, end, targetDates: dates } = previous);
        if (end > lastClosedDate())
          throw new Error("The retry extends beyond the closed calendar.");
      } else {
        dates = [
          ...new Set([...previous.failedDates, ...previous.pendingDates]),
        ]
          .filter(isDate)
          .sort();
        if (!dates.length)
          throw new Error("That run has no recorded sitting dates to retry.");
        if (dates.at(-1) > end)
          throw new Error(
            "The retry contains dates beyond the closed calendar.",
          );
        start = dates[0];
        end = dates.at(-1);
      }
    } else {
      start ||= [
        shift(manifest.stats.toDate, -HISTORY_DAYS),
        ...previouslyPending,
      ]
        .filter(isDate)
        .sort()[0];
    }
    report.targetDates = dates;
    report.start = start;
    report.end = end;
    if (start > end) {
      report.status = "unchanged";
      await event("complete", "No closed sitting dates to check.");
      return report;
    }

    await fs.mkdir(workRoot, { recursive: true, mode: 0o700 });
    work = await fs.mkdtemp(path.join(workRoot, "eurowatch-update-"));
    const available = await fs.statfs(work, { bigint: true });
    if (
      available.bavail * available.bsize <
      BigInt(current.value.checkpoint.bytes) * 2n + 536_870_912n
    )
      throw new Error(
        "The update workspace needs space for the private checkpoint and public SQLite release.",
      );
    const filename = path.join(work, "archive.db");
    await event(
      "checkpoint",
      "Downloading and verifying the private checkpoint.",
    );
    await storage.downloadCheckpoint(current.value.checkpoint, filename);
    await event("import", "Checking official Parliament sitting records.");
    const result = await importData({
      start,
      end,
      dates,
      filename,
      request,
      updateRevisions: true,
      sourcePreference,
      log: (message) =>
        log(JSON.stringify({ runId, time: today(), stage: "source", message })),
      onEvent(item) {
        if (item.stage === "sitting" && item.retryable) {
          const dates =
            item.outcome === "pending_publication"
              ? report.pendingDates
              : report.failedDates;
          dates.push(item.date);
        }
        const message =
          item.stage === "sitting"
            ? `${item.date} ${item.outcome}`
            : `Calendar ${item.outcome}`;
        event(item.stage, message, { item });
      },
    });
    report.result = {
      imported: result.imported,
      revised: result.revised,
      existing: result.existing,
      pending: result.pending,
      failed: result.failed,
    };
    await reporter.flush();
    if (result.failed)
      throw new Error(
        `${result.failed} sitting records failed. The current release is still active.`,
      );
    const resolved = new Set(
      result.items
        .filter((item) =>
          ["existing", "imported", "revised"].includes(item.outcome),
        )
        .map((item) => item.date),
    );
    const pendingDates = [
      ...new Set([
        ...previouslyPending.filter((date) => !resolved.has(date)),
        ...report.pendingDates,
      ]),
    ].sort();
    async function savePending(next) {
      if (JSON.stringify(cursor?.value.dates ?? []) !== JSON.stringify(next)) {
        await lease.assertHeld();
        await storage.writePendingDates(
          { dates: next },
          { expectedETag: cursor?.etag ?? null },
        );
      }
    }
    let rosterChanged = false;
    if (rosterSync) {
      try {
        const roster = await rosterSync(filename, {
          date: end,
          dates: result.items
            .filter((item) => ["imported", "revised"].includes(item.outcome))
            .map((item) => item.date),
          onEvent: (item) =>
            reporter.event("roster", `MEP ${item.personId} ${item.outcome}`, {
              item,
            }),
        });
        rosterChanged = Boolean(
          roster.inserted || roster.changed || roster.linkedSpeeches,
        );
        await event(
          "roster",
          "Checked the official MEP roster and dated memberships.",
          { roster },
        );
      } catch {
        await event(
          "roster",
          "The official roster could not be synchronized. Transcript data was verified separately.",
        );
      }
    }
    if (!result.imported && !result.revised && !rosterChanged) {
      await savePending(pendingDates);
      report.status = "unchanged";
      await event(
        "complete",
        "No changed records. The current release is still active.",
      );
      return report;
    }
    const output = path.join(work, "release");
    await event("prepare", "Rebuilding the public archive and analytics.");
    await prepare({
      source: filename,
      output,
      refresh: { completedAt: today(), pendingDates, failedDates: [] },
      log: (message) => reporter.event("prepare", message),
    });
    await event(
      "upload",
      "Verifying and uploading the release and its checkpoint.",
    );
    const publicRef = await storage.uploadRelease(output);
    const checkpoint = await storage.uploadCheckpoint(filename, runId);
    await reporter.flush();
    await storage.publishRelease(
      { ...publicRef, checkpoint },
      { expectedETag: current.etag, beforeCommit: lease.assertHeld },
    );
    report.releaseId = publicRef.id;
    report.status = "published";
    try {
      await savePending(pendingDates);
    } catch {
      // The manifest carries additions and a stale private cursor only repeats
      // extra work. Do not describe a committed release as a failed publication.
      reporter.event(
        "cursor",
        "The pending-date cursor will be reconciled on the next run.",
      );
    }
    await event("complete", `Published release ${publicRef.id}.`);
    return report;
  } catch (error) {
    report.status = "failed";
    await event("failed", error.message);
    throw error;
  } finally {
    report.finishedAt = today();
    try {
      await reporter.persist();
      await reporter.flush();
    } finally {
      try {
        await lease?.close();
      } finally {
        if (work) await fs.rm(work, { recursive: true, force: true });
      }
    }
  }
}

async function main() {
  require("dotenv").config({ quiet: true });
  const { values } = parseArgs({
    options: {
      seed: { type: "boolean" },
      dataset: { type: "string" },
      checkpoint: { type: "string" },
      trigger: { type: "string" },
      "run-id": { type: "string" },
      retry: { type: "string" },
      start: { type: "string" },
      end: { type: "string" },
    },
  });
  const storage = createReleaseStorageFromEnv();
  if (!storage)
    throw new Error("Configure RELEASE_BUCKET for hosted data updates.");
  try {
    if (values.seed) {
      if (!values.dataset || !values.checkpoint)
        throw new Error(
          "Supply --dataset and --checkpoint for the one-time seed.",
        );
      const result = await seedHosted({
        storage,
        directory: values.dataset,
        checkpoint: values.checkpoint,
      });
      console.log(JSON.stringify({ releaseId: result.value.id }));
    } else {
      if (
        values.trigger &&
        !["manual", "scheduled", "retry"].includes(values.trigger)
      )
        throw new Error("Unknown update trigger.");
      await runHosted({
        storage,
        trigger: values.trigger,
        runId: values["run-id"],
        retryRunId: values.retry,
        start: values.start,
        end: values.end,
        workRoot: process.env.DATA_UPDATE_WORK_DIR,
        sourcePreference: process.env.PLENARY_SOURCE || "docx",
      });
    }
  } finally {
    storage.close();
  }
}

if (require.main === module)
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });

module.exports = { claimLease, runHosted, seedHosted };
