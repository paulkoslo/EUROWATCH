const fs = require("node:fs/promises");
const path = require("node:path");
const lockfile = require("proper-lockfile");
const { setTimeout: delay } = require("node:timers/promises");
const { createReleaseStorageFromEnv } = require("../data/release-storage");
const { hashFile } = require("../data/dataset");
const { open, all, close } = require("../data/sqlite");
const { hash } = require("./model");
const { corpusBackfill } = require("./corpus-backfill");
const { durableWrite } = require("./backfill");
const { options, read, providerFor } = require("./cli");

async function readState(filename) {
  try {
    return JSON.parse(await fs.readFile(filename, "utf8"));
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    return {};
  }
}

async function recentWindow(filename, manifest, prior, lookbackDays) {
  const db = await open(filename, { readonly: true });
  try {
    const rows = await all(
      db,
      `
      SELECT s.id, s.activity_date AS date, r.official_id AS officialId,
        v.format, v.payload_sha256 AS sha256
      FROM sitting_provenance p JOIN sittings s ON s.id=p.sitting_id
      JOIN source_revisions v ON v.id=p.revision_id
      JOIN source_records r ON r.id=v.source_id WHERE r.provider='europarl'
      ORDER BY s.activity_date,s.id
    `,
    );
    const revisions = Object.fromEntries(
      rows.map((row) => [
        row.id,
        hash([row.officialId, row.format, row.sha256]),
      ]),
    );
    const latest = manifest.stats.toDate;
    const from = prior?.revisions
      ? rows.find((row) => prior.revisions[row.id] !== revisions[row.id])?.date
      : new Date(Date.parse(latest) - (lookbackDays - 1) * 86400_000)
          .toISOString()
          .slice(0, 10);
    return { from, revisions };
  } finally {
    await close(db);
  }
}

function createRecentEnrichment({
  storage,
  workspace,
  config,
  provider,
  budgetUsd,
  maxItems = 100,
  lookbackDays = 14,
  retryFailed = false,
  kinds = ["debate", "sitting"],
  onEvent = () => {},
  backfill = corpusBackfill,
}) {
  if (
    !workspace ||
    !config ||
    !Number.isInteger(lookbackDays) ||
    lookbackDays < 1 ||
    lookbackDays > 90 ||
    !Number.isInteger(maxItems) ||
    maxItems < 1
  )
    throw new Error(
      "Configure a persistent workspace, enrichment settings and bounded input.",
    );
  const scope = hash({
    config,
    kinds,
    lookbackDays,
    provider: provider?.kind || (provider ? "custom" : "none"),
  });
  const root = path.resolve(workspace);
  const filename = path.join(root, "archive.db");
  const stateFile = path.join(root, "recent-state.json");
  const lock = path.join(root, "recent-writer.lock");
  let verified;
  return async function checkForEnrichment() {
    await fs.mkdir(root, { recursive: true, mode: 0o700 });
    const unlock = await lockfile.lock(root, {
      lockfilePath: lock,
      stale: 120_000,
    });
    try {
      const state = await readState(stateFile);
      const current = await storage.readCurrent();
      if (!current?.value?.checkpoint)
        throw new Error("No verified canonical checkpoint is published.");
      const { id, checkpoint } = current.value;
      const prior = state.completed?.scope === scope ? state.completed : null;
      if (prior?.id === id && !(retryFailed && state.retryFrom))
        return { id, status: "unchanged" };
      const { manifest } = await storage.inspectRelease(current.value);
      if (
        verified !== checkpoint.sha256 &&
        state.downloaded?.sha256 === checkpoint.sha256
      ) {
        const checksum = await hashFile(filename).catch((error) => {
          if (error.code !== "ENOENT") throw error;
          return null;
        });
        if (checksum === checkpoint.sha256) verified = checksum;
      }
      if (verified !== checkpoint.sha256) {
        const disk = await fs.statfs(root, { bigint: true });
        if (disk.bavail * disk.bsize < BigInt(checkpoint.bytes) + 536_870_912n)
          throw new Error(
            "The enrichment workspace needs room for a new canonical checkpoint.",
          );
        await storage.downloadCheckpoint(checkpoint, filename);
        verified = checkpoint.sha256;
        state.downloaded = { id, sha256: checkpoint.sha256 };
        durableWrite(stateFile, state);
      }

      const { from: changedFrom, revisions } = await recentWindow(
        filename,
        manifest,
        prior,
        lookbackDays,
      );
      const from = [
        changedFrom,
        state.pendingFrom,
        retryFailed && state.retryFrom,
      ]
        .filter(Boolean)
        .sort()[0];
      const to = manifest.stats.toDate;
      let retryable = false;
      const emit = (event) => onEvent({ releaseId: id, ...event });
      emit({ type: "candidate_check", from, to });
      if (from) {
        const report = await backfill({
          filename,
          checkpoint: path.join(root, "candidates"),
          config,
          provider,
          budgetUsd,
          maxItems,
          retryFailed,
          from,
          to,
          kinds,
          onEvent: emit,
        });
        retryable = report.retryable;
        if (retryable)
          state.retryFrom = [state.retryFrom, from].filter(Boolean).sort()[0];
        if (
          report.spendHalted ||
          report.incomplete ||
          report.examinedWindows < report.windows
        ) {
          state.pendingFrom = from;
          durableWrite(stateFile, state);
          return {
            id,
            status: report.spendHalted ? "spend_halted" : "pending",
            from,
            to,
          };
        }
        if (retryFailed && !retryable) delete state.retryFrom;
      }
      delete state.pendingFrom;
      state.completed = { id, scope, revisions };
      durableWrite(stateFile, state);
      return {
        id,
        status: retryable ? "retry_available" : from ? "prepared" : "unchanged",
        from,
        to,
      };
    } finally {
      await unlock();
    }
  };
}

async function main() {
  require("dotenv").config({ quiet: true });
  const opts = options(process.argv.slice(2));
  if (!opts.config || !opts.workspace)
    throw new Error("Use --config and --workspace on a persistent disk.");
  const config = read(opts.config);
  if (opts.watch === "true" && opts["retry-failed"] === "true")
    throw new Error("Retry failed attempts explicitly in a single run.");

  const storage = createReleaseStorageFromEnv();
  if (!storage)
    throw new Error("Configure shared release storage for enrichment.");
  const interval = Number(opts.interval || 900);
  if (!Number.isInteger(interval) || interval < 60 || interval > 86400)
    throw new Error("Use an interval of 60–86,400 seconds.");
  const signal = new AbortController();
  const stop = () => signal.abort();
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  const log = (event) =>
    console.log(
      JSON.stringify({
        stage: "enrichment",
        time: new Date().toISOString(),
        ...event,
      }),
    );
  try {
    const check = createRecentEnrichment({
      storage,
      workspace: opts.workspace,
      config,
      provider: providerFor(opts, config),
      budgetUsd: Number(opts.budget || 0),
      maxItems: Number(opts["max-items"] || 100),
      lookbackDays: Number(opts["lookback-days"] || 14),
      retryFailed: opts["retry-failed"] === "true",
      kinds: opts.kinds?.split(","),
      onEvent: log,
    });
    do {
      try {
        log(await check());
      } catch (error) {
        if (opts.watch !== "true") throw error;
        log({ status: "failed", message: error.message });
      }
      if (opts.watch === "true")
        await delay(interval * 1000, undefined, {
          signal: signal.signal,
        }).catch((error) => {
          if (error.name !== "AbortError") throw error;
        });
    } while (opts.watch === "true" && !signal.signal.aborted);
  } finally {
    storage.close();
  }
}
if (require.main === module)
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
module.exports = { createRecentEnrichment };
