const fs = require("node:fs/promises");
const path = require("node:path");
const { open, close, all } = require("../data/sqlite");
const { hash } = require("./model");
const { streamCorpus } = require("./corpus");
const { backfill, durableWrite } = require("./backfill");
const { options, read, write, providerFor } = require("./cli");

async function corpusBackfill({
  filename,
  checkpoint,
  config,
  provider,
  budgetUsd = 0,
  maxItems = 100,
  retryFailed = false,
  from,
  to,
  kinds,
  officialEvidence = [],
  onEvent = () => {},
}) {
  if (!filename || !checkpoint || !Number.isInteger(maxItems) || maxItems < 1)
    throw new Error(
      "An archive, checkpoint and positive batch size are required.",
    );
  await fs.mkdir(checkpoint, { recursive: true, mode: 0o700 });
  const lock = path.join(checkpoint, "corpus-writer.lock");
  await fs.mkdir(lock);
  try {
    durableWrite(path.join(lock, "owner.json"), {
      pid: process.pid,
      startedAt: new Date().toISOString(),
    });
    const identity = async () => {
      const wal = await fs.stat(`${filename}-wal`).catch((error) => {
        if (error.code !== "ENOENT") throw error;
      });
      if (wal?.size)
        throw new Error(
          "Backfill needs a checkpointed archive with no pending SQLite WAL.",
        );
      const stat = await fs.stat(filename, { bigint: true });
      return [stat.dev, stat.ino, stat.size, stat.mtimeNs, stat.ctimeNs]
        .map(String)
        .join(":");
    };
    const source = await identity();
    const db = await open(filename, { readonly: true });
    let windows;
    try {
      windows = await all(
        db,
        `
        SELECT substr(activity_date,1,7) AS month, MIN(activity_date) AS start, MAX(activity_date) AS end
        FROM sittings WHERE activity_date IS NOT NULL AND (? IS NULL OR activity_date>=?) AND (? IS NULL OR activity_date<=?)
        GROUP BY month ORDER BY month
      `,
        [from || null, from || null, to || null, to || null],
      );
    } finally {
      await close(db);
    }
    const scope = hash({
      source,
      config,
      from,
      to,
      kinds,
      officialEvidence,
      provider: provider?.kind || (provider ? "custom" : "none"),
    });
    const directory = path.join(checkpoint, "monthly-reports", scope);
    await fs.mkdir(directory, { recursive: true });
    const stateFile = path.join(directory, "progress.json");
    let progress = {};
    try {
      progress = JSON.parse(await fs.readFile(stateFile, "utf8"));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    const aggregate = {
      windows: windows.length,
      examinedWindows: 0,
      attempts: 0,
      reused: 0,
      reservedUsd: 0,
      reportedCostUsd: 0,
      spendHalted: false,
      coverage: {},
      diagnostics: 0,
    };
    for (const window of windows) {
      let summary = progress[window.month];
      if (!summary?.complete || (retryFailed && summary.retryable)) {
        if ((await identity()) !== source)
          throw new Error(
            "Archive changed during the backfill. Restart with a stable snapshot.",
          );
        const records = [];
        let diagnostics = 0;
        for await (const batch of streamCorpus({
          filename,
          from: window.start,
          to: window.end,
          kinds,
        })) {
          for (const record of batch.records) records.push(record);
          diagnostics += batch.diagnostics.length;
        }
        if ((await identity()) !== source)
          throw new Error("Archive changed while reading a monthly batch.");
        const report = await backfill({
          records,
          config,
          provider,
          checkpoint,
          budgetUsd,
          maxItems: maxItems - aggregate.attempts,
          officialEvidence,
          retryFailed,
          onEvent,
        });
        durableWrite(path.join(directory, `${window.month}.json`), {
          ...report,
          window,
          diagnostics,
        });
        summary = {
          coverage: report.coverage,
          diagnostics,
          reservedUsd: report.reservedUsd,
          reportedCostUsd: report.reportedCostUsd,
          spendHalted: report.spendHalted,
          complete: !report.results.some((result) =>
            ["batch_limit", "budget", "usage_above_reservation"].includes(
              result.reason,
            ),
          ),
          retryable: report.results.some((result) =>
            ["failed", "interrupted"].includes(result.status),
          ),
        };
        progress[window.month] = summary;
        durableWrite(stateFile, progress);
        aggregate.attempts += report.attempts;
        aggregate.reused += report.reused;
      }
      aggregate.examinedWindows++;
      aggregate.reservedUsd = Math.max(
        aggregate.reservedUsd,
        summary.reservedUsd,
      );
      aggregate.reportedCostUsd += summary.reportedCostUsd;
      aggregate.spendHalted ||= summary.spendHalted;
      aggregate.diagnostics += summary.diagnostics;
      for (const [key, counts] of Object.entries(summary.coverage)) {
        aggregate.coverage[key] ||= {};
        for (const [status, count] of Object.entries(counts))
          aggregate.coverage[key][status] =
            (aggregate.coverage[key][status] || 0) + count;
      }
      if (
        aggregate.attempts >= maxItems ||
        aggregate.spendHalted ||
        !summary.complete
      )
        break;
    }
    if ((await identity()) !== source)
      throw new Error("Archive changed during the backfill.");
    durableWrite(path.join(checkpoint, "corpus-report.json"), aggregate);
    return aggregate;
  } finally {
    await fs.rm(lock, { recursive: true, force: true });
  }
}

if (require.main === module) {
  const opts = options(process.argv.slice(2));
  if (!opts.db || !opts.config || !opts.output)
    throw new Error("Use --db, --config, --checkpoint and --output.");
  const output = path.resolve(opts.output);
  const checkpoint = path.resolve(opts.checkpoint || ".");
  if (
    [opts.db, opts.config, opts.evidence, opts.responses]
      .filter(Boolean)
      .some((input) => path.resolve(input) === output) ||
    output === checkpoint ||
    output.startsWith(checkpoint + path.sep)
  )
    throw new Error("Choose an output outside the input files and checkpoint.");
  const config = read(opts.config);
  corpusBackfill({
    filename: opts.db,
    checkpoint: opts.checkpoint,
    config,
    officialEvidence: opts.evidence ? read(opts.evidence) : [],
    provider: providerFor(opts, config),
    budgetUsd: Number(opts.budget || 0),
    maxItems: Number(opts["max-items"] || 100),
    retryFailed: opts["retry-failed"] === "true",
    from: opts.from,
    to: opts.to,
    kinds: opts.kinds ? opts.kinds.split(",") : undefined,
    onEvent: (event) => process.stderr.write(JSON.stringify(event) + "\n"),
  })
    .then((report) => write(opts.output, report))
    .catch((error) => {
      console.error(error.message);
      process.exitCode = 1;
    });
}
module.exports = { corpusBackfill };
