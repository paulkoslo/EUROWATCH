const fs = require("node:fs");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const { hash, requestFor, resultFor, validateCandidate } = require("./model");

function durableWrite(file, data) {
  const temp = `${file}.${randomUUID()}.tmp`;
  const fd = fs.openSync(temp, "wx", 0o600);
  try {
    fs.writeFileSync(fd, JSON.stringify(data) + "\n");
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(temp, file);
  const dir = fs.openSync(path.dirname(file), "r");
  try {
    fs.fsyncSync(dir);
  } finally {
    fs.closeSync(dir);
  }
}

async function backfill({
  records,
  officialEvidence = [],
  config,
  provider,
  checkpoint,
  budgetUsd = 0,
  maxItems = 100,
  retryFailed = false,
  onEvent = () => {},
}) {
  if (
    !checkpoint ||
    !Number.isFinite(budgetUsd) ||
    budgetUsd < 0 ||
    !Number.isInteger(maxItems) ||
    maxItems < 1
  )
    throw new Error(
      "Checkpoint, nonnegative lifetime budget and positive batch size required",
    );
  const ids = new Set();
  for (const record of records) {
    if (ids.has(record.id)) throw new Error("Duplicate record ID");
    ids.add(record.id);
  }
  fs.mkdirSync(checkpoint, { recursive: true, mode: 0o700 });
  const lock = path.join(checkpoint, "writer.lock");
  try {
    fs.mkdirSync(lock);
  } catch (error) {
    if (error.code === "EEXIST")
      throw new Error(
        "Checkpoint locked; verify prior writer stopped before removing writer.lock",
      );
    throw error;
  }
  let ledgerFd;
  const runId = randomUUID();
  try {
    durableWrite(path.join(lock, "owner.json"), {
      pid: process.pid,
      runId,
      startedAt: new Date().toISOString(),
    });
    const objects = path.join(checkpoint, "objects");
    fs.mkdirSync(objects, { recursive: true });
    const ledger = path.join(checkpoint, "events.jsonl");
    // Fail closed on a damaged journal: never silently lose a prior spend reservation.
    const prior = fs.existsSync(ledger)
      ? fs
          .readFileSync(ledger, "utf8")
          .split("\n")
          .filter(Boolean)
          .map((line) => JSON.parse(line))
      : [];
    let reservedUsd = 0;
    const attempted = new Set();
    for (const event of prior)
      if (event.type === "reserved") {
        if (!Number.isFinite(event.costUsd) || event.costUsd < 0)
          throw new Error("Invalid reservation ledger");
        reservedUsd += event.costUsd;
        attempted.add(event.key);
      }
    ledgerFd = fs.openSync(ledger, "a", 0o600);
    const checkpointFd = fs.openSync(checkpoint, "r");
    try {
      fs.fsyncSync(checkpointFd);
    } finally {
      fs.closeSync(checkpointFd);
    }
    const emit = (event) => {
      const entry = { at: new Date().toISOString(), runId, ...event };
      fs.writeSync(ledgerFd, JSON.stringify(entry) + "\n");
      fs.fsyncSync(ledgerFd);
      // An operator UI/stdout failure must never lose a completed item or its reservation.
      try {
        onEvent(entry);
      } catch {
        /* durable journal remains authoritative */
      }
    };
    const results = [],
      coverage = {};
    let attempts = 0,
      reused = 0;
    for (const record of records) {
      const request = requestFor(record, config, officialEvidence);
      const file = path.join(objects, `${request.key}.json`);
      let result;
      if (fs.existsSync(file)) {
        result = JSON.parse(fs.readFileSync(file, "utf8"));
        if (result.key !== request.key || result.recordId !== record.id)
          throw new Error("Checkpoint identity mismatch");
        if (["candidate", "uncertain"].includes(result.status)) {
          validateCandidate(result.candidate, request);
          reused++;
        } else if (retryFailed) result = undefined;
      } else if (attempted.has(request.key) && !retryFailed) {
        result = {
          key: request.key,
          recordId: record.id,
          status: "interrupted",
          reason: "prior_attempt_without_result",
        };
      }
      if (!result) {
        let reason;
        const reserve = provider ? provider.reserve(request) : 0;
        if (!Number.isFinite(reserve) || reserve < 0)
          throw new Error("Invalid provider reservation");
        if (!provider) reason = "provider_disabled";
        else if (attempts >= maxItems) reason = "batch_limit";
        else if (reservedUsd + reserve > budgetUsd) reason = "budget";
        if (reason)
          result = {
            key: request.key,
            recordId: record.id,
            status: "skipped",
            reason,
          };
        else {
          // Persist the worst-case charge BEFORE any network call. A crash or an
          // invalid response retains it; retries require a new budget reservation.
          emit({
            type: "reserved",
            key: request.key,
            recordId: record.id,
            costUsd: reserve,
          });
          reservedUsd += reserve;
          attempts++;
          try {
            const response = await provider.generate(request);
            if (
              response.usage &&
              (!Number.isFinite(response.usage.costUsd) ||
                response.usage.costUsd < 0 ||
                response.usage.costUsd > reserve + 1e-9)
            )
              throw new Error("Provider usage exceeds reservation");
            result = resultFor(request, response);
          } catch {
            result = {
              key: request.key,
              recordId: record.id,
              inputRevision: record.inputRevision,
              versions: request.versions,
              status: "failed",
              reason: "provider_or_validation_failed",
              generatedAt: new Date().toISOString(),
            };
          }
          durableWrite(file, result);
          emit({
            type: "outcome",
            key: request.key,
            recordId: record.id,
            status: result.status,
          });
        }
      }
      results.push(result);
      const stratum = `${record.term || "unknown"}|${record.language || "unknown"}|${record.kind}`;
      if (!coverage[stratum])
        coverage[stratum] = {
          total: 0,
          candidate: 0,
          uncertain: 0,
          failed: 0,
          skipped: 0,
          interrupted: 0,
        };
      coverage[stratum].total++;
      coverage[stratum][result.status]++;
    }
    const report = {
      mode: "historical-backfill",
      runId,
      config,
      inputFingerprint: hash(records),
      attempts,
      reused,
      reservedUsd,
      budgetUsd,
      reportedCostUsd: results.reduce(
        (sum, r) => sum + (r.usage?.costUsd || 0),
        0,
      ),
      coverage,
      results,
      publicationAllowed: false,
    };
    durableWrite(path.join(checkpoint, "latest-report.json"), report);
    emit({
      type: "completed",
      attempts,
      reused,
      reservedUsd,
      records: records.length,
    });
    return report;
  } finally {
    if (ledgerFd !== undefined) fs.closeSync(ledgerFd);
    fs.rmSync(lock, { recursive: true });
  }
}
module.exports = { backfill, durableWrite };
