const fs = require("node:fs/promises");
const { appendFileSync, constants } = require("node:fs");
const path = require("node:path");
const { createHash, randomUUID } = require("node:crypto");
const { parseArgs } = require("node:util");
const lockfile = require("proper-lockfile");
const {
  currentDirectory,
  directoryFromPointer,
  loadDataset,
  prepare,
  promote,
} = require("./dataset");
const { importSittings, lastClosedDate } = require("./plenary-import");
const { close, get, open, run } = require("./sqlite");

const project = path.resolve(__dirname, "../..");

function refreshWorkspace(root, datasetId) {
  const key = createHash("sha256").update(datasetId).digest("hex");
  return path.resolve(
    root || path.join(project, "data", "refresh-preview"),
    key,
  );
}

async function refreshDirectory(workspace, fallback) {
  try {
    return await directoryFromPointer(path.join(workspace, "current.json"));
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    return fallback;
  }
}

async function initializeStaging(directory, filename, runId) {
  try {
    await fs.access(filename);
    return false;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const temporary = `${filename}.${runId}`;
  try {
    await fs.copyFile(
      path.join(directory, "ep_data.db"),
      temporary,
      constants.COPYFILE_FICLONE,
    );
    await fs.chmod(temporary, 0o600);
    const staging = await open(temporary);
    try {
      await run(staging, "DELETE FROM dataset_metadata WHERE key = 'release'");
    } finally {
      await close(staging);
    }
    await fs.rename(temporary, filename);
    return true;
  } finally {
    await fs.rm(temporary, { force: true });
  }
}

async function runRefresh({
  directory,
  workspaceRoot,
  start,
  end,
  request,
  onEvent = () => {},
}) {
  const baseline = await loadDataset(directory);
  const workspace = refreshWorkspace(workspaceRoot, baseline.release.id);
  const runId = randomUUID();
  const logs = path.join(workspace, "runs");
  const logFile = path.join(logs, `${runId}.jsonl`);
  const emit = (stage, message, details = {}) => {
    const event = {
      runId,
      time: new Date().toISOString(),
      stage,
      message,
      ...details,
    };
    appendFileSync(logFile, JSON.stringify(event) + "\n", { mode: 0o600 });
    onEvent(event);
    return event;
  };

  let unlock;
  let current;
  try {
    await fs.mkdir(logs, { recursive: true, mode: 0o700 });
    start ||= new Date(Date.parse(baseline.release.stats.toDate) + 86400000)
      .toISOString()
      .slice(0, 10);
    emit("start", "Starting a local data refresh.", {
      start,
      end: end || lastClosedDate(),
      baselineId: baseline.release.id,
      logFile,
    });
    try {
      unlock = await lockfile.lock(workspace, { stale: 120000 });
    } catch (error) {
      if (error.code === "ELOCKED")
        throw new Error("A refresh is already running for this dataset.", {
          cause: error,
        });
      throw error;
    }
    const selected = await refreshDirectory(workspace, baseline.directory);
    current =
      selected === baseline.directory ? baseline : await loadDataset(selected);
    if (!end && start > lastClosedDate())
      return emit("complete", "No new sitting dates to check yet.", {
        directory: current.directory,
        datasetId: current.release.id,
        added: 0,
        outcome: "complete",
      });
    const filename = path.join(workspace, "staging.db");
    emit("staging", "Checking the local staging database.");
    const created = await initializeStaging(
      baseline.directory,
      filename,
      runId,
    );
    emit(
      "staging",
      created
        ? "Created staging from the baseline."
        : "Resuming the existing staging database.",
    );
    emit("import", "Checking Parliament’s sitting calendar…");
    const result = await importSittings({
      start,
      end,
      filename,
      request,
      log: (message) => emit("import", message),
    });
    const staging = await open(filename, { readonly: true });
    let count;
    try {
      const from = baseline.release.subsetFrom;
      count = from
        ? (
            await get(
              staging,
              "SELECT COUNT(*) AS n FROM individual_speeches i JOIN sittings s ON s.id = i.sitting_id WHERE s.activity_date >= ?",
              [from],
            )
          ).n
        : (await get(staging, "SELECT COUNT(*) AS n FROM individual_speeches"))
            .n;
    } finally {
      await close(staging);
    }
    const added = count - current.release.stats.speeches;
    let output = current.directory;
    let datasetId = current.release.id;
    if (added > 0) {
      output = path.join(workspace, "releases", runId);
      emit("prepare", "Preparing the refreshed preview…");
      await prepare({
        source: filename,
        output,
        from: baseline.release.subsetFrom,
        log: (message) => emit("prepare", message),
      });
      emit("validate", "Verifying the refreshed preview…");
      datasetId = await promote(output, path.join(workspace, "current.json"));
    }
    let message =
      added > 0
        ? `Added ${added.toLocaleString("en-GB")} speeches to the preview.`
        : "No new speeches were added.";
    const partial = result.pending > 0 || result.failed > 0;
    if (partial)
      message += ` ${result.pending} sittings pending, ${result.failed} failed. Check again to retry.`;
    return emit("complete", message, {
      directory: output,
      datasetId,
      added,
      outcome: partial ? "partial" : "complete",
      result,
    });
  } catch (error) {
    // A workspace creation error can prevent even the failure record from being saved.
    try {
      emit("failed", error.message);
    } catch {
      // Preserve the error that caused the refresh to fail.
    }
    throw error;
  } finally {
    try {
      if (current && current !== baseline) await current.close();
    } finally {
      try {
        await baseline.close();
      } finally {
        if (unlock) await unlock();
      }
    }
  }
}

async function main() {
  const { values } = parseArgs({
    options: {
      dataset: { type: "string" },
      workspace: { type: "string" },
      start: { type: "string" },
      end: { type: "string" },
    },
  });
  const result = await runRefresh({
    directory:
      values.dataset ||
      process.env.DATASET_DIR ||
      (await currentDirectory(project)),
    workspaceRoot: values.workspace || process.env.LOCAL_REFRESH_WORKSPACE,
    start: values.start || process.env.LOCAL_REFRESH_START,
    end: values.end || process.env.LOCAL_REFRESH_END,
    onEvent(event) {
      console.log(JSON.stringify(event));
      if (process.connected) process.send(event);
    },
  });
  if (result.outcome === "partial") process.exitCode = 1;
}

if (require.main === module)
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });

module.exports = { runRefresh, refreshWorkspace, refreshDirectory };
