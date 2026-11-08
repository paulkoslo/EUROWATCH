const express = require("express");
const { fork } = require("node:child_process");
const path = require("node:path");
const { once } = require("node:events");
const { createApp } = require("./app");
const { currentDirectory, loadDataset } = require("../data/dataset");
const {
  refreshDirectory,
  refreshWorkspace,
  runRefresh,
} = require("../data/refresh-run");

function forkRefresh({ directory, workspaceRoot, start, end, onEvent }) {
  const args = ["--dataset", directory];
  for (const [name, value] of [
    ["workspace", workspaceRoot],
    ["start", start],
    ["end", end],
  ]) {
    if (value) args.push(`--${name}`, value);
  }
  return new Promise((resolve, reject) => {
    const child = fork(require.resolve("../data/refresh-run"), args);
    let result;
    let failure;
    child.on("message", (event) => {
      onEvent(event);
      if (event.stage === "complete") result = event;
      if (event.stage === "failed") failure = event.message;
    });
    child.once("error", reject);
    child.once("close", (code, signal) => {
      if (result && code === (result.outcome === "partial" ? 1 : 0))
        return resolve(result);
      reject(
        new Error(
          failure ||
            `Refresh worker exited with ${signal || code}. Check its terminal output.`,
        ),
      );
    });
  });
}

async function createLocalPreview({
  directory,
  workspaceRoot,
  start,
  end,
  request,
}) {
  const baseline = await loadDataset(directory);
  const workspace = refreshWorkspace(workspaceRoot, baseline.release.id);
  let selected;
  try {
    selected = await refreshDirectory(workspace, baseline.directory);
  } catch (error) {
    await baseline.close();
    throw error;
  }
  let dataset = baseline;
  if (selected !== baseline.directory) {
    await baseline.close();
    dataset = await loadDataset(selected);
  }
  let explorer = createApp(dataset);
  const opened = [dataset];
  let work;
  let status = { running: false, message: "" };
  const app = express();
  app.disable("x-powered-by");
  app.use((req, res, next) => {
    const port = req.socket.localPort;
    if (![`127.0.0.1:${port}`, `localhost:${port}`].includes(req.get("host")))
      return res.sendStatus(403);
    next();
  });
  app.get("/api/localrun", (req, res) =>
    res.set("Cache-Control", "no-store").json({ localrun: true }),
  );
  app.get("/api/local-refresh", (req, res) =>
    res.set("Cache-Control", "no-store").json(status),
  );
  app.post("/api/local-refresh", (req, res) => {
    if (
      req.get("origin") !== `http://${req.get("host")}` ||
      req.get("x-eurowatch-action") !== "refresh"
    )
      return res
        .status(403)
        .json({ error: "Open the local preview to refresh." });
    if (!work) {
      status = {
        running: true,
        message: "Preparing a temporary staging copy…",
      };
      work = refresh()
        .catch((error) => {
          status.message = `Refresh failed: ${error.message}`;
        })
        .finally(() => {
          status.running = false;
          work = null;
        });
    }
    res.status(202).json(status);
  });
  app.use((req, res, next) => explorer(req, res, next));

  async function refresh() {
    const execute = request ? runRefresh : forkRefresh;
    const result = await execute({
      directory,
      workspaceRoot,
      start,
      end,
      request,
      onEvent(event) {
        status.runId = event.runId;
        if (event.stage !== "complete") status.message = event.message;
      },
    });
    if (result.datasetId !== dataset.release.id) {
      const updated = await loadDataset(result.directory);
      // Requests already running can finish against their original snapshot.
      opened.push(updated);
      explorer = createApp(updated);
      dataset = updated;
      status.datasetId = dataset.release.id;
    }
    status.message = result.message;
    status.outcome = result.outcome;
  }

  return {
    app,
    async close() {
      await work;
      await Promise.all(opened.map((snapshot) => snapshot.close()));
    },
  };
}

async function main() {
  const root = path.resolve(__dirname, "../..");
  const port = Number(process.env.PORT || 4319);
  if (!Number.isInteger(port) || port < 0 || port > 65535)
    throw new Error("PORT must be an integer between 0 and 65535.");
  const preview = await createLocalPreview({
    directory: process.env.DATASET_DIR || (await currentDirectory(root)),
    workspaceRoot: process.env.LOCAL_REFRESH_WORKSPACE,
    start: process.env.LOCAL_REFRESH_START,
    end: process.env.LOCAL_REFRESH_END,
  });
  const server = preview.app.listen(port, "127.0.0.1");
  try {
    await once(server, "listening");
  } catch (error) {
    await preview.close();
    if (error.code === "EADDRINUSE")
      throw new Error(
        `Port ${port} is already in use. Open the existing preview or run PORT=0 npm run preview:refresh for a free port.`,
        { cause: error },
      );
    throw error;
  }
  console.log(`Refresh preview at http://127.0.0.1:${server.address().port}`);
  const shutdown = () =>
    server.close(() => preview.close().catch(console.error));
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
  server.once("error", (error) => {
    console.error(error.message);
    process.exitCode = 1;
    preview.close().catch(console.error);
  });
}

if (require.main === module)
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });

module.exports = { createLocalPreview };
