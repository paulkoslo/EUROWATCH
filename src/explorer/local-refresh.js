const express = require("express");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { constants } = require("node:fs");
const { randomUUID } = require("node:crypto");
const { once } = require("node:events");
const { createApp } = require("./app");
const { currentDirectory, loadDataset, prepare } = require("../data/dataset");
const { importSittings, lastClosedDate } = require("../data/plenary-import");
const { close, get, open, run } = require("../data/sqlite");

async function createLocalPreview({ directory, start, end, request }) {
  let dataset = await loadDataset(directory);
  let explorer = createApp(dataset);
  const opened = [dataset];
  const { toDate } = dataset.release.stats;
  const from = dataset.release.subsetFrom;
  start ||= new Date(Date.parse(toDate) + 86400000).toISOString().slice(0, 10);
  let workspace;
  let stagingReady = false;
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
    if (!end && start > lastClosedDate()) {
      status.message = "No new sitting dates to check yet.";
      return;
    }
    workspace ??= await fs.mkdtemp(
      path.join(os.tmpdir(), "eurowatch-refresh-"),
    );
    const filename = path.join(workspace, "staging.db");
    if (!stagingReady) {
      await fs.copyFile(
        path.join(dataset.directory, "ep_data.db"),
        filename,
        constants.COPYFILE_FICLONE,
      );
      await fs.chmod(filename, 0o600);
      const staging = await open(filename);
      try {
        await run(
          staging,
          "DELETE FROM dataset_metadata WHERE key = 'release'",
        );
      } finally {
        await close(staging);
      }
      stagingReady = true;
    }
    status.message = "Checking Parliament’s sitting calendar…";
    const result = await importSittings({
      start,
      end,
      filename,
      request,
      log(message) {
        status.message = message;
      },
    });
    const staging = await open(filename, { readonly: true });
    let speechCount;
    try {
      speechCount = (
        await get(staging, "SELECT COUNT(*) AS n FROM individual_speeches")
      ).n;
    } finally {
      await close(staging);
    }
    const added = speechCount - dataset.release.stats.speeches;
    if (added > 0) {
      status.message = "Preparing and verifying the refreshed preview…";
      const output = path.join(workspace, randomUUID());
      await prepare({ source: filename, output, from, log() {} });
      const updated = await loadDataset(output, { verifyHashes: true });
      // Requests already running can finish against their original snapshot.
      opened.push(updated);
      explorer = createApp(updated);
      dataset = updated;
      status.datasetId = dataset.release.id;
    }
    status.message =
      added > 0
        ? `Added ${added.toLocaleString("en-GB")} speeches to the preview.`
        : "No new speeches were added.";
    if (result.pending || result.failed)
      status.message += ` ${result.pending} sittings pending, ${result.failed} failed. Check again to retry.`;
  }

  return {
    app,
    async close() {
      await work;
      await Promise.all(opened.map((snapshot) => snapshot.close()));
      if (workspace) await fs.rm(workspace, { recursive: true, force: true });
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
