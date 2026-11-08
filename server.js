require("dotenv").config({ quiet: true });
const { loadDataset, currentDirectory } = require("./src/data/dataset");
const path = require("node:path");
const { createReleaseStorageFromEnv } = require("./src/data/release-storage");
const { createReleaseManager } = require("./src/data/release-manager");
const { createApp } = require("./src/explorer/app");
const { createUpdateControlFromEnv } = require("./src/explorer/update-control");

async function start() {
  const storage = createReleaseStorageFromEnv();
  let source;
  let updateControl;
  try {
    updateControl = createUpdateControlFromEnv(storage);
  } catch (error) {
    storage?.close();
    throw error;
  }
  if (storage) {
    const fallbackDirectory =
      process.env.DATASET_DIR ||
      (await currentDirectory(__dirname).catch(() => undefined));
    try {
      source = await createReleaseManager({
        storage,
        cacheDir:
          process.env.RELEASE_CACHE_DIR ||
          path.join(__dirname, "data/release-cache"),
        fallbackDirectory,
        pollIntervalMs: Number(process.env.RELEASE_POLL_INTERVAL_MS || 60000),
        maxCacheBytes: Number(
          process.env.RELEASE_CACHE_MAX_BYTES || 16 * 1024 ** 3,
        ),
      });
    } catch (error) {
      storage.close();
      throw error;
    }
  } else {
    const directory =
      process.env.DATASET_DIR || (await currentDirectory(__dirname));
    source = await loadDataset(directory);
  }
  const app = createApp(source, {
    fullDownload: process.env.ENABLE_FULL_DATASET_DOWNLOAD !== "0",
    trustProxy: process.env.TRUST_PROXY === "1",
    updateControl,
  });
  const port = Number(process.env.PORT || 3000),
    host =
      process.env.HOST ||
      (process.env.NODE_ENV === "production" ? "0.0.0.0" : "127.0.0.1");
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    await source.close();
    throw new Error("PORT must be between 0 and 65535.");
  }
  const server = app.listen(port, host, () =>
    console.log(
      `EUROWATCH listening on http://${host}:${server.address().port} · dataset ${(source.dataset || source).release.id}`,
    ),
  );
  server.requestTimeout = 30000;
  server.headersTimeout = 10000;
  let stopping = false;
  const stop = () => {
    if (stopping) return;
    stopping = true;
    server.close(async () => {
      await source.close();
      process.exitCode = 0;
    });
    server.closeIdleConnections();
    const timeout = setTimeout(() => {
      server.closeAllConnections();
    }, 5000);
    timeout.unref();
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  server.once("error", async (error) => {
    console.error(error.message);
    await source.close();
    process.exitCode = 1;
  });
  server.once("close", () => {
    process.removeListener("SIGTERM", stop);
    process.removeListener("SIGINT", stop);
  });
  return server;
}

if (require.main === module)
  start().catch((error) => {
    console.error(
      `EUROWATCH could not start: ${error.message}\nPrepare a snapshot with npm run data:prepare, or use npm run data:fixture. See README.md.`,
    );
    process.exitCode = 1;
  });
module.exports = { start };
