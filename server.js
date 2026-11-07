require("dotenv").config({ quiet: true });
const { loadDataset, currentDirectory } = require("./src/data/dataset");
const { createApp } = require("./src/explorer/app");

async function start() {
  const directory =
    process.env.DATASET_DIR || (await currentDirectory(__dirname));
  const dataset = await loadDataset(directory);
  const app = createApp(dataset, {
    fullDownload: process.env.ENABLE_FULL_DATASET_DOWNLOAD !== "0",
    trustProxy: process.env.TRUST_PROXY === "1",
  });
  const port = Number(process.env.PORT || 3000),
    host =
      process.env.HOST ||
      (process.env.NODE_ENV === "production" ? "0.0.0.0" : "127.0.0.1");
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    await dataset.close();
    throw new Error("PORT must be between 0 and 65535.");
  }
  const server = app.listen(port, host, () =>
    console.log(
      `EUROWATCH listening on http://${host}:${server.address().port} · dataset ${dataset.release.id}`,
    ),
  );
  server.requestTimeout = 30000;
  server.headersTimeout = 10000;
  let stopping = false;
  const stop = () => {
    if (stopping) return;
    stopping = true;
    server.close(async () => {
      await dataset.close();
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
    await dataset.close();
    process.exitCode = 1;
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
