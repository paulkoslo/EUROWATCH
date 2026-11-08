const fs = require("node:fs/promises");
const path = require("node:path");
const lockfile = require("proper-lockfile");
const { currentDirectory, prepare, promote } = require("./dataset");

const reserveBytes = 512n * 1024n * 1024n;

async function existingDirectory(root) {
  try {
    return await currentDirectory(root);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    return null;
  }
}

async function bootstrapArchive(root, log) {
  const data = path.join(root, "data");
  const pointer = path.join(data, "current.json");
  const source = path.join(data, "ep_data.db");
  const releases = path.join(data, "releases");
  const output = path.join(releases, "render-bootstrap");
  const staging = path.join(releases, ".render-bootstrap-preparing");
  const archive = await fs.stat(source, { bigint: true }).catch((error) => {
    if (error.code !== "ENOENT") throw error;
    throw new Error(
      `No prepared release is selected, and the full Render archive is missing at ${source}.`,
      { cause: error },
    );
  });
  if (!archive.isFile() || archive.size === 0n)
    throw new Error(
      `The Render archive at ${source} must be a nonempty SQLite file.`,
    );

  const unlock = await lockfile.lock(data, {
    lockfilePath: path.join(data, "render-bootstrap.lock"),
    stale: 20_000,
    retries: { retries: 48, factor: 1, minTimeout: 2500 },
  });
  try {
    const selected = await existingDirectory(root);
    if (selected) return selected;
    const existing = await fs.stat(output).catch((error) => {
      if (error.code !== "ENOENT") throw error;
      return null;
    });
    if (!existing) {
      // A killed startup may leave a partial copy here. Only the writer lock
      // holder can remove this reserved staging directory.
      await fs.rm(staging, { recursive: true, force: true });
      const space = await fs.statfs(data, { bigint: true });
      if (space.bavail * space.bsize < archive.size + reserveBytes)
        throw new Error(
          "Render archive bootstrap needs free disk space for one full archive copy plus 512 MiB of working space.",
        );
      log(
        `Preparing the existing Render archive at ${source}. This runs once.`,
      );
      try {
        await prepare({ source, output: staging, log });
        await fs.rename(staging, output);
      } finally {
        await fs.rm(staging, { recursive: true, force: true });
      }
    }
    let id;
    try {
      id = await promote(output, pointer, { replace: false });
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      const selected = await existingDirectory(root);
      if (selected) return selected;
      throw error;
    }
    log(`Verified and selected Render release ${id} at ${output}.`);
    return output;
  } finally {
    await unlock();
  }
}

async function resolveStartupDataset(
  root,
  env = process.env,
  log = console.log,
) {
  if (env.DATASET_DIR) return env.DATASET_DIR;
  if (env.RENDER !== "true") return currentDirectory(root);
  return (await existingDirectory(root)) ?? bootstrapArchive(root, log);
}

module.exports = { resolveStartupDataset };
