const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { loadDataset } = require("./dataset");
const { validateDirectory, validateReference } = require("./release-storage");

const publicReference = ({ id, manifestSha256 }) => ({ id, manifestSha256 });
const same = (a, b) =>
  a?.id === b?.id && a?.manifestSha256 === b?.manifestSha256;

// One manager owns this dedicated cache. At most active + previous + one staged
// release exist. An older in-flight request defers the next pickup, never loses
// its handles or files, and cannot grow the retained cache without bound.
async function createReleaseManager({
  storage,
  cacheDir,
  fallbackDirectory,
  pollIntervalMs = 60000,
  maxCacheBytes = 16 * 1024 ** 3,
  logger = console,
}) {
  if (!storage || !cacheDir)
    throw new Error(
      "Storage and a dedicated release cache directory are required.",
    );
  if (
    !Number.isSafeInteger(pollIntervalMs) ||
    pollIntervalMs < 1000 ||
    !Number.isSafeInteger(maxCacheBytes) ||
    maxCacheBytes <= 0
  )
    throw new Error("Invalid release polling interval or cache size.");
  cacheDir = path.resolve(cacheDir);
  await fs.mkdir(cacheDir, { recursive: true, mode: 0o700 });
  const releasesDir = path.join(cacheDir, "releases");
  await fs.mkdir(releasesDir, { recursive: true, mode: 0o700 });
  const stateFile = path.join(cacheDir, "current.json");
  let current = null,
    previous = null,
    pending = null,
    stopped = false,
    timer;
  const records = new Set();
  const directoryFor = (ref) =>
    path.join(releasesDir, `${ref.id}-${ref.manifestSha256}`);
  async function openRecord(directory, reference, managed = true) {
    validateReference(reference);
    await validateDirectory(directory, reference);
    const dataset = await loadDataset(directory);
    const record = {
      dataset,
      reference: publicReference(reference),
      directory,
      managed,
      refs: 0,
      closed: false,
      closing: null,
    };
    records.add(record);
    return record;
  }
  async function closeRecord(record) {
    if (!record || record.refs || record === current || record.closed) return;
    record.closing ||= record.dataset.close().then(() => {
      record.closed = true;
    });
    await record.closing;
  }
  async function prune() {
    for (const record of records) {
      if (record === current || record.refs) continue;
      await closeRecord(record);
      if (record !== previous) {
        if (record.managed)
          await fs.rm(record.directory, { recursive: true, force: true });
        records.delete(record);
      }
    }
  }
  async function persist(active, rollback) {
    const temp = `${stateFile}.${crypto.randomUUID()}.tmp`;
    try {
      await fs.writeFile(
        temp,
        JSON.stringify({
          active: active.managed ? active.reference : null,
          previous: rollback?.managed ? rollback.reference : null,
        }) + "\n",
        { flag: "wx", mode: 0o600 },
      );
      await fs.rename(temp, stateFile);
    } finally {
      await fs.rm(temp, { force: true });
    }
  }
  async function cleanUntracked() {
    const keep = new Set(
      [...records]
        .filter((record) => record.managed)
        .map((record) => path.basename(record.directory)),
    );
    for (const name of await fs.readdir(releasesDir)) {
      if (!keep.has(name))
        await fs.rm(path.join(releasesDir, name), {
          recursive: true,
          force: true,
        });
    }
  }
  async function restore() {
    let state;
    try {
      state = JSON.parse(await fs.readFile(stateFile, "utf8"));
      if (!state || typeof state !== "object" || Array.isArray(state))
        throw new Error("Invalid cache state.");
    } catch (error) {
      if (error.code !== "ENOENT")
        logger.error(
          "[release] Cached pointer unavailable; attempting remote recovery.",
        );
      return;
    }
    for (const reference of [state.active, state.previous]) {
      if (!reference || same(current?.reference, reference)) continue;
      try {
        validateReference(reference);
        const record = await openRecord(directoryFor(reference), reference);
        if (!current) current = record;
        else {
          previous = record;
          await closeRecord(record);
        }
      } catch {
        logger.error(
          "[release] Cached release failed validation; trying another verified release.",
        );
      }
    }
  }
  async function cachedBytes() {
    let size = 0;
    for (const record of records) {
      if (!record.managed) continue;
      for (const name of ["manifest.json", "ep_data.db", "analytics.db"])
        size += (await fs.stat(path.join(record.directory, name))).size;
    }
    return size;
  }
  async function checkOnce() {
    if (stopped) return false;
    // A request using the prior release must finish before another replacement.
    if (previous?.refs) return false;
    const pointer = await storage.readCurrent();
    if (!pointer) throw new Error("No published release exists.");
    const reference = publicReference(pointer.value);
    validateReference(reference);
    if (same(current?.reference, reference)) return false;
    let candidate;
    let destination;
    try {
      if (same(previous?.reference, reference)) {
        await closeRecord(previous);
        candidate = await openRecord(previous.directory, reference);
      } else {
        const { totalBytes } = await storage.inspectRelease(reference);
        if (!Number.isSafeInteger(totalBytes) || totalBytes <= 0)
          throw new Error("Invalid staged release size.");
        const diskReserve = 64 * 1024 * 1024;
        async function fits() {
          const disk = await fs.statfs(cacheDir);
          return (
            totalBytes + (await cachedBytes()) <= maxCacheBytes &&
            disk.bavail * disk.bsize >= totalBytes + diskReserve
          );
        }
        if (!(await fits()) && previous?.managed && !previous.refs) {
          // The active verified release is the rollback during staging. Free an
          // older, idle copy on small disks before trying to download again.
          await persist(current, null);
          previous = null;
          await prune();
        }
        if (!(await fits()))
          throw new Error(
            "Insufficient disk space or cache budget for a staged release.",
          );
        destination = directoryFor(reference);
        await storage.downloadRelease(reference, destination);
        candidate = await openRecord(destination, reference);
      }
      if (stopped) throw new Error("Release manager stopped during download.");
      // Do not install a candidate superseded while a multi-GB download ran.
      const latest = await storage.readCurrent();
      if (!latest || !same(reference, latest.value))
        throw new Error(
          "Published release changed during download; retrying next poll.",
        );
      await persist(candidate, current);
      const obsolete = previous;
      previous = current;
      current = candidate;
      if (
        obsolete &&
        obsolete !== candidate &&
        obsolete.directory === candidate.directory
      )
        records.delete(obsolete);
      await prune();
      logger.info(`[release] Serving dataset ${current.dataset.release.id}`);
      return true;
    } catch (error) {
      if (candidate && candidate !== current && candidate !== previous) {
        await closeRecord(candidate);
        records.delete(candidate);
      }
      if (
        destination &&
        ![current?.directory, previous?.directory].includes(destination)
      )
        await fs.rm(destination, { recursive: true, force: true });
      throw error;
    }
  }
  function checkForUpdate() {
    if (stopped) return Promise.resolve(false);
    if (!pending)
      pending = checkOnce().finally(() => {
        pending = null;
      });
    return pending;
  }
  try {
    await restore();
    await cleanUntracked();
    try {
      await checkForUpdate();
    } catch {
      logger.error(
        "[release] Remote pickup failed; retaining the last verified local release.",
      );
    }
    if (!current && fallbackDirectory) {
      const { manifest, bytes } = await validateDirectory(fallbackDirectory);
      current = await openRecord(
        path.resolve(fallbackDirectory),
        {
          id: manifest.id,
          manifestSha256: crypto
            .createHash("sha256")
            .update(bytes)
            .digest("hex"),
        },
        false,
      );
    }
    if (!current)
      throw new Error(
        "No verified release is available locally or in storage.",
      );
    // Repair fallback state after a cached active release failed validation.
    await persist(current, previous);
    await prune();
  } catch (error) {
    current = null;
    for (const record of records) await closeRecord(record);
    throw error;
  }
  timer = setInterval(() => {
    checkForUpdate().catch(() =>
      logger.error(
        "[release] Remote pickup failed; retaining the last verified local release.",
      ),
    );
  }, pollIntervalMs);
  timer.unref();
  return {
    acquire() {
      if (stopped || !current)
        throw new Error("Release manager is not serving requests.");
      const record = current;
      record.refs++;
      let released = false;
      return {
        dataset: record.dataset,
        release() {
          if (released) return;
          released = true;
          record.refs--;
          if (record !== current)
            closeRecord(record).catch(() =>
              logger.error("[release] Retired database close failed."),
            );
        },
      };
    },
    get dataset() {
      return current.dataset;
    },
    checkForUpdate,
    async close() {
      if (stopped) return;
      stopped = true;
      clearInterval(timer);
      await pending?.catch(() => {});
      current = null;
      for (const record of records) await closeRecord(record);
      storage.close?.();
    },
  };
}
module.exports = { createReleaseManager };
