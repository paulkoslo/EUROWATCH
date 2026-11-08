const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const http = require("node:http");
const { createFixture } = require("./fixtures/create");
const { prepare, loadDataset } = require("../src/data/dataset");
const { all } = require("../src/data/sqlite");
const { createReleaseManager } = require("../src/data/release-manager");
const { createApp } = require("../src/explorer/app");
const queries = require("../src/explorer/queries");
const logger = { info() {}, error() {} };
const tick = () => new Promise((resolve) => setImmediate(resolve));
function deferred() {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function fixture(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-pickup-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const releases = new Map();
  const refs = [];
  for (let i = 0; i < 3; i++) {
    const source = await createFixture(path.join(root, `source-${i}.db`));
    const directory = path.join(root, `release-${i}`);
    await prepare({ source, output: directory, log() {} });
    const bytes = await fs.readFile(path.join(directory, "manifest.json"));
    const manifest = JSON.parse(bytes);
    const ref = {
      id: manifest.id,
      manifestSha256: crypto.createHash("sha256").update(bytes).digest("hex"),
    };
    refs.push(ref);
    releases.set(ref.id, {
      directory,
      totalBytes:
        bytes.length +
        Object.values(manifest.files).reduce(
          (sum, file) => sum + file.bytes,
          0,
        ),
    });
  }
  const storage = {
    current: refs[0],
    fail: false,
    downloads: 0,
    async readCurrent() {
      if (this.fail) throw new Error("Offline");
      return this.current ? { value: this.current, etag: '"test"' } : null;
    },
    async inspectRelease(ref) {
      return releases.get(ref.id);
    },
    async downloadRelease(ref, destination) {
      this.downloads++;
      await fs.cp(releases.get(ref.id).directory, destination, {
        recursive: true,
        errorOnExist: true,
        force: false,
      });
      if (this.afterDownload) await this.afterDownload(ref, destination);
    },
    close() {},
  };
  const options = {
    storage,
    cacheDir: path.join(root, "cache"),
    pollIntervalMs: 3600000,
    logger,
  };
  return { root, refs, releases, storage, options };
}
test("pickup pins request handles, defers a third release, retains two and safely rolls back", async (t) => {
  const { refs, options, storage } = await fixture(t);
  const manager = await createReleaseManager(options);
  t.after(() => manager.close());
  const first = manager.acquire();
  storage.current = refs[1];
  assert.equal(await manager.checkForUpdate(), true);
  assert.equal(manager.dataset.release.id, refs[1].id);
  assert.equal((await all(first.dataset.db, "SELECT 1 AS n"))[0].n, 1);
  storage.current = refs[2];
  assert.equal(await manager.checkForUpdate(), false);
  assert.equal(storage.downloads, 2);
  first.release();
  first.release(); // release is idempotent
  await manager.checkForUpdate();
  assert.equal(manager.dataset.release.id, refs[2].id);
  assert.equal(
    (await fs.readdir(path.join(options.cacheDir, "releases"))).length,
    2,
  );
  await assert.rejects(all(first.dataset.db, "SELECT 1"), /closed/);
  storage.current = refs[1];
  assert.equal(await manager.checkForUpdate(), true);
  assert.equal((await all(manager.dataset.db, "SELECT 1 AS n"))[0].n, 1);
  assert.equal(manager.dataset.release.id, refs[1].id);
  assert.equal(
    (await fs.readdir(path.join(options.cacheDir, "releases"))).length,
    2,
  );
});
test("restarts offline on verified cache and falls back to previous when active bytes are corrupt", async (t) => {
  const { refs, options, storage } = await fixture(t);
  let manager = await createReleaseManager(options);
  storage.current = refs[1];
  await manager.checkForUpdate();
  const activePath = manager.dataset.directory;
  await manager.close();
  storage.fail = true;
  manager = await createReleaseManager(options);
  assert.equal(manager.dataset.release.id, refs[1].id);
  await manager.close();
  await fs.writeFile(path.join(activePath, "analytics.db"), "corrupted");
  manager = await createReleaseManager(options);
  assert.equal(manager.dataset.release.id, refs[0].id);
  await manager.close();
  await fs.rm(options.cacheDir, { recursive: true });
  await assert.rejects(createReleaseManager(options), /No verified release/);
});
test("failed, superseded or oversized candidates never replace current and leave no partial release", async (t) => {
  const { refs, options, storage } = await fixture(t);
  const manager = await createReleaseManager(options);
  t.after(() => manager.close());
  storage.current = refs[1];
  storage.afterDownload = async (ref, destination) => {
    await fs.writeFile(path.join(destination, "ep_data.db"), "bad");
  };
  await assert.rejects(manager.checkForUpdate(), /hash mismatch/);
  assert.equal(manager.dataset.release.id, refs[0].id);
  assert.equal(
    (await fs.readdir(path.join(options.cacheDir, "releases"))).length,
    1,
  );
  storage.afterDownload = async () => {
    storage.current = refs[2];
  };
  await assert.rejects(manager.checkForUpdate(), /changed during download/);
  assert.equal(manager.dataset.release.id, refs[0].id);
  assert.equal(
    (await fs.readdir(path.join(options.cacheDir, "releases"))).length,
    1,
  );
  storage.inspectRelease = async () => ({ totalBytes: 20 * 1024 ** 3 });
  await assert.rejects(manager.checkForUpdate(), /byte budget/);
  assert.equal(manager.dataset.release.id, refs[0].id);
});
test("shutdown during download cleans candidate and concurrent polls share one transfer", async (t) => {
  const { refs, options, storage } = await fixture(t);
  const manager = await createReleaseManager(options);
  const entered = deferred(),
    finish = deferred();
  storage.current = refs[1];
  storage.afterDownload = async () => {
    entered.resolve();
    await finish.promise;
  };
  const update = manager.checkForUpdate();
  assert.equal(manager.checkForUpdate(), update);
  await entered.promise;
  const closed = manager.close();
  finish.resolve();
  await assert.rejects(update, /stopped during download/);
  await closed;
  assert.equal(
    (await fs.readdir(path.join(options.cacheDir, "releases"))).length,
    1,
  );
});
test("HTTP requests retain a single release across pickup and client disconnect until async queries finish", async (t) => {
  const { refs, options, storage } = await fixture(t);
  const manager = await createReleaseManager(options);
  const app = createApp(manager, { logger });
  const server = await new Promise((resolve) => {
    const s = app.listen(0, "127.0.0.1", () => resolve(s));
  });
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    await manager.close();
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  const original = queries.sitting;
  t.after(() => {
    queries.sitting = original;
  });
  const entered = deferred(),
    finish = deferred();
  let held;
  queries.sitting = async (dataset, id) => {
    held = dataset;
    entered.resolve();
    await finish.promise;
    return original(dataset, id);
  };
  const response = fetch(base + "/api/sittings/sitting-2024-01-15");
  await entered.promise;
  storage.current = refs[1];
  await manager.checkForUpdate();
  assert.equal((await all(held.db, "SELECT 1 AS n"))[0].n, 1);
  finish.resolve();
  const result = await response;
  assert.equal(result.status, 200);
  assert.equal(result.headers.get("x-dataset-id"), refs[0].id);
  await result.json();
  assert.equal(
    (await (await fetch(base + "/readyz")).json()).datasetId,
    refs[1].id,
  );
  const entered2 = deferred(),
    finish2 = deferred(),
    done2 = deferred();
  queries.sitting = async (dataset, id) => {
    held = dataset;
    entered2.resolve();
    await finish2.promise;
    try {
      return await original(dataset, id);
    } finally {
      done2.resolve();
    }
  };
  const req = http.get(base + "/api/sittings/sitting-2024-01-15");
  req.on("error", () => {});
  await entered2.promise;
  req.destroy();
  await tick();
  storage.current = refs[2];
  await manager.checkForUpdate();
  assert.equal((await all(held.db, "SELECT 1 AS n"))[0].n, 1);
  finish2.resolve();
  await done2.promise;
  await tick();
});
test("explicit local fallback remains outside managed cache and private references never enter HTTP responses", async (t) => {
  const { refs, options, storage, releases } = await fixture(t);
  storage.fail = true;
  const directory = releases.get(refs[0].id).directory;
  const manager = await createReleaseManager({
    ...options,
    fallbackDirectory: directory,
  });
  assert.equal(manager.dataset.release.id, refs[0].id);
  storage.fail = false;
  storage.current = {
    ...refs[1],
    checkpoint: { key: "secret", sha256: "secret", bytes: 1 },
  };
  await manager.checkForUpdate();
  await manager.close();
  const state = await fs.readFile(
    path.join(options.cacheDir, "current.json"),
    "utf8",
  );
  assert.ok(!state.includes("secret"));
  const stillThere = await loadDataset(directory);
  await stillThere.close();
});
