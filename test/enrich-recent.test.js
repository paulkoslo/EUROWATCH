const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");
const os = require("node:os");
const { createFixture } = require("./fixtures/create");
const { migrateArchive } = require("../src/data/archive-migration");
const { open, close, exec } = require("../src/data/sqlite");
const { hashFile } = require("../src/data/dataset");
const { createRecentEnrichment } = require("../src/insights/enrich-recent");

test("recent enrichment resumes a verified release and detects a correction outside the initial window", async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-enrich-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const source = await createFixture(path.join(dir, "fixture.db"));
  const canonical = path.join(dir, "archive.db");
  await migrateArchive({ source, output: canonical, log() {} });
  const identify = async (id) => ({
    value: {
      id,
      checkpoint: {
        bytes: (await fs.stat(canonical)).size,
        sha256: await hashFile(canonical),
      },
    },
  });
  let current = await identify("first");
  let downloads = 0;
  const storage = {
    async readCurrent() {
      return current;
    },
    async inspectRelease() {
      return { manifest: { stats: { toDate: "2025-01-13" } } };
    },
    async downloadCheckpoint(_ref, to) {
      downloads++;
      await fs.copyFile(canonical, to);
    },
  };
  const windows = [];
  const check = createRecentEnrichment({
    storage,
    workspace: path.join(dir, "worker"),
    config: { model: "fixture" },
    async backfill(options) {
      windows.push([options.from, options.to]);
      return {
        examinedWindows: 1,
        windows: 1,
        incomplete: windows.length === 1,
      };
    },
  });
  assert.equal((await check()).status, "pending");
  assert.equal((await check()).status, "prepared");
  assert.equal((await check()).status, "unchanged");
  assert.equal(downloads, 1);
  const db = await open(canonical);
  try {
    await exec(
      db,
      "UPDATE source_records SET provider='europarl',official_id='CRE-9-2024-01-15' WHERE source_key='sitting-2024-01-15'",
    );
  } finally {
    await close(db);
  }
  current = await identify("second");
  assert.equal((await check()).status, "prepared");
  assert.deepEqual(windows, [
    ["2024-12-31", "2025-01-13"],
    ["2024-12-31", "2025-01-13"],
    ["2024-01-15", "2025-01-13"],
  ]);
  assert.equal(downloads, 2);
});
