const assert = require("node:assert/strict");
const test = require("node:test");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { createFixture } = require("./fixtures/create");
const { open, close, get, run } = require("../src/data/sqlite");
const { migrateArchive } = require("../src/data/archive-migration");
const { loadDataset, prepare } = require("../src/data/dataset");

test("archive migration preserves IDs and labels and rebuilds a public release", async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-migrate-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const source = await createFixture(path.join(dir, "legacy.db"));
  const canonical = path.join(dir, "canonical.db");
  const output = path.join(dir, "release");
  const log = () => {};
  const migrate = (from, to) =>
    migrateArchive({ source: from, output: to, log });

  const first = await migrate(source, canonical);
  assert.equal(first.speeches, 5);
  assert.equal(first.speechSources, 5);
  assert.equal(first.sittingSources, 3);
  assert.equal(first.unresolvedSittingSources, 3);
  assert.equal(first.legacyEnrichments, 5);
  assert.deepEqual(await migrate(source, path.join(dir, "repeat.db")), first);
  assert.deepEqual(
    await migrate(canonical, path.join(dir, "replayed.db")),
    first,
  );
  await assert.rejects(migrate(source, canonical), { code: "EEXIST" });

  const original = await open(source, { readonly: true });
  const migrated = await open(canonical);
  try {
    assert.equal(
      await get(
        original,
        "SELECT name FROM sqlite_schema WHERE name='source_records'",
      ),
      undefined,
    );
    assert.equal(
      (
        await get(
          migrated,
          "SELECT outcome FROM speech_enrichment WHERE speech_id=1",
        )
      ).outcome,
      "legacy",
    );
    await run(
      migrated,
      "UPDATE speech_enrichment SET outcome='failed',macro_topic='Rejected' WHERE speech_id=2",
    );
    await run(
      migrated,
      "UPDATE individual_speeches SET macro_topic='Stale projection' WHERE id=1",
    );
  } finally {
    await close(original);
    await close(migrated);
  }

  await prepare({ source: canonical, output, log });
  const dataset = await loadDataset(output, { verifyHashes: true });
  try {
    assert.equal(dataset.release.stats.speeches, 5);
    assert.deepEqual(
      await get(
        dataset.db,
        "SELECT id,macro_topic FROM individual_speeches WHERE id=1",
      ),
      {
        id: 1,
        macro_topic: "Climate &amp; energy",
      },
    );
    assert.equal(
      (
        await get(
          dataset.db,
          "SELECT macro_topic FROM individual_speeches WHERE id=2",
        )
      ).macro_topic,
      null,
    );
    assert.equal(
      await get(
        dataset.db,
        "SELECT name FROM sqlite_schema WHERE name='source_revisions'",
      ),
      undefined,
    );
    assert.equal(
      (
        await get(
          dataset.analytics,
          "SELECT count FROM topics WHERE topic='Climate & energy'",
        )
      ).count,
      3,
    );
  } finally {
    await dataset.close();
  }
});

test("an orphaned archive is rejected without exposing a partial checkpoint", async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-orphan-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const source = await createFixture(path.join(dir, "legacy.db"));
  const output = path.join(dir, "canonical.db");
  const db = await open(source);
  try {
    await run(
      db,
      "UPDATE individual_speeches SET sitting_id='missing' WHERE id=1",
    );
  } finally {
    await close(db);
  }
  await assert.rejects(
    migrateArchive({ source, output, log() {} }),
    /Foreign key violation/,
  );
  await assert.rejects(fs.access(output), { code: "ENOENT" });
});
