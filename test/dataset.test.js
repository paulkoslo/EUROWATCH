const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { createFixture } = require("./fixtures/create");
const {
  prepare,
  loadDataset,
  promote,
  currentDirectory,
  hashFile,
} = require("../src/data/dataset");
const { open, get, run, close } = require("../src/data/sqlite");
const { migrate } = require("../src/data/schema");

test("fresh maintenance schema is complete and idempotent without seeding upstream data", async () => {
  const db = await open(":memory:");
  try {
    await migrate(db);
    await migrate(db);
    assert.equal(
      (
        await get(
          db,
          "SELECT COUNT(*) AS n FROM individual_speeches WHERE mep_id IS NULL AND macro_topic IS NULL AND language IS NULL AND political_group_std IS NULL",
        )
      ).n,
      0,
    );
    assert.equal((await get(db, "SELECT COUNT(*) AS n FROM meps")).n, 0);
  } finally {
    await close(db);
  }
});

test("blank legacy speaker references preserve contributions without inventing numeric profiles", async (t) => {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "eurowatch-blank-id-test-"),
  );
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const source = await createFixture(path.join(root, "source.db"));
  const db = await open(source);
  await run(
    db,
    "INSERT INTO individual_speeches(sitting_id,mep_id,speaker_name,speech_content,speech_order) VALUES ('sitting-2025-01-13','','Raw Chair Label','Keep this speech',99)",
  );
  await close(db);
  await prepare({ source, output: path.join(root, "release"), log: () => {} });
  const dataset = await loadDataset(path.join(root, "release"));
  try {
    assert.equal(dataset.release.stats.speeches, 6);
    assert.equal(dataset.release.stats.unresolvedSpeakerSpeeches, 2);
    assert.equal(
      (
        await get(
          dataset.db,
          "SELECT COUNT(*) AS n FROM explorer_speakers WHERE id=0",
        )
      ).n,
      0,
    );
    assert.equal(
      (
        await get(
          dataset.db,
          "SELECT speech_content FROM individual_speeches WHERE mep_id=''",
        )
      ).speech_content,
      "Keep this speech",
    );
  } finally {
    await dataset.close();
  }
});

test("snapshot preserves its source, resolves missing profiles for browsing, and promotes atomically", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-data-test-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const source = await createFixture(path.join(root, "source.db"));
  const before = await hashFile(source);
  const output = path.join(root, "releases/one");
  await prepare({ source, output, log: () => {} });
  assert.equal(await hashFile(source), before);
  const dataset = await loadDataset(output, { verifyHashes: true });
  assert.equal(dataset.release.stats.speeches, 5);
  assert.equal(dataset.release.stats.unresolvedSpeakerSpeeches, 1);
  assert.equal(
    (await get(dataset.db, "SELECT source FROM explorer_speakers WHERE id=999"))
      .source,
    "unresolved",
  );
  assert.equal(
    (
      await get(
        dataset.analytics,
        "SELECT SUM(count) AS n FROM topic_series WHERE topic='Climate & energy'",
      )
    ).n,
    3,
  );
  await assert.rejects(
    run(dataset.db, "DELETE FROM individual_speeches"),
    /readonly/,
  );
  await dataset.close();
  await promote(output, path.join(root, "data/current.json"));
  assert.equal(await currentDirectory(root), output);
  const next = path.join(root, "releases/two");
  await prepare({ source, output: next, from: "2025-01-01", log: () => {} });
  await promote(next, path.join(root, "data/current.json"));
  assert.equal(await currentDirectory(root), next);
  await promote(output, path.join(root, "data/current.json"));
  assert.equal(
    await currentDirectory(root),
    output,
    "a previous release can be restored without overwriting files",
  );
  await assert.rejects(prepare({ source, output, log: () => {} }), /EEXIST/);
});

test("subsets remove dependent speeches and reject source sitting orphans", async (t) => {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "eurowatch-subset-test-"),
  );
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const source = await createFixture(path.join(root, "source.db"));
  await prepare({
    source,
    output: path.join(root, "subset"),
    from: "2025-01-01",
    log: () => {},
  });
  const dataset = await loadDataset(path.join(root, "subset"));
  assert.equal(dataset.release.stats.speeches, 1);
  assert.equal(dataset.release.stats.sittings, 1);
  assert.equal(dataset.release.stats.sittingOrphans, 0);
  await dataset.close();
  const db = await open(source);
  await run(
    db,
    "INSERT INTO individual_speeches(sitting_id,speaker_name) VALUES ('absent','Orphan')",
  );
  await close(db);
  await assert.rejects(
    prepare({ source, output: path.join(root, "bad"), log: () => {} }),
    /without sittings/,
  );
  await assert.rejects(loadDataset(path.join(root, "bad")), /ENOENT/);
});

test("mismatched analytics cannot be served or promoted", async (t) => {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "eurowatch-mismatch-test-"),
  );
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const source = await createFixture(path.join(root, "source.db"));
  const output = path.join(root, "release");
  await prepare({ source, output, log: () => {} });
  const db = await open(path.join(output, "analytics.db"));
  await run(db, "UPDATE dataset_metadata SET value=? WHERE key=?", [
    JSON.stringify({ id: "different" }),
    "release",
  ]);
  await close(db);
  const dataset = await loadDataset(output);
  assert.equal(dataset.analytics, null);
  assert.match(dataset.analyticsError, /another dataset/);
  await dataset.close();
  await assert.rejects(
    promote(output, path.join(root, "current.json")),
    /hash mismatch/,
  );
});
