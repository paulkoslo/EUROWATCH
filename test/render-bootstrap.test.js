const assert = require("node:assert/strict");
const test = require("node:test");
const fs = require("node:fs/promises");
const { writeFileSync } = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { createFixture } = require("./fixtures/create");
const { loadDataset, hashFile } = require("../src/data/dataset");
const { resolveStartupDataset } = require("../src/data/render-bootstrap");

const env = { RENDER: "true" };
const log = () => {};

async function workspace(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-render-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  return root;
}

test("Render prepares the legacy archive once and reuses the verified release", async (t) => {
  const root = await workspace(t);
  const source = await createFixture(path.join(root, "data", "ep_data.db"));
  const sourceHash = await hashFile(source);
  const staging = path.join(
    root,
    "data",
    "releases",
    ".render-bootstrap-preparing",
  );
  await fs.mkdir(staging, { recursive: true });
  await fs.writeFile(path.join(staging, "ep_data.db"), "interrupted copy");

  const [directory, concurrent] = await Promise.all([
    resolveStartupDataset(root, env, log),
    resolveStartupDataset(root, env, log),
  ]);
  assert.equal(directory, concurrent);
  assert.equal(await hashFile(source), sourceHash);
  assert.equal(await resolveStartupDataset(root, env, log), directory);
  await assert.rejects(fs.access(staging), { code: "ENOENT" });
  const dataset = await loadDataset(directory, { verifyHashes: true });
  try {
    assert.equal(dataset.release.stats.speeches, 5);
    assert.equal(dataset.release.stats.sittings, 3);
    assert.ok(dataset.analytics);
  } finally {
    await dataset.close();
  }

  // Recover a completed release whose pointer was not installed yet.
  await fs.unlink(path.join(root, "data", "current.json"));
  assert.equal(await resolveStartupDataset(root, env, log), directory);
  const pointer = JSON.parse(
    await fs.readFile(path.join(root, "data", "current.json")),
  );
  assert.equal(pointer.id, dataset.release.id);
});

test("a configured path takes precedence and a missing or corrupt source leaves no pointer", async (t) => {
  const root = await workspace(t);
  assert.equal(
    await resolveStartupDataset(
      root,
      { ...env, DATASET_DIR: "/explicit" },
      log,
    ),
    "/explicit",
  );
  await assert.rejects(resolveStartupDataset(root, {}, log), {
    code: "ENOENT",
  });
  await assert.rejects(
    resolveStartupDataset(root, env, log),
    /full Render archive is missing/,
  );
  await fs.mkdir(path.join(root, "data"));
  await fs.writeFile(
    path.join(root, "data", "ep_data.db"),
    "not a SQLite archive",
  );
  await assert.rejects(
    resolveStartupDataset(root, env, log),
    /Preparation failed/,
  );
  await assert.rejects(fs.access(path.join(root, "data", "current.json")), {
    code: "ENOENT",
  });
  await assert.rejects(
    fs.access(
      path.join(root, "data", "releases", ".render-bootstrap-preparing"),
    ),
    { code: "ENOENT" },
  );
});

test("a manual pointer selected during preparation wins", async (t) => {
  const root = await workspace(t);
  await createFixture(path.join(root, "data", "ep_data.db"));
  const pointer = path.join(root, "data", "current.json");
  const result = await resolveStartupDataset(root, env, (message) => {
    if (message.startsWith("Preparing the existing"))
      writeFileSync(
        pointer,
        JSON.stringify({ directory: "manual-release", id: "manual" }),
      );
  });
  assert.equal(result, path.join(root, "data", "manual-release"));
  assert.equal(JSON.parse(await fs.readFile(pointer)).id, "manual");
});
