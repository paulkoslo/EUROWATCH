const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { execFile } = require("node:child_process");
const { promisify } = require("node:util");
const { createFixture } = require("./fixtures/create");
const { hashFile, loadDataset, prepare } = require("../src/data/dataset");
const { refreshWorkspace, runRefresh } = require("../src/data/refresh-run");

const first = "2026-03-09";
const second = "2026-03-10";
const transcript = `<html><body>
<table><tr><td class="doc_title"><img src="arrow_title_doc.gif">1. Research</td></tr></table>
<p>Ana Example (PPE). – We should discuss how Parliament can support international research.</p>
</body></html>`;

async function fixture(t) {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "eurowatch-refresh-test-"),
  );
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const source = await createFixture(path.join(root, "source.db"));
  const directory = path.join(root, "baseline");
  const baseline = await prepare({ source, output: directory, log() {} });
  const workspaceRoot = path.join(root, "refresh");
  return { directory, baseline, workspaceRoot };
}

test("a partial refresh survives a retry and preserves both run reports", async (t) => {
  const options = await fixture(t);
  const original = path.join(options.directory, "ep_data.db");
  const originalHash = await hashFile(original);
  const fetched = [];
  const discoveryStarted = Promise.withResolvers();
  const discoveryMayFinish = Promise.withResolvers();
  let fail = true;
  const request = async (url) => {
    if (url.endsWith("/meetings")) {
      if (fail) {
        discoveryStarted.resolve();
        await discoveryMayFinish.promise;
      }
      return {
        data: [first, second].map((date) => ({
          activity_date: date,
          had_activity_type: "def/ep-activities/PLENARY_SITTING",
          recorded_in_a_realization_of: [`eli/dl/doc/CRE-10-${date}`],
        })),
      };
    }
    if (url.endsWith("_EN.html")) fetched.push(url);
    if (fail && url.includes(second))
      throw new Error("simulated upstream timeout");
    return transcript;
  };
  const refresh = () =>
    runRefresh({ ...options, start: first, end: second, request });
  const firstRun = refresh();
  await discoveryStarted.promise;
  try {
    await assert.rejects(refresh(), /already running/);
  } finally {
    discoveryMayFinish.resolve();
  }
  const partial = await firstRun;
  assert.equal(partial.outcome, "partial");
  assert.equal(partial.added, 1);
  assert.equal(partial.result.failed, 1);
  fail = false;
  const retry = await refresh();
  assert.equal(retry.outcome, "complete");
  assert.equal(retry.added, 1);
  assert.equal(retry.result.existing, 1);
  assert.equal(fetched.filter((url) => url.includes(first)).length, 1);
  assert.notEqual(partial.datasetId, retry.datasetId);
  const workspace = refreshWorkspace(
    options.workspaceRoot,
    options.baseline.id,
  );
  const report = async (id) =>
    fs.readFile(path.join(workspace, "runs", `${id}.jsonl`), "utf8");
  assert.match(
    await report(partial.runId),
    /2026-03-10 failed: .*simulated upstream timeout/,
  );
  assert.match(
    await report(retry.runId),
    /Resuming the existing staging database/,
  );
  const latest = await loadDataset(retry.directory, { verifyHashes: true });
  try {
    assert.equal(
      latest.release.stats.speeches,
      options.baseline.stats.speeches + 2,
    );
    assert.equal(latest.release.id, retry.datasetId);
    assert.equal(await hashFile(original), originalHash);
  } finally {
    await latest.close();
  }
});

test("the refresh CLI prints and saves the same run events", async (t) => {
  const options = await fixture(t);
  const { stdout } = await promisify(execFile)(
    process.execPath,
    [
      require.resolve("../src/data/refresh-run"),
      "--dataset",
      options.directory,
      "--workspace",
      options.workspaceRoot,
      "--start",
      "2099-01-01",
    ],
    { env: { ...process.env, LOCAL_REFRESH_END: "" }, timeout: 5000 },
  );
  const events = stdout.trim().split("\n").map(JSON.parse);
  assert.deepEqual(
    events.map((event) => event.stage),
    ["start", "complete"],
  );
  assert.equal(events[1].datasetId, options.baseline.id);
  assert.equal(await fs.readFile(events[0].logFile, "utf8"), stdout);
});

test("an invalid refresh date fails before staging is created", async (t) => {
  const options = await fixture(t);
  await assert.rejects(
    runRefresh({ ...options, start: "2099-99-99" }),
    /valid inclusive/,
  );
  const workspace = refreshWorkspace(
    options.workspaceRoot,
    options.baseline.id,
  );
  await assert.rejects(fs.access(path.join(workspace, "staging.db")), {
    code: "ENOENT",
  });
});
