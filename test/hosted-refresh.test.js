const { test } = require("node:test");
const assert = require("node:assert/strict");
const { once } = require("node:events");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { hashFile, prepare } = require("../src/data/dataset");
const { migrateArchive } = require("../src/data/archive-migration");
const { createReleaseManager } = require("../src/data/release-manager");
const { createApp } = require("../src/explorer/app");
const {
  createUpdateControl,
  renderLauncher,
} = require("../src/explorer/update-control");
const {
  runHosted,
  seedHosted,
  claimLease,
} = require("../src/data/hosted-refresh");
const { createFixture } = require("./fixtures/create");
const log = () => {};

function fakeStorage(root) {
  const current = new Map(),
    releases = new Map(),
    checkpoints = new Map();
  const read = async (key) => current.get(key) || null;
  async function write(key, value, { expectedETag }) {
    const existing = current.get(key)?.etag ?? null;
    if (existing !== expectedETag)
      throw Object.assign(new Error("CAS conflict"), {
        code: "RELEASE_CONFLICT",
      });
    const row = {
      value: structuredClone(value),
      etag: String(Number(existing || 0) + 1),
    };
    current.set(key, row);
    return row;
  }
  return {
    readCurrent: () => read("current"),
    async publishRelease(value, options) {
      await options.beforeCommit?.();
      return write("current", value, options);
    },
    readLease: () => read("lease"),
    writeLease: (value, options) => write("lease", value, options),
    readRunReport: (id) => read(id),
    readLaunch: (id) => read("launch-" + id),
    writeLaunch: (id, jobId) =>
      write("launch-" + id, { jobId }, { expectedETag: null }),
    writeRunReport: write,
    async listRunReports() {
      return {
        items: [...current.keys()]
          .filter((id) => id.includes("-") && !id.startsWith("launch-"))
          .map((runId) => ({ runId })),
      };
    },
    async uploadRelease(directory) {
      const file = path.join(directory, "manifest.json");
      const manifest = JSON.parse(await fs.readFile(file, "utf8"));
      const destination = path.join(root, manifest.id);
      await fs.cp(directory, destination, { recursive: true });
      const ref = { id: manifest.id, manifestSha256: await hashFile(file) };
      releases.set(ref.id, { destination, manifest, totalBytes: 1_000_000 });
      return ref;
    },
    async inspectRelease(ref) {
      return releases.get(ref.id);
    },
    async downloadRelease(ref, destination) {
      await fs.cp(releases.get(ref.id).destination, destination, {
        recursive: true,
      });
    },
    async uploadCheckpoint(filename, key) {
      const dest = path.join(root, key + ".db");
      await fs.copyFile(filename, dest);
      checkpoints.set(key, dest);
      return {
        key,
        bytes: (await fs.stat(dest)).size,
        sha256: await hashFile(dest),
      };
    },
    async downloadCheckpoint(ref, destination) {
      await fs.copyFile(checkpoints.get(ref.key), destination);
    },
    close() {},
  };
}

test("shared update job publishes twice, replays unchanged and preserves the previous release on failure", async (t) => {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "eurowatch-hosted-test-"),
  );
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const source = await createFixture(path.join(root, "source.db"));
  const checkpoint = path.join(root, "seed.db");
  const directory = path.join(root, "seed-release");
  await migrateArchive({ source, output: checkpoint, log });
  await prepare({ source: checkpoint, output: directory, log });
  const storage = fakeStorage(root);
  await seedHosted({ storage, directory, checkpoint });
  const manager = await createReleaseManager({
    storage,
    cacheDir: path.join(root, "cache"),
    logger: { info: log, error: log },
  });
  const password = "operator-test-credential-0123456";
  const auth = `Basic ${Buffer.from(`operator:${password}`).toString("base64")}`;
  let launched;
  const updateControl = createUpdateControl({
    storage,
    password,
    async launch(value) {
      launched = value;
      return "job-test";
    },
  });
  const server = createApp(manager, { updateControl }).listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(async () => {
    server.close();
    await manager.close();
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  const startUpdate = (headers) =>
    fetch(base + "/operator/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: "{}",
    });
  assert.equal((await fetch(base + "/operator/runs")).status, 401);
  const page = await fetch(base + "/operator/", {
    headers: { Authorization: auth },
  });
  assert.match(
    page.headers.get("content-security-policy"),
    /frame-ancestors 'none'/,
  );
  assert.equal(page.status, 200);
  assert.equal(
    (await startUpdate({ Authorization: auth, "Sec-Fetch-Site": "cross-site" }))
      .status,
    403,
  );
  assert.equal((await startUpdate({ Authorization: auth })).status, 202);

  const dates = ["2026-03-09", "2026-03-10", "2026-03-11", "2026-03-12"];
  let failDate;
  let words =
    "We should support international research across the European Parliament.";
  const request = async (url, { params } = {}) => {
    if (url.endsWith("/meetings"))
      return {
        data:
          params.year === 2026
            ? dates.map((date) => ({
                activity_date: date,
                had_activity_type: "def/ep-activities/PLENARY_SITTING",
                parliamentary_term: "org/ep-10",
                recorded_in_a_realization_of: [`eli/dl/doc/CRE-10-${date}`],
              }))
            : [],
      };
    if (failDate && url.includes(failDate))
      throw new Error("forced fetch failure");
    return `<html><body><table><tr><td class="doc_title"><img src="arrow_title_doc.gif">1. Research</td></tr></table>
      <p>Ana Example (PPE). – ${words}</p></body></html>`;
  };
  const run = (options) =>
    runHosted({
      storage,
      request,
      workRoot: root,
      sourcePreference: "html",
      log,
      ...options,
    });
  async function assertPublic(report, expectedCount) {
    assert.equal(await manager.checkForUpdate(), true);
    const stats = await fetch(base + "/api/data-status");
    const download = await fetch(base + "/api/download/full-dataset");
    assert.equal(stats.headers.get("x-dataset-id"), report.releaseId);
    assert.equal(download.headers.get("x-dataset-id"), report.releaseId);
    assert.equal((await stats.json()).stats.speeches, expectedCount);
    const bytes = Buffer.from(await download.arrayBuffer());
    const file = path.join(root, "public-download.db");
    await fs.writeFile(file, bytes);
    const { manifest } = await storage.inspectRelease({ id: report.releaseId });
    assert.equal(await hashFile(file), manifest.files["ep_data.db"].sha256);
  }
  const manual = await run({
    start: dates[0],
    end: dates[0],
    trigger: "manual",
    runId: launched.runId,
  });
  assert.equal(manual.status, "published");
  await assertPublic(manual, 6);
  const scheduled = await run({ start: dates[0], end: dates[1] });
  assert.equal(scheduled.result.imported, 1);
  await assertPublic(scheduled, 7);
  const repeat = await run({ start: dates[0], end: dates[1] });
  assert.equal(repeat.status, "unchanged");
  assert.equal(repeat.releaseId, scheduled.releaseId);
  assert.equal(repeat.result.imported, 0);
  assert.equal(await manager.checkForUpdate(), false);
  const held = await claimLease(storage, "existing-writer");
  assert.equal((await run({ start: dates[0], end: dates[1] })).status, "busy");
  await held.close();

  failDate = dates[3];
  const failedId = "00000000-0000-4000-8000-000000000001";
  await assert.rejects(
    run({ start: dates[2], end: dates[3], runId: failedId }),
    /1 sitting records failed/,
  );
  assert.equal(await manager.checkForUpdate(), false);
  assert.equal(manager.dataset.release.id, scheduled.releaseId);
  const failed = await storage.readRunReport(failedId);
  assert.equal(failed.value.status, "failed");
  assert.deepEqual(failed.value.failedDates, [dates[3]]);
  failDate = null;
  const retry = await run({
    retryRunId: failedId,
    trigger: "retry",
    end: dates[3],
  });
  assert.equal(retry.result.imported, 2);
  await assertPublic(retry, 9);

  const speechUrl = base + "/api/sittings/sitting-" + dates[3] + "/speeches";
  const initial = (await (await fetch(speechUrl)).json()).data[0];
  words =
    "We should support international research and maintain open archives in Europe.";
  const correction = await run({ start: dates[3], end: dates[3] });
  assert.equal(correction.result.revised, 1);
  await assertPublic(correction, 9);
  const corrected = (await (await fetch(speechUrl)).json()).data[0];
  assert.equal(corrected.id, initial.id);
  assert.notEqual(corrected.content, initial.content);
});

test("the Render launcher uses fixed commands and validated identifiers", async () => {
  const launch = renderLauncher({
    serviceId: "crn-example",
    apiKey: "test",
    async request(url, { body }) {
      assert.equal(url, "https://api.render.com/v1/services/crn-example/jobs");
      assert.match(
        JSON.parse(body).startCommand,
        /^npm run data:update:hosted -- --trigger retry --run-id [0-9a-f-]+ --retry [0-9a-f-]+$/,
      );
      return new Response('{"id":"job-test"}', { status: 201 });
    },
  });
  const id = "00000000-0000-4000-8000-000000000001";
  assert.equal(await launch({ runId: id, retryRunId: id }), "job-test");
  await assert.rejects(launch({ runId: ";date" }), /Invalid run identifier/);
});
