const { test, before, after } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { createFixture } = require("./fixtures/create");
const { prepare, loadDataset, hashFile } = require("../src/data/dataset");
const { createApp, csv } = require("../src/explorer/app");

let root, dataset, server, base, beforeHash;
before(async () => {
  root = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-api-test-"));
  const source = await createFixture(path.join(root, "source.db"));
  await prepare({ source, output: path.join(root, "release"), log: () => {} });
  dataset = await loadDataset(path.join(root, "release"));
  beforeHash = await hashFile(path.join(root, "release/ep_data.db"));
  const app = createApp(dataset, { logger: { error: () => {} } });
  server = await new Promise((resolve) => {
    const instance = app.listen(0, "127.0.0.1", () => resolve(instance));
  });
  base = `http://127.0.0.1:${server.address().port}`;
});
after(async () => {
  await new Promise((resolve) => server.close(resolve));
  await dataset.close();
  assert.equal(
    await hashFile(path.join(root, "release/ep_data.db")),
    beforeHash,
    "browsing must not alter the snapshot",
  );
  await fs.rm(root, { recursive: true, force: true });
});
const json = async (url) => {
  const response = await fetch(base + url);
  assert.equal(response.status, 200, url);
  return response.json();
};

test("missing analytics returns an explicit unavailable state without blocking stored speech browsing", async () => {
  const app = createApp(
    { ...dataset, analytics: null },
    { logger: { error: () => {} }, fullDownload: false },
  );
  const instance = await new Promise((resolve) => {
    const listening = app.listen(0, "127.0.0.1", () => resolve(listening));
  });
  const host = `http://127.0.0.1:${instance.address().port}`;
  try {
    assert.equal((await fetch(host + "/api/topics")).status, 503);
    assert.equal(
      (await fetch(host + "/api/analytics/time-series")).status,
      503,
    );
    assert.equal((await fetch(host + "/api/sittings")).status, 200);
    assert.equal(
      (await fetch(host + "/api/download/full-dataset")).status,
      404,
    );
    assert.equal(
      (await (await fetch(host + "/readyz")).json()).analyticsAvailable,
      false,
    );
  } finally {
    await new Promise((resolve) => instance.close(resolve));
  }
});

test(
  "missing snapshots fail startup without creating an empty database",
  { timeout: 10000 },
  async () => {
    const missing = path.join(root, "not-a-dataset");
    const child = spawn(
      process.execPath,
      [path.resolve(__dirname, "../server.js")],
      { cwd: root, env: { ...process.env, DATASET_DIR: missing, PORT: "0" } },
    );
    let output = "";
    child.stderr.on("data", (chunk) => {
      output += chunk;
    });
    const code = await new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", resolve);
    });
    assert.equal(code, 1);
    assert.match(output, /Prepare a snapshot/);
    await assert.rejects(fs.access(missing), /ENOENT/);
  },
);

test(
  "the production entry point closes its database and exits on SIGTERM",
  { timeout: 10000 },
  async (t) => {
    const child = spawn(
      process.execPath,
      [path.resolve(__dirname, "../server.js")],
      {
        cwd: root,
        env: {
          ...process.env,
          DATASET_DIR: path.join(root, "release"),
          PORT: "0",
          HOST: "127.0.0.1",
        },
      },
    );
    t.after(() => {
      if (child.exitCode === null) child.kill("SIGKILL");
    });
    let output = "";
    const address = await new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code) => reject(new Error(`Early exit ${code}`)));
      child.stdout.on("data", (chunk) => {
        output += chunk;
        const match = output.match(/http:\/\/127\.0\.0\.1:(\d+)/);
        if (match) resolve(`http://127.0.0.1:${match[1]}`);
      });
    });
    const ready = await fetch(address + "/readyz");
    assert.equal(ready.status, 200);
    assert.deepEqual(await ready.json(), {
      ready: true,
      datasetId: dataset.release.id,
      analyticsAvailable: true,
    });
    const stopped = new Promise((resolve) => child.once("exit", resolve));
    child.kill("SIGTERM");
    assert.equal(await stopped, 0);
  },
);

test("health, readiness and status identify the served snapshot", async () => {
  assert.equal((await json("/healthz")).status, "ok");
  assert.equal((await json("/readyz")).datasetId, dataset.release.id);
  const status = await json("/api/data-status");
  assert.equal(status.stats.speeches, 5);
  assert.equal(status.analyticsAvailable, true);
});
test("all maintenance and paid compute routes reject every public mutation", async () => {
  for (const route of [
    "refresh-all",
    "refresh-speeches",
    "refresh-meps",
    "refresh-perfect",
    "refresh-speeches-full",
    "refresh-sittings",
    "link-historic-meps",
    "rebuild-database",
    "refresh-languages",
    "normalize-parties",
    "normalize-macro-topics",
    "generate-analytics",
    "test-pipeline",
    "refresh-mep-dataset",
    "ai/chat",
  ]) {
    const response = await fetch(`${base}/api/${route}`, { method: "POST" });
    assert.equal(response.status, 405, route);
  }
  assert.equal((await json("/api/localrun")).localrun, false);
});
test("sitting date boundaries and pagination count the same rows", async () => {
  const data = await json(
    "/api/sittings?startDate=2024-03-04&endDate=2025-01-13&limit=1",
  );
  assert.equal(data.meta.total, 2);
  assert.equal(data.data[0].date, "2025-01-13");
  const next = await json(
    "/api/sittings?startDate=2024-03-04&endDate=2025-01-13&limit=1&offset=1",
  );
  assert.equal(next.data[0].date, "2024-03-04");
  assert.equal(
    (await json("/api/sittings?startDate=2026-01-01")).meta.total,
    0,
  );
});
test("invalid and repeated parameters fail explicitly", async () => {
  for (const query of [
    "limit=-1",
    "limit=0",
    "limit=1000000",
    "offset=-1",
    "limit=1&limit=2",
    "startDate=2024-02-30",
    "startDate=2025-01-01&endDate=2024-01-01",
  ])
    assert.equal(
      (await fetch(`${base}/api/sittings?${query}`)).status,
      400,
      query,
    );
  for (const query of [
    "interval=week",
    "from=2024-13",
    "from=2025-01&to=2024-01",
    "top=999",
    "topics={}",
  ])
    assert.equal(
      (await fetch(`${base}/api/analytics/time-series?${query}`)).status,
      400,
      query,
    );
});
test("speaker queries retain unresolved identities and literal search characters", async () => {
  const data = await json("/api/speakers?q=Ana");
  assert.equal(data.meta.total, 1);
  assert.equal(data.data[0].source, "api");
  assert.equal((await json("/api/speakers?q=%25")).meta.total, 0);
  assert.equal((await json("/api/speakers?source=unresolved")).data[0].id, 999);
  const profile = await json("/api/speakers/999");
  assert.equal(profile.label, "Unresolved speaker");
  const rows = await json("/api/speakers/999/speeches");
  assert.equal(rows.meta.total, 1);
  assert.match(rows.data[0].content, /spreadsheet/);
  assert.equal((await fetch(`${base}/api/speakers/10000`)).status, 404);
});
test("speaker dates are chronological and legacy person filter follows speech relationships", async () => {
  const rows = await json("/api/speakers/1/speeches");
  assert.deepEqual(
    rows.data.map((r) => r.date),
    ["2024-03-04", "2024-01-15"],
  );
  assert.equal(
    (await json("/api/speakers/1/speeches?startDate=2024-03-04")).meta.total,
    1,
  );
  assert.equal((await json("/api/speeches?personId=1")).meta.total, 2);
});
test("sitting speeches preserve source text, group changes and source links", async () => {
  const data = await json("/api/sittings/sitting-2024-01-15/speeches");
  assert.equal(data.meta.total, 2);
  assert.match(data.data[0].content, /<img/);
  assert.match(data.data[0].sourceUrl, /CRE-9-2024-01-15_EN.html$/);
  assert.equal(
    (await fetch(`${base}/api/sittings/missing/speeches`)).status,
    404,
  );
});
test("cached series preserves topic selection, dates and intervals", async () => {
  const initial = await json("/api/analytics/time-series?top=1");
  assert.equal(initial.datasets.length, 1);
  const url =
    "/api/analytics/time-series?topics=" +
    encodeURIComponent(JSON.stringify(["Climate & energy"])) +
    "&from=2024-01&to=2024-03";
  const first = await fetch(base + url),
    cold = await first.json();
  assert.equal(first.headers.get("x-cache"), "MISS");
  assert.deepEqual(cold.datasets[0].data, [1, null, 2]);
  const second = await fetch(base + url);
  assert.equal(second.headers.get("x-cache"), "HIT");
  assert.deepEqual(await second.json(), cold);
  const quarter = await json(url + "&interval=quarter");
  assert.deepEqual(quarter.labels, ["2024-Q1"]);
  assert.deepEqual(quarter.datasets[0].data, [3]);
  assert.equal(quarter.coverage[0].observedMonths, 2);
  assert.equal(quarter.coverage[0].months, 3);
  assert.equal(
    (await json("/api/analytics/time-series?topics=NO_SUCH_TOPIC")).datasets
      .length,
    0,
  );
  assert.equal(
    (await json("/api/analytics/time-series?topics=[]")).datasets.length,
    0,
  );
});
test("topic CSV exports the exact series and distinguishes blanks from zero", async () => {
  const response = await fetch(
    base +
      "/api/export/topics?from=2024-01&to=2024-03&topics=" +
      encodeURIComponent('["Budget - oversight"]'),
  );
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type"), /text\/csv/);
  const csv = await response.text();
  assert.match(csv, /2024-01,1,2,1,1/);
  assert.match(csv, /2024-02,,0,0,1/);
  assert.match(csv, /2024-03,0,2,1,1/);
});
test("speech export validates fields, dates, escaping and bounded counts", async () => {
  const count = await json(
    "/api/export/speeches?startDate=2024-01-15&endDate=2024-01-15&countOnly=true",
  );
  assert.equal(count.count, 2);
  const response = await fetch(
    base +
      "/api/export/speeches?startDate=2024-01-15&endDate=2024-01-15&fields=date,speech_content",
  );
  const text = await response.text();
  assert.match(text, /date,speech_content/);
  assert.match(text, /""quotes""/);
  assert.equal(
    (await fetch(base + "/api/export/speeches?fields=id,nonsense")).status,
    400,
  );
  assert.equal(
    (await fetch(base + "/api/export/speeches?fields=constructor")).status,
    400,
  );
  assert.equal(
    (await fetch(base + "/api/export/speeches?fields=id,id")).status,
    400,
  );
  assert.equal(csv("=1+1"), "'=1+1");
  const status = await json("/api/full-dataset/status");
  assert.equal(status.available, true);
  const full = await fetch(base + "/api/download/full-dataset");
  assert.equal(full.status, 200);
  assert.match(full.headers.get("content-disposition"), /\.db/);
  const bytes = Buffer.from(await full.arrayBuffer());
  assert.equal(bytes.length, status.sizeBytes);
  assert.equal(
    require("node:crypto").createHash("sha256").update(bytes).digest("hex"),
    status.sha256,
  );
});
test("legacy bookmarks redirect and security headers prohibit third-party scripts", async () => {
  const response = await fetch(base + "/speech.html?id=sitting-2024-01-15", {
    redirect: "manual",
  });
  assert.equal(response.status, 302);
  assert.equal(
    response.headers.get("location"),
    "/?view=sittings&sitting=sitting-2024-01-15",
  );
  assert.match(
    response.headers.get("content-security-policy"),
    /script-src 'self'/,
  );
  assert.equal((await fetch(base + "/api/missing")).status, 404);
});
