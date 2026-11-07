const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { once } = require("node:events");
const { setTimeout: delay } = require("node:timers/promises");
const { createFixture } = require("./fixtures/create");
const { hashFile, prepare } = require("../src/data/dataset");
const { createLocalPreview } = require("../src/explorer/local-refresh");

test("local button runs one import and switches the preview after validation", async (t) => {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "eurowatch-button-test-"),
  );
  const source = await createFixture(path.join(root, "source.db"));
  const directory = path.join(root, "release");
  const baseline = await prepare({ source, output: directory, log() {} });
  const original = path.join(directory, "ep_data.db");
  const beforeHash = await hashFile(original);
  const date = "2026-03-09";
  const calendar = Promise.withResolvers();
  let requests = 0;
  const preview = await createLocalPreview({
    directory,
    start: date,
    end: date,
    async request(url) {
      if (url.endsWith("/meetings")) {
        requests++;
        return calendar.promise;
      }
      assert.match(url, /CRE-10-2026-03-09_EN\.html$/);
      return `<html><body>
<table><tr><td class="doc_title"><img src="arrow_title_doc.gif">1. Research</td></tr></table>
<p>Ana Example (PPE). – We should discuss how Parliament can support international research.</p>
</body></html>`;
    },
  });
  const server = preview.app.listen(0, "127.0.0.1");
  t.after(async () => {
    calendar.resolve({ data: [] });
    await new Promise((resolve) => server.close(resolve));
    await preview.close();
    await fs.rm(root, { recursive: true, force: true });
  });
  await once(server, "listening");
  const url = `http://127.0.0.1:${server.address().port}`;
  const endpoint = `${url}/api/local-refresh`;
  const headers = { Origin: url, "X-EUROWATCH-Action": "refresh" };
  assert.deepEqual(
    await fetch(`${url}/api/localrun`).then((res) => res.json()),
    { localrun: true },
  );
  assert.equal(
    (
      await fetch(endpoint, {
        method: "POST",
        headers: { ...headers, Origin: "https://example.com" },
      })
    ).status,
    403,
  );
  assert.equal((await fetch(endpoint, { method: "POST" })).status, 403);
  const trigger = () => fetch(endpoint, { method: "POST", headers });
  assert.equal((await trigger()).status, 202);
  assert.equal((await trigger()).status, 202);
  const readiness = () => fetch(`${url}/readyz`).then((res) => res.json());
  assert.equal((await readiness()).datasetId, baseline.id);
  calendar.resolve({
    data: [
      {
        activity_date: date,
        had_activity_type: "def/ep-activities/PLENARY_SITTING",
        recorded_in_a_realization_of: [`eli/dl/doc/CRE-10-${date}`],
      },
    ],
  });
  let job;
  for (let attempt = 0; attempt < 100; attempt++) {
    job = await fetch(endpoint).then((res) => res.json());
    if (!job.running) break;
    await delay(25);
  }
  assert.equal(job.running, false);
  assert.match(job.message, /Added 1 speech/);
  assert.equal(requests, 1);
  assert.notEqual(job.datasetId, baseline.id);
  assert.equal((await readiness()).datasetId, job.datasetId);
  const list = await fetch(`${url}/api/sittings?startDate=${date}`).then(
    (res) => res.json(),
  );
  assert.equal(list.data[0].speechCount, 1);
  assert.equal(await hashFile(original), beforeHash);
});
