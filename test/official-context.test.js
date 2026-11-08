const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { once } = require("node:events");
const {
  collectOfficialContext,
  validateOfficialContext,
} = require("../src/insights/official-context");
const { storeOfficialContext } = require("../src/insights/context-view");
const { createFixture } = require("./fixtures/create");
const { prepare, loadDataset } = require("../src/data/dataset");
const { createApp } = require("../src/explorer/app");

test("official context follows exact Parliament IDs and serves one verified release", async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-context-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const request = async (href) => {
    const url = new URL(href);
    assert.equal(url.origin, "https://data.europarl.europa.eu");
    const endpoint = url.pathname.slice("/api/v2/".length);
    const records = {
      "procedures/2026-0801": [
        { id: "eli/dl/proc/2026-0801", label: { en: "Report on oversight" } },
      ],
      "procedures/2026-0801/events": [
        {
          id: "eli/dl/event/2026-0801-ANPRO-2026-02-09",
          activity_date: "2026-02-09",
          had_activity_type: "def/ep-activities/ANNOUNCEMENT",
        },
        { id: "eli/dl/event/UNDATED" },
      ],
      "parliamentary-questions/E-10-2026-000002": [
        {
          id: "eli/dl/doc/E-10-2026-000002",
          label: { en: "<script>alert(1)</script>" },
          document_date: "2026-01-02",
        },
      ],
      "committee-documents/AFCO-AD-592152": [
        { id: "eli/dl/doc/AFCO-AD-592152", label: { en: "Committee opinion" } },
      ],
      "committee-documents/AFCO-AD-111111": [
        { id: "eli/dl/doc/OTHER", label: "Wrong identity" },
      ],
    };
    return { data: records[endpoint] ?? [] };
  };
  const context = await collectOfficialContext({
    procedures: ["2026-0801"],
    questions: ["E-10-2026-000002"],
    committees: ["AFCO-AD-592152", "AFCO-AD-111111"],
    get: request,
  });
  assert.equal(context.records.length, 3);
  assert.equal(context.records[0].events.length, 1);
  assert.equal(context.records[0].coverage, "partial");
  assert.deepEqual(context.issues, [
    { kind: "committees", id: "AFCO-AD-111111", outcome: "source_error" },
  ]);
  assert.throws(
    () => validateOfficialContext({ ...context, records: [] }),
    /hash mismatch/,
  );
  const source = await createFixture(path.join(dir, "archive.db"));
  assert.equal(await storeOfficialContext(source, context), 1);
  assert.equal(await storeOfficialContext(source, context), 0);
  const output = path.join(dir, "release");
  await prepare({ source, output, log() {} });
  const dataset = await loadDataset(output, { verifyHashes: true });
  const server = createApp(dataset).listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(async () => {
    server.close();
    await dataset.close();
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  const api = await fetch(base + "/api/official-context");
  assert.equal(api.headers.get("x-dataset-id"), dataset.release.id);
  assert.equal((await api.json()).context.contextId, context.contextId);
  const html = await (await fetch(base + "/parliament")).text();
  assert.match(html, /Report on oversight/);
  assert.match(html, /Partial timeline/);
  assert.match(html, /&lt;script&gt;/);
  assert.doesNotMatch(html, /<script>/);
});
