const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { backfill } = require("../src/insights/backfill");
const { requestFor } = require("../src/insights/model");
const config = {
  model: "fake",
  promptVersion: "v1",
  taxonomyVersion: "t1",
  topics: ["Climate"],
};
const record = {
  id: "debate:1",
  kind: "debate",
  inputRevision: "r1",
  effectiveAt: "2026-03-09T00:00:00Z",
  term: 10,
  language: "en",
  officialIds: [],
  passages: [
    {
      id: "p1",
      text: "Clean energy.",
      sourceUrl: "https://www.europarl.europa.eu/test",
      sourceRevision: "s1",
      speakerId: null,
    },
  ],
};
const candidate = {
  topics: [{ label: "Climate", confidence: 0.8, passageIds: ["p1"] }],
  claims: [
    {
      text: "Clean energy is discussed.",
      passageId: "p1",
      quote: "Clean energy.",
      speakerId: null,
    },
  ],
  uncertain: false,
};
function setup(t) {
  const checkpoint = fs.mkdtempSync(path.join(os.tmpdir(), "insights-test-"));
  t.after(() => fs.rmSync(checkpoint, { recursive: true, force: true }));
  return {
    checkpoint,
    config,
    records: [record],
    budgetUsd: 1,
    provider: {
      reserve: () => 0.1,
      generate: async () => ({ candidate, usage: { costUsd: 0.05 } }),
    },
  };
}
test("restart reuses output; source and taxonomy changes create new versioned candidates", async (t) => {
  const options = setup(t);
  const first = await backfill(options);
  assert.equal(first.attempts, 1);
  const replay = await backfill(options);
  assert.equal(replay.attempts, 0);
  assert.equal(replay.reused, 1);
  assert.equal(replay.reservedUsd, 0.1);
  const revised = await backfill({
    ...options,
    records: [{ ...record, inputRevision: "r2" }],
  });
  assert.equal(revised.attempts, 1);
  await backfill({ ...options, config: { ...config, taxonomyVersion: "t2" } });
  assert.equal(
    fs.readdirSync(path.join(options.checkpoint, "objects")).length,
    3,
  );
  assert.equal(first.publicationAllowed, false);
});
test("failed attempts retain reservations; retries are explicit and bounded", async (t) => {
  const options = setup(t);
  const failed = await backfill({
    ...options,
    provider: {
      reserve: () => 0.1,
      generate: async () => {
        throw new Error("secret provider diagnostic");
      },
    },
  });
  assert.equal(failed.results[0].status, "failed");
  assert.equal((await backfill(options)).attempts, 0);
  const blocked = await backfill({
    ...options,
    retryFailed: true,
    budgetUsd: 0.1,
  });
  assert.equal(blocked.results[0].reason, "budget");
  const retry = await backfill({ ...options, retryFailed: true });
  assert.equal(retry.results[0].status, "candidate");
  assert.equal(retry.reservedUsd, 0.2);
  assert.ok(
    !fs
      .readFileSync(path.join(options.checkpoint, "events.jsonl"), "utf8")
      .includes("secret"),
  );
});
test("interrupted paid attempt does not automatically spend again", async (t) => {
  const options = setup(t);
  const key = requestFor(record, config).key;
  fs.writeFileSync(
    path.join(options.checkpoint, "events.jsonl"),
    JSON.stringify({ type: "reserved", key, costUsd: 0.1 }) + "\n",
  );
  const report = await backfill(options);
  assert.equal(report.results[0].status, "interrupted");
  assert.equal(report.attempts, 0);
  assert.equal(
    (await backfill({ ...options, retryFailed: true })).reservedUsd,
    0.2,
  );
});
test("bounded batches and exclusive writer preserve durable progress", async (t) => {
  const options = setup(t);
  const records = [record, { ...record, id: "debate:2" }];
  const batch = await backfill({ ...options, records, maxItems: 1 });
  assert.equal(batch.results[1].reason, "batch_limit");
  const resumed = await backfill({ ...options, records, maxItems: 1 });
  assert.equal(resumed.reused, 1);
  assert.equal(resumed.attempts, 1);
  fs.mkdirSync(path.join(options.checkpoint, "writer.lock"));
  await assert.rejects(backfill(options), /locked/);
});
test("corrupt ledger fails closed without issuing requests", async (t) => {
  const options = setup(t);
  fs.writeFileSync(path.join(options.checkpoint, "events.jsonl"), "{broken");
  await assert.rejects(backfill(options), SyntaxError);
  assert.equal(
    fs.existsSync(path.join(options.checkpoint, "writer.lock")),
    false,
  );
});
