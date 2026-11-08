const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { open, exec, run, close } = require("../src/data/sqlite");
const {
  makeRecords,
  streamCorpus,
  exportCorpus,
} = require("../src/insights/corpus");
const { requestFor } = require("../src/insights/model");
const config = {
  model: "offline",
  promptVersion: "v1",
  taxonomyVersion: "t1",
  topics: ["Policy"],
};
const sitting = {
  id: "sitting-2026-03-09",
  activity_date: "2026-03-09",
  label: "March 9",
  content:
    '<link rel="canonical" href="https://www.europarl.europa.eu/doceo/document/CRE-10-2026-03-09_EN.html">',
};
const row = {
  id: 7,
  topic: "Energy",
  speech_content: "Energy matters.",
  speaker_name: "A",
  speech_order: 1,
  language: "en",
  mep_id: 12,
};

test("bounded chunks retain every character and flag partial scope; no synthetic official IDs", () => {
  const long = { ...row, speech_content: "Energy matters. ".repeat(100) };
  const { records } = makeRecords(sitting, [long], {
    maxChars: 256,
    kinds: ["debate"],
  });
  assert.ok(records.length > 1);
  assert.equal(
    records.flatMap((r) => r.passages.map((p) => p.text)).join(""),
    long.speech_content,
  );
  for (const r of records) {
    assert.ok(
      r.passages.reduce((n, p) => n + Array.from(p.text).length, 0) <= 256,
    );
    assert.equal(r.scope.complete, false);
    assert.deepEqual(r.officialIds, []);
    assert.equal(r.parentId, "debate:sitting-2026-03-09:7");
    assert.doesNotThrow(() => requestFor(r, config));
  }
});
test("repeated noncontiguous agenda headings remain distinct; provenance gaps are reported", () => {
  const records = makeRecords(
    sitting,
    [row, { ...row, id: 8, topic: "Procedure" }, { ...row, id: 9 }],
    { kinds: ["debate"] },
  ).records;
  assert.deepEqual(
    records.map((r) => r.id),
    [
      "debate:sitting-2026-03-09:7",
      "debate:sitting-2026-03-09:8",
      "debate:sitting-2026-03-09:9",
    ],
  );
  assert.equal(
    makeRecords({ ...sitting, content: "" }, [row]).diagnostics[0].reason,
    "missing_source_url",
  );
});
test("legacy SQLite export is read-only, streamed, deterministic and directly usable by pilot", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "corpus-test-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const filename = path.join(dir, "archive.db");
  const db = await open(filename);
  await exec(
    db,
    "CREATE TABLE sittings(id TEXT PRIMARY KEY,activity_date TEXT,label TEXT,content TEXT); CREATE TABLE individual_speeches(id INTEGER PRIMARY KEY,sitting_id TEXT,speaker_name TEXT,speech_content TEXT,speech_order INTEGER,topic TEXT);",
  );
  await run(db, "INSERT INTO sittings VALUES (?,?,?,?)", [
    sitting.id,
    sitting.activity_date,
    sitting.label,
    sitting.content,
  ]);
  await run(db, "INSERT INTO individual_speeches VALUES (7,?,?,?,1,?)", [
    sitting.id,
    row.speaker_name,
    row.speech_content,
    row.topic,
  ]);
  await close(db);
  const before = fs.readFileSync(filename);
  const batches = [];
  for await (const batch of streamCorpus({ filename })) batches.push(batch);
  assert.equal(batches[0].records.length, 3);
  const output = path.join(dir, "corpus.json");
  await exportCorpus({ filename, output, config });
  const exported = JSON.parse(fs.readFileSync(output));
  assert.deepEqual(exported.records, batches[0].records);
  assert.deepEqual(fs.readFileSync(filename), before);
  for (const record of exported.records)
    assert.doesNotThrow(() => requestFor(record, config));
});
test("canonical source revision and known official IDs survive without leaking sitting ID to debates", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "canonical-corpus-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const filename = path.join(dir, "archive.db");
  const db = await open(filename);
  await exec(
    db,
    `CREATE TABLE sittings(id TEXT PRIMARY KEY,activity_date TEXT,label TEXT,content TEXT);
    CREATE TABLE individual_speeches(id INTEGER PRIMARY KEY,sitting_id TEXT,speaker_name TEXT,speech_content TEXT,speech_order INTEGER);
    CREATE TABLE canonical_migrations(version INTEGER); INSERT INTO canonical_migrations VALUES(1);
    CREATE TABLE sitting_provenance(sitting_id TEXT, revision_id INTEGER);
    CREATE TABLE speech_provenance(speech_id INTEGER, revision_id INTEGER, official_id TEXT);
    CREATE TABLE source_records(id INTEGER,provider TEXT,official_id TEXT);
    CREATE TABLE source_revisions(id INTEGER,source_id INTEGER,payload_sha256 TEXT,source_url TEXT);
    INSERT INTO source_records VALUES(1,'europarl','CRE-10-2026-03-09');
    INSERT INTO source_revisions VALUES(1,1,'source-hash','https://data.europarl.europa.eu/transcript/1');
    INSERT INTO sitting_provenance VALUES('sitting-2026-03-09',1);
    INSERT INTO speech_provenance VALUES(7,1,'official-speech-7');`,
  );
  await run(db, "INSERT INTO sittings VALUES (?,?,?,?)", [
    sitting.id,
    sitting.activity_date,
    sitting.label,
    "",
  ]);
  await run(db, "INSERT INTO individual_speeches VALUES (7,?,?,?,1)", [
    sitting.id,
    row.speaker_name,
    row.speech_content,
  ]);
  await close(db);
  for await (const batch of streamCorpus({ filename })) {
    assert.deepEqual(
      batch.records.find((r) => r.kind === "speech").officialIds,
      ["official-speech-7"],
    );
    assert.deepEqual(
      batch.records.find((r) => r.kind === "sitting").officialIds,
      ["CRE-10-2026-03-09"],
    );
    assert.deepEqual(
      batch.records.find((r) => r.kind === "debate").officialIds,
      [],
    );
    assert.equal(
      batch.records[0].passages[0].sourceRevision,
      "sha256:source-hash",
    );
  }
});
