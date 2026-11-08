const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { createFixture } = require("./fixtures/create");
const { get, open, close, exec } = require("../src/data/sqlite");
const { migrateArchive } = require("../src/data/archive-migration");
const { loadDataset, prepare } = require("../src/data/dataset");
const { hash, requestFor, resultFor } = require("../src/insights/model");
const { streamCorpus } = require("../src/insights/corpus");
const {
  verifyApprovals,
  writeReviewed,
  readReviewedPublication,
} = require("../src/insights/release");
const { buildPublication } = require("../src/insights/publication");

test("only exact reviewed current results change the projection or public briefing", async (t) => {
  const directory = await fs.mkdtemp(
    path.join(os.tmpdir(), "eurowatch-reviewed-"),
  );
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const legacy = await createFixture(path.join(directory, "legacy.db"));
  const filename = path.join(directory, "canonical.db");
  const log = () => {};
  await migrateArchive({ source: legacy, output: filename, log });
  const db = await open(filename);
  try {
    await exec(
      db,
      "UPDATE source_revisions SET source_url='https://www.europarl.europa.eu/doceo/document/CRE-9-2024-01-15_EN.html'",
    );
  } finally {
    await close(db);
  }
  const config = {
    model: "fixture",
    promptVersion: "one",
    taxonomyVersion: "one",
    topics: ["Research"],
  };
  const range = {
    filename,
    from: "2024-01-15",
    to: "2024-01-15",
    kinds: ["speech", "debate"],
  };
  const records = [];
  for await (const batch of streamCorpus(range)) records.push(...batch.records);
  const corpus = { records, config, officialEvidence: [] };
  const results = records.map((record) => {
    const first = record.passages[0];
    return resultFor(requestFor(record, config), {
      candidate: {
        topics: [
          { label: "Research", confidence: 0.8, passageIds: [first.id] },
        ],
        claims: [
          {
            text: "Discussed policy",
            quote: first.text,
            passageId: first.id,
            speakerId: first.speakerId,
          },
        ],
        uncertain: false,
      },
    });
  });
  const report = { results };
  const approvals = results.map((result) => ({
    key: result.key,
    approved: true,
    reviewer: "Private reviewer",
    reviewedAt: "2026-11-08T00:00:00Z",
    candidateHash: hash(result.candidate),
  }));
  const approved = await verifyApprovals({
    filename,
    corpus,
    report,
    approvals,
  });
  const releaseId = "7b804a2f-9632-447e-8701-a7d38e980e10";
  const publication = buildPublication({
    corpus,
    report,
    approvals: approvals.filter(
      ({ key }) => results.find((r) => r.key === key).kind === "debate",
    ),
    releaseId,
    weekStart: "2024-01-15",
    publicBaseUrl: "https://eurowatch.example",
  });
  const write = { filename, approved, publication, editionKey: "fixture" };
  assert.ok((await writeReviewed(write)).changed);
  assert.equal((await writeReviewed(write)).changed, 0);
  const output = path.join(directory, "release");
  await prepare({ source: filename, output, releaseId, log });
  const dataset = await loadDataset(output, { verifyHashes: true });
  try {
    assert.equal(dataset.release.id, releaseId);
    assert.equal(
      (
        await get(
          dataset.db,
          "SELECT macro_topic FROM individual_speeches WHERE id=1",
        )
      ).macro_topic,
      "Research",
    );
    assert.equal(
      await get(
        dataset.db,
        "SELECT 1 FROM sqlite_schema WHERE name='private_insight_reviews'",
      ),
      undefined,
    );
    const item = await readReviewedPublication(dataset.db);
    assert.equal(item.releaseId, releaseId);
    assert.equal(item.publicationId, publication.public.publicationId);
    assert.doesNotMatch(JSON.stringify(item), /Private reviewer/);
  } finally {
    await dataset.close();
  }
  const revised = await open(filename);
  try {
    await exec(
      revised,
      "UPDATE individual_speeches SET topic='Changed agenda' WHERE id=1",
    );
  } finally {
    await close(revised);
  }
  await assert.rejects(
    verifyApprovals({ filename, corpus, report, approvals }),
    /changed|missing/,
  );
});
