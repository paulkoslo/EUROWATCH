const fs = require("node:fs/promises");
const path = require("node:path");
const os = require("node:os");
const { randomUUID } = require("node:crypto");
const { open, close, exec, get, run } = require("../data/sqlite");
const { prepare } = require("../data/dataset");
const { canonicalVersion } = require("../data/canonical-schema");
const { claimLease } = require("../data/hosted-refresh");
const { createReleaseStorageFromEnv } = require("../data/release-storage");
const { streamCorpus } = require("./corpus");
const { requestFor, hash, validateCandidate } = require("./model");
const { buildPublication } = require("./publication");
const { options, read } = require("./cli");

async function verifyApprovals({ filename, corpus, report, approvals }) {
  const results = new Map(report.results.map((result) => [result.key, result]));
  const records = new Map(corpus.records.map((record) => [record.id, record]));
  const requested = new Map();
  if (!approvals.length)
    throw new Error("Choose at least one reviewed candidate.");
  for (const approval of approvals) {
    const result = results.get(approval.key);
    const record = records.get(result?.recordId);
    if (
      !record ||
      requested.has(record.id) ||
      result.status !== "candidate" ||
      approval.approved !== true ||
      !approval.reviewer?.trim() ||
      !Number.isFinite(Date.parse(approval.reviewedAt)) ||
      approval.candidateHash !== hash(result.candidate)
    )
      throw new Error(
        "Each result needs a unique review of its exact candidate.",
      );
    if (record.scope?.complete === false && approval.allowPartial !== true)
      throw new Error("Approve partial evidence explicitly.");
    requested.set(record.id, { record, result, approval });
  }
  const dates = [...requested.values()]
    .map(({ record }) => record.effectiveAt.slice(0, 10))
    .sort();
  const kinds = [
    ...new Set([...requested.values()].map(({ record }) => record.kind)),
  ];
  const approved = [];
  for await (const { records: current } of streamCorpus({
    filename,
    from: dates[0],
    to: dates.at(-1),
    kinds,
  })) {
    for (const record of current) {
      const reviewed = requested.get(record.id);
      if (!reviewed) continue;
      const request = requestFor(
        record,
        corpus.config,
        corpus.officialEvidence,
      );
      if (
        request.key !== reviewed.result.key ||
        reviewed.result.inputRevision !== record.inputRevision
      )
        throw new Error(`Reviewed record changed: ${record.id}`);
      const candidate = validateCandidate(reviewed.result.candidate, request);
      if (candidate.uncertain)
        throw new Error(
          "Uncertain results need correction before publication.",
        );
      approved.push({ ...reviewed, record, request, candidate });
      requested.delete(record.id);
    }
  }
  if (requested.size)
    throw new Error(
      "One or more reviewed records are missing from the current archive.",
    );
  return approved;
}

async function writeReviewed({ filename, approved, publication, editionKey }) {
  const db = await open(filename);
  let changed = 0;
  try {
    if (!(await canonicalVersion(db)))
      throw new Error("Use a canonical checkpoint.");
    await exec(
      db,
      `
      PRAGMA foreign_keys=ON;
      BEGIN IMMEDIATE;
      CREATE TABLE IF NOT EXISTS public_insights (
        id TEXT PRIMARY KEY, release_id TEXT NOT NULL, week_start TEXT NOT NULL, published_at TEXT NOT NULL,
        publication_json TEXT NOT NULL CHECK(json_valid(publication_json))
      );
      CREATE TABLE IF NOT EXISTS private_insight_editions (
        key TEXT PRIMARY KEY, publication_id TEXT NOT NULL REFERENCES public_insights(id)
      );
      CREATE TABLE IF NOT EXISTS private_insight_reviews (
        key TEXT PRIMARY KEY, evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json))
      );
    `,
    );
    try {
      for (const item of approved) {
        const { record, request, candidate, approval, result } = item;
        const audit = JSON.stringify({
          approval,
          versions: request.versions,
          inputRevision: record.inputRevision,
          candidate,
        });
        const previous = await get(
          db,
          "SELECT evidence_json FROM private_insight_reviews WHERE key=?",
          [request.key],
        );
        if (
          previous &&
          JSON.parse(previous.evidence_json).approval.candidateHash !==
            approval.candidateHash
        )
          throw new Error(
            "A different result already exists for this request. Change the enrichment version.",
          );
        await run(
          db,
          "INSERT OR IGNORE INTO private_insight_reviews VALUES (?,?)",
          [request.key, audit],
        );
        if (
          record.kind !== "speech" ||
          !/^speech:\d+$/.test(record.id) ||
          record.scope?.complete === false
        )
          continue;
        const id = Number(record.id.split(":")[1]);
        const revision = await get(
          db,
          "SELECT revision_id FROM speech_provenance WHERE speech_id=?",
          [id],
        );
        if (!revision)
          throw new Error(`Missing canonical provenance for speech ${id}.`);
        const topic = [...candidate.topics].sort(
          (a, b) =>
            b.confidence - a.confidence || a.label.localeCompare(b.label),
        )[0];
        const versions = [
          request.versions.model,
          request.versions.promptVersion,
          corpusTaxonomy(request),
        ];
        const keys = [id, revision.revision_id, ...versions];
        const existing = await get(
          db,
          `
          SELECT evidence_json FROM speech_enrichment
          WHERE speech_id=? AND revision_id=? AND model_version=? AND prompt_version=? AND taxonomy_version=?
        `,
          keys,
        );
        if (
          existing &&
          JSON.parse(existing.evidence_json).candidateHash !==
            approval.candidateHash
        )
          throw new Error(
            `Speech ${id} already has a different result for these versions.`,
          );
        await run(
          db,
          `
          INSERT OR IGNORE INTO speech_enrichment
            (speech_id,revision_id,model_version,prompt_version,taxonomy_version,outcome,evidence_json,macro_topic,
             macro_confidence,macro_classified_by,macro_classified_at,macro_classification_cost)
          VALUES (?,?,?,?,?,'valid',?,?,?,?,?,?)
        `,
          [
            ...keys,
            JSON.stringify({
              candidateHash: approval.candidateHash,
              requestKey: request.key,
              topics: candidate.topics,
            }),
            topic.label,
            topic.confidence,
            request.versions.model,
            Math.floor(Date.parse(result.generatedAt) / 1000),
            result.usage?.costUsd ?? null,
          ],
        );
        changed += (
          await run(
            db,
            `
          INSERT INTO published_speech_enrichment VALUES (?,?,?,?,?)
          ON CONFLICT(speech_id) DO UPDATE SET revision_id=excluded.revision_id,
            model_version=excluded.model_version,prompt_version=excluded.prompt_version,taxonomy_version=excluded.taxonomy_version
          WHERE revision_id IS NOT excluded.revision_id OR model_version IS NOT excluded.model_version
            OR prompt_version IS NOT excluded.prompt_version OR taxonomy_version IS NOT excluded.taxonomy_version
        `,
            keys,
          )
        ).changes;
      }
      const previousEdition = editionKey
        ? await get(
            db,
            "SELECT publication_id FROM private_insight_editions WHERE key=?",
            [editionKey],
          )
        : null;
      if (publication && !previousEdition) {
        const entry = publication.public;
        changed += (
          await run(
            db,
            "INSERT OR IGNORE INTO public_insights VALUES (?,?,?,?,?)",
            [
              entry.publicationId,
              entry.releaseId,
              entry.weekStart,
              entry.publishedAt,
              JSON.stringify(entry),
            ],
          )
        ).changes;
      }
      if (publication && !previousEdition && editionKey)
        await run(db, "INSERT INTO private_insight_editions VALUES (?,?)", [
          editionKey,
          publication.public.publicationId,
        ]);
      await exec(db, "COMMIT");
      return {
        changed,
        publicationId:
          previousEdition?.publication_id || publication?.public.publicationId,
      };
    } catch (error) {
      await exec(db, "ROLLBACK");
      throw error;
    }
  } finally {
    await close(db);
  }
}

function corpusTaxonomy(request) {
  return `${request.versions.taxonomyVersion}:${hash(request.input.taxonomy).slice(0, 12)}`;
}

async function readReviewedPublication(db, id) {
  if (
    !(await get(db, "SELECT 1 FROM sqlite_schema WHERE name='public_insights'"))
  )
    return null;
  const row = id
    ? await get(db, "SELECT publication_json FROM public_insights WHERE id=?", [
        id,
      ])
    : await get(
        db,
        "SELECT publication_json FROM public_insights ORDER BY week_start DESC,published_at DESC,id LIMIT 1",
      );
  return row ? JSON.parse(row.publication_json) : null;
}

async function publishReviewed({
  storage,
  corpus,
  report,
  approvals,
  weekStart,
  publicBaseUrl,
  workRoot = os.tmpdir(),
}) {
  const runId = randomUUID();
  const lease = await claimLease(storage, runId);
  if (!lease) throw new Error("Another archive update is running.");
  let work;
  try {
    const current = await storage.readCurrent();
    if (!current?.value.checkpoint)
      throw new Error("The archive has not been seeded.");
    const { manifest } = await storage.inspectRelease(current.value);
    await fs.mkdir(workRoot, { recursive: true, mode: 0o700 });
    work = await fs.mkdtemp(path.join(workRoot, "eurowatch-insights-"));
    const space = await fs.statfs(work, { bigint: true });
    if (
      space.bavail * space.bsize <
      BigInt(current.value.checkpoint.bytes) * 2n + 536_870_912n
    )
      throw new Error(
        "The workspace needs room for the checkpoint and public release.",
      );
    const filename = path.join(work, "archive.db");
    await storage.downloadCheckpoint(current.value.checkpoint, filename);
    const approved = await verifyApprovals({
      filename,
      corpus,
      report,
      approvals,
    });
    const releaseId = randomUUID();
    const briefings = approved.filter(({ record }) => record.kind !== "speech");
    const publication = briefings.length
      ? buildPublication({
          corpus,
          report,
          approvals: briefings.map(({ approval }) => approval),
          releaseId,
          weekStart,
          publicBaseUrl,
        })
      : null;
    const editionKey = publication
      ? hash({
          weekStart,
          publicBaseUrl,
          reviews: briefings
            .map(({ approval }) => [approval.key, approval.candidateHash])
            .sort(),
        })
      : null;
    const written = await writeReviewed({
      filename,
      approved,
      publication,
      editionKey,
    });
    if (!written.changed)
      return {
        releaseId: manifest.id,
        publicationId: written.publicationId,
        changed: false,
      };
    const output = path.join(work, "release");
    await prepare({
      source: filename,
      output,
      refresh: manifest.refresh,
      releaseId,
    });
    const publicRef = await storage.uploadRelease(output);
    const checkpoint = await storage.uploadCheckpoint(filename, runId);
    await storage.publishRelease(
      { ...publicRef, checkpoint },
      { expectedETag: current.etag, beforeCommit: lease.assertHeld },
    );
    return {
      releaseId,
      publicationId: written.publicationId,
      changed: true,
    };
  } finally {
    try {
      await lease.close();
    } finally {
      if (work) await fs.rm(work, { recursive: true, force: true });
    }
  }
}

if (require.main === module) {
  require("dotenv").config({ quiet: true });
  const opts = options(process.argv.slice(2));
  const storage = createReleaseStorageFromEnv();
  if (!storage) throw new Error("Configure shared release storage.");
  Promise.resolve()
    .then(() =>
      publishReviewed({
        storage,
        corpus: read(opts.corpus),
        report: read(opts.report),
        approvals: read(opts.approvals),
        weekStart: opts.week,
        publicBaseUrl: opts["base-url"],
        workRoot: process.env.DATA_UPDATE_WORK_DIR,
      }),
    )
    .then((result) => console.log(JSON.stringify(result)))
    .catch((error) => {
      console.error(error.message);
      process.exitCode = 1;
    })
    .finally(() => storage.close());
}

module.exports = {
  publishReviewed,
  verifyApprovals,
  writeReviewed,
  readReviewedPublication,
};
