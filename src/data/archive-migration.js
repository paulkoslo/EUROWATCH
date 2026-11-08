const fs = require("node:fs/promises");
const path = require("node:path");
const { createHash } = require("node:crypto");
const { all, backup, close, exec, get, open, run } = require("./sqlite");
const { migrate } = require("./schema");
const {
  CANONICAL_VERSION,
  canonicalVersion,
  createCanonicalSchema,
  legacyEnrichmentFields,
} = require("./canonical-schema");

const batchSize = 128;
const digest = (data) => createHash("sha256").update(data).digest("hex");
const fileOptions = { mode: 0o600 };

async function rowDigest(db, table, key) {
  const hash = createHash("sha256");
  let previous = null;
  while (true) {
    const rows = await all(
      db,
      `SELECT * FROM ${table} ${previous === null ? "" : `WHERE ${key} > ?`}
       ORDER BY ${key} LIMIT ${batchSize}`,
      previous === null ? [] : [previous],
    );
    if (!rows.length) break;
    for (const row of rows) hash.update(JSON.stringify(row) + "\n");
    previous = rows.at(-1)[key];
  }
  return hash.digest("hex");
}

async function snapshot(db) {
  const byDate = await all(
    db,
    `
    SELECT s.activity_date AS date, COUNT(DISTINCT s.id) AS sittings, COUNT(i.id) AS speeches
    FROM sittings s LEFT JOIN individual_speeches i ON i.sitting_id=s.id
    GROUP BY s.activity_date ORDER BY s.activity_date
  `,
  );
  const hashes = {};
  for (const table of ["sittings", "individual_speeches", "meps", "speeches"])
    hashes[table] = await rowDigest(db, table, "id");
  return { byDate, hashes };
}

async function seedSources(db) {
  // Legacy HTML is already retained verbatim in the sitting row. Do not claim it
  // is an official revision or duplicate the raw bytes in the private database.
  const sittings = await all(
    db,
    `SELECT id, docIdentifier, notationId FROM sittings ORDER BY id`,
  );
  for (const { id } of sittings) {
    const { content } = await get(
      db,
      "SELECT content FROM sittings WHERE id=?",
      [id],
    );
    const source = await run(
      db,
      `
      INSERT INTO source_records (provider,kind,source_key) VALUES ('legacy','transcript',?)
    `,
      [id],
    );
    const revision = await run(
      db,
      `
      INSERT INTO source_revisions
        (source_id,format,payload_sha256,legacy_payload_ref)
      VALUES (?,'archive-row',?,?)
    `,
      [source.lastID, digest(JSON.stringify(content)), id],
    );
    await run(db, "INSERT INTO sitting_sources VALUES (?,?)", [
      id,
      source.lastID,
    ]);
    await run(db, "INSERT INTO sitting_provenance VALUES (?,?)", [
      id,
      revision.lastID,
    ]);
  }
  await exec(
    db,
    `
    INSERT INTO speech_provenance (speech_id,revision_id)
    SELECT i.id, p.revision_id FROM individual_speeches i
    JOIN sitting_provenance p ON p.sitting_id=i.sitting_id;
  `,
  );
}

async function seedEnrichment(db) {
  const hasLegacyValue = legacyEnrichmentFields
    .map((field) => `i.${field} IS NOT NULL`)
    .join(" OR ");
  await exec(
    db,
    `
    INSERT INTO speech_enrichment (
      speech_id,revision_id,model_version,prompt_version,taxonomy_version,outcome,
      ${legacyEnrichmentFields.join(",")}
    )
    SELECT i.id,p.revision_id,'legacy','unknown','legacy','legacy',
      ${legacyEnrichmentFields.map((field) => `i.${field}`).join(",")}
    FROM individual_speeches i JOIN speech_provenance p ON p.speech_id=i.id
    WHERE ${hasLegacyValue};
    INSERT INTO published_speech_enrichment
    SELECT speech_id,revision_id,model_version,prompt_version,taxonomy_version
    FROM speech_enrichment WHERE outcome='legacy';
  `,
  );
}

async function verify(db) {
  const integrity = await get(db, "PRAGMA quick_check");
  if (Object.values(integrity)[0] !== "ok")
    throw new Error("SQLite integrity check failed.");
  const foreignKey = await get(db, "PRAGMA foreign_key_check");
  if (foreignKey)
    throw new Error(
      `Foreign key violation in ${foreignKey.table}, row ${foreignKey.rowid}`,
    );
  const report = await get(
    db,
    `
    SELECT
      (SELECT COUNT(*) FROM sittings) AS sittings,
      (SELECT COUNT(*) FROM individual_speeches) AS speeches,
      (SELECT COUNT(*) FROM sitting_provenance) AS sittingSources,
      (SELECT COUNT(*) FROM speech_provenance) AS speechSources,
      (SELECT COUNT(*) FROM speech_enrichment WHERE outcome='legacy') AS legacyEnrichments,
      (SELECT COUNT(*) FROM sitting_provenance p JOIN source_revisions v ON v.id=p.revision_id
        JOIN source_records r ON r.id=v.source_id WHERE r.official_id IS NULL) AS unresolvedSittingSources,
      (SELECT COUNT(*) FROM sittings WHERE NULLIF(TRIM(docIdentifier),'') IS NOT NULL
        OR NULLIF(TRIM(notationId),'') IS NOT NULL) AS legacySittingsWithCandidateIdentifiers,
      (SELECT COUNT(*) FROM individual_speeches i LEFT JOIN meps m ON m.id=i.mep_id
        WHERE m.id IS NULL) AS unresolvedSpeakers,
      (SELECT COUNT(*) FROM (
        SELECT sitting_id,speech_order FROM individual_speeches WHERE speech_order IS NOT NULL
        GROUP BY sitting_id,speech_order HAVING COUNT(*)>1
      )) AS duplicateSpeechOrderGroups
  `,
  );
  if (!report.sittings || !report.speeches)
    throw new Error("The archive has no sittings or speeches.");
  if (
    report.sittings !== report.sittingSources ||
    report.speeches !== report.speechSources
  )
    throw new Error(
      "Archive provenance does not cover all sitting and speech rows.",
    );
  return report;
}

async function migrateArchive({ source, output, log = console.log }) {
  source = path.resolve(source);
  output = path.resolve(output);
  await fs.mkdir(path.dirname(output), { recursive: true });
  const work = await fs.mkdtemp(
    path.join(path.dirname(output), ".archive-migration-"),
  );
  const filename = path.join(work, "archive.db");
  try {
    await fs.writeFile(filename, "", fileOptions);
    const original = await open(source, { readonly: true });
    try {
      await backup(original, filename);
    } finally {
      await close(original);
    }
    log("Created a consistent archive copy.");
    const db = await open(filename);
    let report;
    try {
      await exec(db, "PRAGMA journal_mode=DELETE; PRAGMA foreign_keys=ON;");
      await migrate(db);
      if (await canonicalVersion(db)) {
        const stored = await get(
          db,
          "SELECT report FROM canonical_migrations WHERE version=?",
          [CANONICAL_VERSION],
        );
        report = JSON.parse(stored.report);
        await verify(db);
      } else {
        const before = await snapshot(db);
        log(
          "Recorded archive coverage and row digests. Migrating source provenance and legacy labels.",
        );
        await exec(db, "BEGIN IMMEDIATE");
        try {
          await createCanonicalSchema(db);
          await seedSources(db);
          await seedEnrichment(db);
          report = {
            version: CANONICAL_VERSION,
            ...(await verify(db)),
            ...before,
          };
          const after = await snapshot(db);
          if (JSON.stringify(before) !== JSON.stringify(after))
            throw new Error("Archive parity check failed.");
          await run(db, "INSERT INTO canonical_migrations VALUES (?,?)", [
            CANONICAL_VERSION,
            JSON.stringify(report),
          ]);
          await exec(db, "COMMIT");
        } catch (error) {
          await exec(db, "ROLLBACK");
          throw error;
        }
      }
    } finally {
      await close(db);
    }
    await fs.link(filename, output);
    log(
      `Verified ${report.sittings} sittings and ${report.speeches} speeches.`,
    );
    return report;
  } finally {
    await fs.rm(work, { recursive: true, force: true });
  }
}

module.exports = { migrateArchive, archiveSnapshot: snapshot };
