const { exec, get } = require("./sqlite");

const CANONICAL_VERSION = 1;
const legacyEnrichmentFields = [
  "macro_topic",
  "macro_specific_focus",
  "macro_confidence",
  "macro_classified_by",
  "macro_classified_at",
  "macro_classification_cost",
];

async function canonicalVersion(db) {
  const table = await get(
    db,
    "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='canonical_migrations'",
  );
  if (!table) return 0;
  const { version } = await get(
    db,
    "SELECT COALESCE(MAX(version),0) AS version FROM canonical_migrations",
  );
  if (version !== CANONICAL_VERSION)
    throw new Error(`Unsupported canonical archive version: ${version}`);
  return version;
}

async function createCanonicalSchema(db) {
  await exec(
    db,
    `
    CREATE TABLE canonical_migrations (
      version INTEGER PRIMARY KEY,
      report TEXT NOT NULL
    );
    CREATE TABLE source_records (
      id INTEGER PRIMARY KEY,
      provider TEXT NOT NULL,
      kind TEXT NOT NULL,
      source_key TEXT NOT NULL,
      official_id TEXT,
      UNIQUE (provider, kind, source_key)
    );
    CREATE TABLE source_revisions (
      id INTEGER PRIMARY KEY,
      source_id INTEGER NOT NULL REFERENCES source_records(id),
      format TEXT NOT NULL,
      payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64),
      source_url TEXT,
      retrieved_at TEXT,
      payload BLOB,
      legacy_payload_ref TEXT,
      UNIQUE (source_id, format, payload_sha256)
    );
    CREATE TABLE sitting_sources (
      sitting_id TEXT NOT NULL REFERENCES sittings(id) ON DELETE CASCADE,
      source_id INTEGER NOT NULL REFERENCES source_records(id),
      PRIMARY KEY (sitting_id, source_id),
      UNIQUE (source_id)
    );
    CREATE TABLE sitting_provenance (
      sitting_id TEXT PRIMARY KEY REFERENCES sittings(id) ON DELETE CASCADE,
      revision_id INTEGER NOT NULL REFERENCES source_revisions(id)
    );
    CREATE TABLE speech_provenance (
      speech_id INTEGER PRIMARY KEY REFERENCES individual_speeches(id) ON DELETE CASCADE,
      revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
      official_id TEXT,
      UNIQUE (revision_id, official_id)
    );
    CREATE TABLE speech_enrichment (
      speech_id INTEGER NOT NULL REFERENCES individual_speeches(id) ON DELETE CASCADE,
      revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
      model_version TEXT NOT NULL,
      prompt_version TEXT NOT NULL,
      taxonomy_version TEXT NOT NULL,
      outcome TEXT NOT NULL CHECK(outcome IN ('legacy','valid','uncertain','failed','skipped')),
      evidence_json TEXT CHECK(evidence_json IS NULL OR json_valid(evidence_json)),
      macro_topic TEXT,
      macro_specific_focus TEXT,
      macro_confidence REAL,
      macro_classified_by TEXT,
      macro_classified_at INTEGER,
      macro_classification_cost REAL,
      PRIMARY KEY (speech_id, revision_id, model_version, prompt_version, taxonomy_version)
    );
    CREATE TABLE published_speech_enrichment (
      speech_id INTEGER PRIMARY KEY,
      revision_id INTEGER NOT NULL,
      model_version TEXT NOT NULL,
      prompt_version TEXT NOT NULL,
      taxonomy_version TEXT NOT NULL,
      FOREIGN KEY (speech_id, revision_id, model_version, prompt_version, taxonomy_version)
        REFERENCES speech_enrichment ON DELETE CASCADE
    );
  `,
  );
}

async function projectCanonical(db) {
  if (!(await canonicalVersion(db))) return;
  const differences = legacyEnrichmentFields
    .map((field) => `i.${field} IS NOT e.${field}`)
    .join(" OR ");
  await exec(
    db,
    `
    CREATE TEMP TABLE changed_projection AS
      SELECT i.id, ${legacyEnrichmentFields.map((field) => `e.${field}`).join(", ")}
      FROM individual_speeches i
      LEFT JOIN speech_provenance p ON p.speech_id=i.id
      LEFT JOIN published_speech_enrichment s ON s.speech_id=i.id AND s.revision_id=p.revision_id
      LEFT JOIN speech_enrichment e ON e.speech_id=s.speech_id AND e.revision_id=s.revision_id
        AND e.model_version=s.model_version AND e.prompt_version=s.prompt_version
        AND e.taxonomy_version=s.taxonomy_version
      WHERE ${differences};
    UPDATE individual_speeches AS i SET
      ${legacyEnrichmentFields.map((field) => `${field}=e.${field}`).join(", ")}
    FROM changed_projection e WHERE e.id=i.id;
    DROP TABLE changed_projection;
    PRAGMA secure_delete=ON;
    DROP TABLE published_speech_enrichment;
    DROP TABLE speech_enrichment;
    DROP TABLE speech_provenance;
    DROP TABLE sitting_provenance;
    DROP TABLE sitting_sources;
    DROP TABLE source_revisions;
    DROP TABLE source_records;
    DROP TABLE canonical_migrations;
  `,
  );
}

module.exports = {
  CANONICAL_VERSION,
  canonicalVersion,
  createCanonicalSchema,
  legacyEnrichmentFields,
  projectCanonical,
};
