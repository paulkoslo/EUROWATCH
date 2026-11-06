const { all, exec } = require("./sqlite");

const speechColumns = {
  mep_id: "INTEGER",
  language: "TEXT",
  political_group_raw: "TEXT",
  political_group_std: "TEXT",
  political_group_kind: "TEXT",
  political_group_reason: "TEXT",
  topic: "TEXT",
  macro_topic: "TEXT",
  macro_specific_focus: "TEXT",
  macro_confidence: "REAL",
  macro_classified_by: "TEXT",
  macro_classified_at: "INTEGER",
  macro_classification_cost: "REAL",
};

async function migrate(db) {
  await exec(
    db,
    `
    CREATE TABLE IF NOT EXISTS meps (id INTEGER PRIMARY KEY, label TEXT, givenName TEXT, familyName TEXT, sortLabel TEXT, country TEXT, politicalGroup TEXT, is_current INTEGER DEFAULT 0, source TEXT DEFAULT 'api', last_updated INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS sittings (id TEXT PRIMARY KEY, type TEXT, label TEXT, personId INTEGER, date TEXT, content TEXT, docIdentifier TEXT, notationId TEXT, activity_type TEXT, activity_date TEXT, activity_start_date TEXT, last_updated INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS speeches (id TEXT PRIMARY KEY, type TEXT, label TEXT, personId INTEGER, date TEXT, content TEXT, docIdentifier TEXT, notationId TEXT, activity_type TEXT, activity_date TEXT, activity_start_date TEXT, last_updated INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS individual_speeches (id INTEGER PRIMARY KEY AUTOINCREMENT, sitting_id TEXT REFERENCES sittings(id), speaker_name TEXT, political_group TEXT, title TEXT, speech_content TEXT, speech_order INTEGER, created_at INTEGER DEFAULT (strftime('%s','now')));
    CREATE TABLE IF NOT EXISTS sittings_cache (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, last_updated INTEGER);
    CREATE TABLE IF NOT EXISTS cache_status (id INTEGER PRIMARY KEY, meps_last_updated INTEGER DEFAULT 0, speeches_last_updated INTEGER DEFAULT 0, total_speeches INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS dataset_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
  `,
  );
  const columns = new Set(
    (await all(db, "PRAGMA table_info(individual_speeches)")).map(
      (c) => c.name,
    ),
  );
  for (const [name, type] of Object.entries(speechColumns)) {
    if (!columns.has(name))
      await exec(
        db,
        `ALTER TABLE individual_speeches ADD COLUMN ${name} ${type}`,
      );
  }
  const mepColumns = new Set(
    (await all(db, "PRAGMA table_info(meps)")).map((c) => c.name),
  );
  for (const [name, type] of Object.entries({
    is_current: "INTEGER DEFAULT 0",
    source: "TEXT DEFAULT 'api'",
  })) {
    if (!mepColumns.has(name))
      await exec(db, `ALTER TABLE meps ADD COLUMN ${name} ${type}`);
  }
  await exec(
    db,
    `
    CREATE INDEX IF NOT EXISTS explorer_sittings_date ON sittings(activity_date, id);
    CREATE INDEX IF NOT EXISTS explorer_speeches_sitting ON individual_speeches(sitting_id, speech_order, id);
    CREATE INDEX IF NOT EXISTS explorer_speeches_speaker ON individual_speeches(mep_id, sitting_id, speech_order, id);
    CREATE INDEX IF NOT EXISTS explorer_speeches_topic ON individual_speeches(macro_topic, sitting_id);
    CREATE INDEX IF NOT EXISTS explorer_speakers_name ON meps(label COLLATE NOCASE);
  `,
  );
}

async function validateSchema(db) {
  for (const [table, required] of Object.entries({
    sittings: ["id", "activity_date"],
    meps: ["id", "label", "country", "source"],
    individual_speeches: [
      "id",
      "sitting_id",
      "speaker_name",
      "speech_content",
      "speech_order",
      ...Object.keys(speechColumns),
    ],
    dataset_metadata: ["key", "value"],
    explorer_speakers: ["id", "label", "country", "source", "speech_count"],
  })) {
    const actual = new Set(
      (await all(db, `PRAGMA table_info(${table})`)).map((c) => c.name),
    );
    for (const name of required)
      if (!actual.has(name))
        throw new Error(
          `Unsupported dataset schema: ${table}.${name} missing. Run data:prepare on an offline copy.`,
        );
  }
}

module.exports = { migrate, validateSchema };
