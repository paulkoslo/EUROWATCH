const fs = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { open, all, get, run, exec, close, backup } = require("./sqlite");
const { migrate, validateSchema } = require("./schema");

const SCHEMA_VERSION = 1;
function normalizeTopic(value) {
  return String(value || "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]/g, "-")
    .replace(/\s+/g, " ")
    .trim();
}

async function hashFile(filename) {
  const hash = crypto.createHash("sha256");
  const handle = await fs.open(filename, "r");
  try {
    for await (const chunk of handle.createReadStream()) hash.update(chunk);
  } finally {
    await handle.close();
  }
  return hash.digest("hex");
}

async function metadata(db, key) {
  const row = await get(
    db,
    "SELECT value FROM dataset_metadata WHERE key = ?",
    [key],
  );
  return row ? JSON.parse(row.value) : null;
}

async function setMetadata(db, key, value) {
  await run(
    db,
    "INSERT OR REPLACE INTO dataset_metadata (key,value) VALUES (?,?)",
    [key, JSON.stringify(value)],
  );
}

async function collectStats(db) {
  const stats = await get(
    db,
    `SELECT COUNT(*) AS speeches,
    SUM(s.id IS NULL) AS sittingOrphans,
    SUM(m.id IS NULL) AS unresolvedSpeakerSpeeches,
    SUM(i.political_group_std IS NULL OR TRIM(i.political_group_std) = '') AS missingGroups,
    SUM(i.language IS NULL OR TRIM(i.language) = '') AS missingLanguages,
    SUM(i.macro_topic IS NULL OR TRIM(i.macro_topic) = '') AS missingTopics,
    SUM(i.macro_confidence <= 0.3) AS lowConfidenceTopics
    FROM individual_speeches i LEFT JOIN sittings s ON s.id=i.sitting_id LEFT JOIN meps m ON m.id=i.mep_id`,
  );
  const coverage = await get(
    db,
    "SELECT COUNT(*) AS sittings, MIN(activity_date) AS fromDate, MAX(activity_date) AS toDate FROM sittings",
  );
  const people = await get(
    db,
    `SELECT COUNT(*) AS speakerRecords, SUM(country IS NULL OR TRIM(country) = '' OR country = 'Unknown') AS missingCountryRecords FROM meps`,
  );
  return { ...stats, ...coverage, ...people };
}

async function prepare({ source, output, from = null, log = console.log }) {
  source = path.resolve(source);
  output = path.resolve(output);
  await fs.access(source);
  await fs.mkdir(path.dirname(output), { recursive: true });
  await fs.mkdir(output); // Immutable release directories must never be overwritten.
  const mainFile = path.join(output, "ep_data.db");
  let db;
  let analytics;
  try {
    const original = await open(source, { readonly: true });
    try {
      await backup(original, mainFile);
    } finally {
      await close(original);
    }
    log("Consistent source backup created. Preparing the copy...");
    db = await open(mainFile);
    await migrate(db);
    if (from) {
      await exec(db, "BEGIN IMMEDIATE");
      try {
        await run(
          db,
          "DELETE FROM individual_speeches WHERE NOT EXISTS (SELECT 1 FROM sittings s WHERE s.id=individual_speeches.sitting_id AND s.activity_date >= ?)",
          [from],
        );
        await run(
          db,
          "DELETE FROM sittings WHERE activity_date < ? OR activity_date IS NULL",
          [from],
        );
        await exec(
          db,
          "DELETE FROM speeches WHERE NOT EXISTS (SELECT 1 FROM sittings s WHERE s.id=speeches.id); DELETE FROM sittings_cache;",
        );
        await exec(db, "COMMIT");
      } catch (error) {
        await exec(db, "ROLLBACK");
        throw error;
      }
    }
    const stats = await collectStats(db);
    if (stats.sittingOrphans)
      throw new Error(
        `Dataset has ${stats.sittingOrphans} speeches without sittings. Use a consistent source, not the old demo.`,
      );
    if (!stats.speeches || !stats.fromDate || !stats.toDate)
      throw new Error("Dataset has no dated speeches to browse.");
    const invalidDates = await get(
      db,
      "SELECT COUNT(*) AS n FROM sittings WHERE activity_date IS NULL OR date(activity_date) IS NULL OR date(activity_date) != activity_date OR length(activity_date) != 10",
    );
    if (invalidDates.n)
      throw new Error(
        `Dataset has ${invalidDates.n} sittings with invalid dates.`,
      );
    const check = await get(db, "PRAGMA quick_check");
    if (Object.values(check)[0] !== "ok")
      throw new Error("SQLite integrity check failed.");
    await exec(
      db,
      `
      DROP TABLE IF EXISTS explorer_speakers;
      CREATE TABLE explorer_speakers (id INTEGER PRIMARY KEY, label TEXT NOT NULL, country TEXT, source TEXT NOT NULL, speech_count INTEGER NOT NULL);
      INSERT INTO explorer_speakers SELECT m.id, COALESCE(NULLIF(TRIM(m.label),''),'Unknown speaker'), NULLIF(NULLIF(TRIM(m.country),''),'Unknown'), COALESCE(m.source,'api'), COALESCE(c.n,0)
      FROM meps m LEFT JOIN (SELECT mep_id,COUNT(*) AS n FROM individual_speeches GROUP BY mep_id) c ON c.mep_id=m.id;
      INSERT INTO explorer_speakers SELECT i.mep_id, COALESCE(MAX(NULLIF(TRIM(i.speaker_name),'')),'Unknown speaker'), NULL, 'unresolved', COUNT(*)
      FROM individual_speeches i LEFT JOIN meps m ON m.id=i.mep_id WHERE m.id IS NULL AND typeof(i.mep_id)='integer' AND i.mep_id > 0 GROUP BY i.mep_id;
      CREATE INDEX explorer_speakers_count ON explorer_speakers(speech_count DESC, id);
      CREATE INDEX explorer_speakers_label ON explorer_speakers(label COLLATE NOCASE);
      ANALYZE;
    `,
    );
    analytics = await open(path.join(output, "analytics.db"));
    await exec(
      analytics,
      `CREATE TABLE dataset_metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
      CREATE TABLE topic_series (period TEXT NOT NULL, topic TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(period,topic));
      CREATE INDEX topic_series_topic ON topic_series(topic,period);
      CREATE TABLE coverage (period TEXT PRIMARY KEY,count INTEGER NOT NULL);
      CREATE TABLE topics (topic TEXT PRIMARY KEY,count INTEGER NOT NULL);
    `,
    );
    const rows = await all(
      db,
      `SELECT substr(s.activity_date,1,7) AS period, i.macro_topic AS topic, COUNT(*) AS count
      FROM individual_speeches i JOIN sittings s ON s.id=i.sitting_id WHERE TRIM(COALESCE(i.macro_topic,'')) != '' GROUP BY 1,2`,
    );
    await exec(analytics, "BEGIN");
    for (const row of rows) {
      await run(
        analytics,
        "INSERT INTO topic_series VALUES (?,?,?) ON CONFLICT(period,topic) DO UPDATE SET count=count+excluded.count",
        [row.period, normalizeTopic(row.topic), row.count],
      );
    }
    for (const row of await all(
      db,
      "SELECT substr(s.activity_date,1,7) AS period, COUNT(*) AS count FROM individual_speeches i JOIN sittings s ON s.id=i.sitting_id GROUP BY 1",
    )) {
      await run(analytics, "INSERT INTO coverage VALUES (?,?)", [
        row.period,
        row.count,
      ]);
    }
    await exec(
      analytics,
      "INSERT INTO topics SELECT topic,SUM(count) FROM topic_series GROUP BY topic; COMMIT",
    );
    const release = {
      id: crypto.randomUUID(),
      schemaVersion: SCHEMA_VERSION,
      preparedAt: new Date().toISOString(),
      stats,
      subsetFrom: from,
    };
    for (const file of [db, analytics])
      await setMetadata(file, "release", release);
    if (from) await exec(db, "VACUUM");
    await close(analytics);
    analytics = null;
    await exec(
      db,
      "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;",
    );
    await close(db);
    db = null;
    const files = {};
    for (const name of ["ep_data.db", "analytics.db"])
      files[name] = {
        sha256: await hashFile(path.join(output, name)),
        bytes: (await fs.stat(path.join(output, name))).size,
      };
    await fs.writeFile(
      path.join(output, "manifest.json"),
      JSON.stringify({ ...release, files }, null, 2) + "\n",
      { flag: "wx" },
    );
    log(
      `Prepared dataset ${release.id}: ${stats.speeches} speeches, ${stats.sittings} sittings at ${output}`,
    );
    return release;
  } catch (error) {
    // Leave partial output for diagnosis; absence of a manifest prevents serving or promotion.
    throw new Error(`Preparation failed in ${output}: ${error.message}`, {
      cause: error,
    });
  } finally {
    if (analytics) await close(analytics);
    if (db) await close(db);
  }
}

async function loadDataset(directory, { verifyHashes = false } = {}) {
  directory = path.resolve(directory);
  const manifest = JSON.parse(
    await fs.readFile(path.join(directory, "manifest.json"), "utf8"),
  );
  if (manifest.schemaVersion !== SCHEMA_VERSION || !manifest.id)
    throw new Error("Unsupported dataset manifest. Run data:prepare.");
  const db = await open(path.join(directory, "ep_data.db"), { readonly: true });
  let analytics;
  try {
    await validateSchema(db);
    const release = await metadata(db, "release");
    if (
      !release ||
      release.id !== manifest.id ||
      release.schemaVersion !== SCHEMA_VERSION
    )
      throw new Error("Main database and manifest do not match.");
    let analyticsError = null;
    try {
      analytics = await open(path.join(directory, "analytics.db"), {
        readonly: true,
      });
      const analyticRelease = await metadata(analytics, "release");
      if (!analyticRelease || analyticRelease.id !== release.id)
        throw new Error("Analytics belongs to another dataset.");
      await get(analytics, "SELECT COUNT(*) FROM topic_series");
    } catch (error) {
      if (analytics) await close(analytics);
      analytics = null;
      analyticsError = error.message;
    }
    if (verifyHashes) {
      for (const [name, file] of Object.entries(manifest.files)) {
        if (
          !["ep_data.db", "analytics.db"].includes(name) ||
          (await hashFile(path.join(directory, name))) !== file.sha256
        )
          throw new Error(`Dataset hash mismatch: ${name}`);
      }
      if (!analytics) throw new Error(analyticsError);
    }
    return {
      db,
      analytics,
      release,
      directory,
      files: manifest.files,
      analyticsError,
      close: async () => {
        if (analytics) await close(analytics);
        await close(db);
      },
    };
  } catch (error) {
    if (analytics) await close(analytics);
    await close(db);
    throw error;
  }
}

async function promote(directory, pointer = path.resolve("data/current.json")) {
  const dataset = await loadDataset(directory, { verifyHashes: true });
  try {
    await fs.mkdir(path.dirname(pointer), { recursive: true });
    const temp = `${pointer}.${process.pid}.tmp`;
    await fs.writeFile(
      temp,
      JSON.stringify({
        directory: path.relative(
          path.dirname(pointer),
          path.resolve(directory),
        ),
        id: dataset.release.id,
      }) + "\n",
    );
    await fs.rename(temp, pointer);
    return dataset.release.id;
  } finally {
    await dataset.close();
  }
}

async function currentDirectory(root) {
  const pointer = path.join(root, "data/current.json");
  const current = JSON.parse(await fs.readFile(pointer, "utf8"));
  return path.resolve(path.dirname(pointer), current.directory);
}

module.exports = {
  prepare,
  loadDataset,
  promote,
  currentDirectory,
  metadata,
  migrate,
  normalizeTopic,
  hashFile,
  collectStats,
};
