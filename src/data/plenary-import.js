const fs = require("node:fs/promises");
const { parseArgs } = require("node:util");
const { open, all, get, exec, run, close } = require("./sqlite");
const { migrate } = require("./schema");
const { discoverSittings, fetchTranscript } = require("./plenary-source");
const { normalizePoliticalGroup } = require("../core/group-normalizer");
const { createDetector, detectLanguage } = require("../core/detect-language");

function lastClosedDate() {
  const today = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "Europe/Brussels",
    dateStyle: "short",
  }).format(new Date());
  return new Date(Date.parse(today) - 86400000).toISOString().slice(0, 10);
}

function isDate(value) {
  return (
    /^\d{4}-\d{2}-\d{2}$/.test(value) &&
    Number.isFinite(Date.parse(value)) &&
    new Date(value).toISOString().slice(0, 10) === value
  );
}

async function transaction(db, task) {
  await exec(db, "BEGIN IMMEDIATE");
  try {
    const result = await task();
    await exec(db, "COMMIT");
    return result;
  } catch (error) {
    await exec(db, "ROLLBACK");
    throw error;
  }
}

async function alreadyImported(db, date) {
  const rows = await all(
    db,
    `
    SELECT s.activity_date AS date,
      EXISTS (SELECT 1 FROM individual_speeches i WHERE i.sitting_id = s.id) AS hasSpeeches
    FROM sittings s WHERE s.activity_date = ? OR s.id = ?
  `,
    [date, `sitting-${date}`],
  );
  if (!rows.length) return false;
  if (rows.length !== 1 || rows[0].date !== date || !rows[0].hasSpeeches)
    throw new Error(
      "conflicting or incomplete sitting in staging. Inspect it before retrying.",
    );
  return true;
}

function nameKey(name) {
  return name.trim().replace(/\s+/g, " ").normalize("NFC").toLowerCase();
}

async function speakerIndex(db) {
  const speakers = new Map();
  for (const { id, label } of await all(
    db,
    "SELECT id, label FROM meps WHERE label IS NOT NULL",
  )) {
    const key = nameKey(label);
    if (key) speakers.set(key, speakers.has(key) ? null : id);
  }
  return speakers;
}

async function storeSitting(db, sitting, transcript, speakers, detector) {
  const { date, documentId, startDate } = sitting;
  const { content, speeches } = transcript;
  const id = `sitting-${date}`;
  if (!content || !speeches.length) throw new Error("empty transcript");
  const entries = speeches.map((speech) => {
    const rawGroup = speech.political_group;
    const group = rawGroup ? normalizePoliticalGroup(rawGroup) : null;
    return [
      id,
      speech.speaker_name,
      rawGroup,
      speech.title,
      speech.speech_content,
      speech.speech_order,
      speakers.get(nameKey(speech.speaker_name)) ?? null,
      speech.topic,
      detectLanguage(detector, speech.speech_content),
      rawGroup,
      group?.kind === "unknown" ? null : (group?.std ?? null),
      group?.kind ?? null,
      group?.reason ?? null,
    ];
  });

  return transaction(db, async () => {
    if (await alreadyImported(db, date)) return false;
    // Older research databases reference speeches, newer ones reference sittings.
    for (const table of ["sittings", "speeches"]) {
      await run(
        db,
        `
        INSERT INTO ${table}
        (id, type, label, date, activity_date, content, docIdentifier, activity_type, activity_start_date, last_updated)
        VALUES (?, 'PLENARY_DEBATE', ?, ?, ?, ?, ?, 'PLENARY_SITTING', ?, ?)
      `,
        [
          id,
          `Parliamentary Sitting - ${date}`,
          date,
          date,
          content,
          documentId,
          startDate,
          Date.now(),
        ],
      );
    }
    for (const entry of entries) {
      await run(
        db,
        `
        INSERT INTO individual_speeches
        (sitting_id, speaker_name, political_group, title, speech_content, speech_order, mep_id,
         topic, language, political_group_raw, political_group_std, political_group_kind, political_group_reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
        entry,
      );
    }
    return true;
  });
}

async function importSittings({
  start,
  end = lastClosedDate(),
  filename,
  dryRun = false,
  log = console.log,
  request,
}) {
  if (!isDate(start) || !isDate(end) || start > end || end > lastClosedDate())
    throw new Error(
      "Use a valid inclusive --start/--end range ending before today in Brussels.",
    );
  if (!dryRun && !filename)
    throw new Error(
      "Specify a writable staging database with --db, or use --dry-run.",
    );
  let db;
  let detector;
  try {
    let speakers;
    if (filename) {
      const realPath = await fs.realpath(filename);
      if (!(await fs.stat(realPath)).isFile())
        throw new Error("--db must be a SQLite file");
      db = await open(realPath, { readonly: dryRun });
      const schema = await get(
        db,
        `
        SELECT COUNT(*) AS tables FROM sqlite_schema
        WHERE type = 'table' AND name IN ('sittings', 'individual_speeches', 'meps')
      `,
      );
      if (schema.tables !== 3)
        throw new Error(
          "Expected a research database or a database initialized with data:init.",
        );
      if (!dryRun) {
        await exec(db, "PRAGMA foreign_keys = ON");
        await transaction(db, async () => {
          const hasMetadata = await get(
            db,
            "SELECT 1 FROM sqlite_schema WHERE name = 'dataset_metadata'",
          );
          if (
            hasMetadata &&
            (await get(
              db,
              "SELECT 1 FROM dataset_metadata WHERE key = 'release'",
            ))
          )
            throw new Error(
              "Prepared releases are read-only. Use a staging copy of the research database.",
            );
          await migrate(db);
        });
        speakers = await speakerIndex(db);
      }
    }

    const sittings = await discoverSittings(start, end, request);
    const result = {
      confirmed: sittings.length,
      imported: 0,
      previewed: 0,
      existing: 0,
      pending: 0,
      failed: 0,
    };
    for (const sitting of sittings) {
      try {
        if (db && (await alreadyImported(db, sitting.date))) {
          result.existing++;
          log(`${sitting.date} already imported`);
          continue;
        }
        const transcript = await fetchTranscript(sitting, request);
        if (transcript.source === "pending") {
          result.pending++;
          log(
            `${sitting.date} pending transcript publication (HTML: ${transcript.htmlError})`,
          );
          continue;
        }
        if (dryRun) {
          result.previewed++;
        } else {
          detector ??= await createDetector();
          if (
            !(await storeSitting(db, sitting, transcript, speakers, detector))
          ) {
            result.existing++;
            log(`${sitting.date} already imported`);
            continue;
          }
          result.imported++;
        }
        const fallback = transcript.htmlError
          ? ` (HTML: ${transcript.htmlError})`
          : "";
        log(
          `${sitting.date} ${dryRun ? "would import" : "imported"} ${transcript.speeches.length} speeches via ${transcript.source.toUpperCase()}${fallback}`,
        );
      } catch (error) {
        result.failed++;
        log(`${sitting.date} failed: ${error.message}`);
      }
    }
    return result;
  } finally {
    try {
      detector?.cld3.dispose();
    } finally {
      if (db) await close(db);
    }
  }
}

async function main() {
  const { values } = parseArgs({
    options: {
      start: { type: "string" },
      end: { type: "string" },
      db: { type: "string" },
      "dry-run": { type: "boolean" },
      help: { type: "boolean" },
    },
  });
  if (values.help) {
    console.log(
      "npm run data:import -- --start YYYY-MM-DD [--end YYYY-MM-DD] [--db FILE] [--dry-run]",
    );
    return;
  }
  const result = await importSittings({
    start: values.start,
    end: values.end,
    filename: values.db,
    dryRun: values["dry-run"],
  });
  console.log(JSON.stringify(result));
  if (result.failed || result.pending) process.exitCode = 1;
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}

module.exports = { importSittings, isDate, lastClosedDate };
