const path = require("node:path");
const fs = require("node:fs/promises");
const { open, run, close } = require("../../src/data/sqlite");
const { migrate } = require("../../src/data/schema");

async function createFixture(filename) {
  await fs.mkdir(path.dirname(filename), { recursive: true });
  const db = await open(filename);
  try {
    await migrate(db);
    for (const [id, label, country, source] of [
      [1, "Ana Example", "PT", "api"],
      [2, "Bruno Example", null, "historic"],
      [3, "Unused speaker", "DE", "api"],
    ]) {
      await run(
        db,
        "INSERT INTO meps (id,label,country,source) VALUES (?,?,?,?)",
        [id, label, country, source],
      );
    }
    for (const [id, date] of [
      ["sitting-2024-01-15", "2024-01-15"],
      ["sitting-2024-03-04", "2024-03-04"],
      ["sitting-2025-01-13", "2025-01-13"],
    ]) {
      await run(
        db,
        "INSERT INTO sittings (id,label,activity_date) VALUES (?,?,?)",
        [id, `Sitting of ${date}`, date],
      );
    }
    const speeches = [
      [
        "sitting-2024-01-15",
        1,
        "Ana Example",
        "Climate &amp; energy",
        "Solar power matters. <img src=x onerror=alert(1)>",
        "EN",
        "PPE",
      ],
      [
        "sitting-2024-01-15",
        2,
        "Bruno Example",
        "Budget — oversight",
        'A budget with safeguards, and "quotes".\nSecond line.',
        "PT",
        null,
      ],
      [
        "sitting-2024-03-04",
        1,
        "Ana Example",
        "Climate & energy",
        "Wind power and energy security.",
        "EN",
        "Renew",
      ],
      [
        "sitting-2024-03-04",
        999,
        "Unresolved speaker",
        "Climate & energy",
        "=not a spreadsheet formula",
        null,
        null,
      ],
      [
        "sitting-2025-01-13",
        2,
        "Bruno Example",
        "Budget — oversight",
        "Audit these expenses.",
        "PT",
        null,
      ],
    ];
    let order = 0;
    for (const [
      sitting,
      speaker,
      name,
      topic,
      content,
      language,
      group,
    ] of speeches) {
      await run(
        db,
        "INSERT INTO individual_speeches (sitting_id,mep_id,speaker_name,macro_topic,topic,speech_content,language,political_group_std,speech_order,macro_confidence) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
          sitting,
          speaker,
          name,
          topic,
          topic,
          content,
          language,
          group,
          ++order,
          0.9,
        ],
      );
    }
  } finally {
    await close(db);
  }
  return filename;
}

if (require.main === module) {
  const { prepare } = require("../../src/data/dataset");
  const os = require("node:os");
  (async () => {
    const temp = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-fixture-"));
    await createFixture(path.join(temp, "source.db"));
    const output = path.resolve("data/fixture");
    await prepare({ source: path.join(temp, "source.db"), output });
    console.log(`Fixture ready. DATASET_DIR=${output} npm start`);
  })().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}

module.exports = { createFixture };
