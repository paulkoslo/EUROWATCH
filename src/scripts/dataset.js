#!/usr/bin/env node
const path = require("node:path");
const fs = require("node:fs/promises");
const { open, close } = require("../data/sqlite");
const { migrateArchive } = require("../data/archive-migration");
const {
  prepare,
  loadDataset,
  promote,
  currentDirectory,
  migrate,
} = require("../data/dataset");

async function main() {
  const [command, ...args] = process.argv.slice(2);
  const option = (name) => {
    const index = args.indexOf(`--${name}`);
    return index < 0 ? null : args[index + 1];
  };
  if (command === "init") {
    const output = option("output");
    if (!output)
      throw new Error("Usage: npm run data:init -- --output data/staging.db");
    await fs.mkdir(path.dirname(path.resolve(output)), { recursive: true });
    const db = await open(output);
    try {
      await migrate(db);
    } finally {
      await close(db);
    }
    console.log(`Initialized writable maintenance database: ${output}`);
  } else if (command === "migrate") {
    const source = option("source"),
      output = option("output");
    if (!source || !output)
      throw new Error(
        "Usage: npm run data:migrate -- --source data/ep_data.db --output data/canonical/archive.db",
      );
    const report = await migrateArchive({ source, output, log: console.error });
    console.log(JSON.stringify(report, null, 2));
  } else if (command === "prepare") {
    const source = option("source"),
      output = option("output"),
      from = option("from");
    if (!source || !output)
      throw new Error(
        "Usage: npm run data:prepare -- --source data/ep_data.db --output data/releases/NAME [--from YYYY-MM-DD]",
      );
    if (
      from &&
      (!/^\d{4}-\d{2}-\d{2}$/.test(from) ||
        new Date(`${from}T00:00:00Z`).toISOString().slice(0, 10) !== from)
    )
      throw new Error("Invalid --from date");
    await prepare({ source, output, from });
  } else if (command === "promote") {
    if (!option("dataset"))
      throw new Error(
        "Usage: npm run data:promote -- --dataset data/releases/NAME",
      );
    console.log(
      `Selected dataset ${await promote(option("dataset"))}. Restart the explorer to use it.`,
    );
  } else if (command === "validate") {
    const dataset = await loadDataset(
      option("dataset") ||
        (await currentDirectory(path.resolve(__dirname, "../.."))),
      { verifyHashes: true },
    );
    console.log(JSON.stringify(dataset.release, null, 2));
    await dataset.close();
  } else
    throw new Error(
      "Choose init, migrate, prepare, promote or validate. See README.md.",
    );
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
