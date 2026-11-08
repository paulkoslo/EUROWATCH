#!/usr/bin/env node
const fs = require("node:fs");
const path = require("node:path");
const { pilot, selectSample, evaluate } = require("./pilot");
const { requestFor } = require("./model");
const { openAIProvider, offlineProvider } = require("./provider");
const { backfill } = require("./backfill");

function read(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}
function write(file, value) {
  fs.mkdirSync(path.dirname(path.resolve(file)), { recursive: true });
  const temp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(value, null, 2) + "\n", {
    mode: 0o600,
  });
  fs.renameSync(temp, file);
}
function options(args) {
  const result = {};
  for (let i = 0; i < args.length; i += 2) {
    if (
      !args[i].startsWith("--") ||
      !args[i + 1] ||
      args[i + 1].startsWith("--") ||
      Object.hasOwn(result, args[i].slice(2))
    )
      throw new Error("Options require unique --name value pairs");
    result[args[i].slice(2)] = args[i + 1];
  }
  return result;
}
function providerFor(opts, config) {
  if (!opts.provider || opts.provider === "none") return undefined;
  if (opts.provider === "offline") return offlineProvider(read(opts.responses));
  if (opts.provider !== "openai") throw new Error("Unknown provider");
  if (!opts.budget || !(Number(opts.budget) > 0))
    throw new Error("OpenAI requires explicit positive --budget");
  return openAIProvider({
    model: config.model,
    apiKey: process.env.OPENAI_API_KEY,
    inputUsdPerMillion: Number(opts["input-price"]),
    outputUsdPerMillion: Number(opts["output-price"]),
    maxOutputTokens: Number(opts["max-output-tokens"] || 1200),
  });
}
async function main(argv) {
  const [command, ...args] = argv;
  const opts = options(args);
  if (!opts.input || !opts.output)
    throw new Error("Required: --input JSON --output JSON");
  const input = read(opts.input);
  if (command === "evaluate")
    return write(opts.output, evaluate(input, read(opts.reviews)));
  if (!["pilot", "backfill"].includes(command))
    throw new Error("Command must be pilot, evaluate or backfill");
  let records = input.records;
  if (opts.from)
    records = records.filter((r) => r.effectiveAt.slice(0, 10) >= opts.from);
  if (opts.to)
    records = records.filter((r) => r.effectiveAt.slice(0, 10) <= opts.to);
  if (opts.sample) records = selectSample(records, Number(opts.sample));
  const livePilot = command === "pilot" && opts.provider === "openai";
  if (livePilot) {
    if (records.length > Number(opts["max-items"] || 100))
      throw new Error(
        "Choose a smaller pilot sample or explicitly allow its full size with --max-items.",
      );
    if (!opts.checkpoint)
      throw new Error("Live pilots need a durable --checkpoint.");
    for (const record of records)
      requestFor(record, input.config, input.officialEvidence);
  }
  const runner = command === "backfill" || livePilot ? backfill : pilot;
  const report = await runner({
    ...input,
    records,
    provider: providerFor(opts, input.config),
    budgetUsd: Number(opts.budget || 0),
    checkpoint: opts.checkpoint,
    maxItems: Number(opts["max-items"] || 100),
    retryFailed: opts["retry-failed"] === "true",
    onEvent: (event) => process.stderr.write(JSON.stringify(event) + "\n"),
  });
  if (livePilot) report.mode = "pilot";
  write(opts.output, report);
}
if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
module.exports = { main, read, write, options, providerFor };
