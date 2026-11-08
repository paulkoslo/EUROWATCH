const fs = require("node:fs");
const path = require("node:path");
const { hash } = require("./model");
const { options, read, write, providerFor } = require("./cli");
const { durableWrite } = require("./backfill");
const {
  buildSystemPrompt,
} = require("../core/prompts/topic-macro-classification");

async function legacyBaseline({
  corpus,
  taxonomy,
  provider,
  checkpoint,
  budgetUsd = 0,
  batchSize = 20,
}) {
  if (
    !Array.isArray(taxonomy) ||
    !taxonomy.length ||
    !taxonomy.every((name) => typeof name === "string" && name.trim()) ||
    !Number.isFinite(budgetUsd) ||
    budgetUsd < 0 ||
    !Number.isInteger(batchSize) ||
    batchSize < 1 ||
    batchSize > 20
  )
    throw new Error(
      "Supply a fixed legacy taxonomy and a valid budget/batch size.",
    );
  const records = corpus.records.filter((record) => record.kind === "debate");
  const titles = [
    ...new Set(records.map((record) => record.agendaTitle).filter(Boolean)),
  ].sort();
  if (titles.length > 1000)
    throw new Error("Use a reviewed sample of up to 1,000 agenda headings.");
  if (provider && !checkpoint)
    throw new Error("Live legacy baseline needs a durable checkpoint.");
  const version = hash({
    model: corpus.config.model,
    taxonomy,
    prompt: buildSystemPrompt(taxonomy),
    provider: provider?.kind || "none",
  });
  const lock = checkpoint && path.join(checkpoint, "legacy.lock");
  if (lock) {
    fs.mkdirSync(checkpoint, { recursive: true, mode: 0o700 });
    fs.mkdirSync(lock);
  }
  const stateFile = checkpoint && path.join(checkpoint, "legacy-baseline.json");
  try {
    const state =
      stateFile && fs.existsSync(stateFile)
        ? read(stateFile)
        : { version, reservedUsd: 0, batches: {} };
    if (state.version !== version)
      throw new Error("Use a new checkpoint for a changed baseline version.");
    const byTitle = new Map();
    for (let i = 0; i < titles.length; i += batchSize) {
      const batch = titles.slice(i, i + batchSize);
      const key = hash(batch);
      const request = {
        key,
        jsonArray: true,
        messages: [
          { role: "system", content: buildSystemPrompt(taxonomy) },
          {
            role: "user",
            content:
              "Topics:\n" +
              batch.map((title, index) => `${index}. ${title}`).join("\n"),
          },
        ],
      };
      let item = state.batches[key];
      if (!item && provider && !state.spendHalted) {
        const cost = provider.reserve(request);
        if (!Number.isFinite(cost) || cost < 0)
          throw new Error("Invalid legacy provider reservation.");
        if (state.reservedUsd + cost <= budgetUsd) {
          state.reservedUsd += cost;
          item = state.batches[key] = { status: "interrupted" };
          durableWrite(stateFile, state);
          let usage;
          try {
            const response = await provider.generate(request);
            usage = response.usage;
            if (
              !Array.isArray(response.candidate) ||
              response.candidate.length !== batch.length ||
              response.candidate.some(
                (result) =>
                  !result ||
                  typeof result.macro_topic !== "string" ||
                  !result.macro_topic.trim() ||
                  !Number.isFinite(result.confidence) ||
                  result.confidence < 0 ||
                  result.confidence > 1,
              )
            )
              throw new Error("Invalid legacy output.");
            item = {
              status: "candidate",
              usage,
              values: response.candidate.map((result) => ({
                label: result.macro_topic,
                confidence: result.confidence,
              })),
            };
          } catch (error) {
            item = { status: "failed", usage: usage || error.usage || null };
          }
          if (
            Number.isFinite(item.usage?.costUsd) &&
            item.usage.costUsd > cost + 1e-9
          )
            state.spendHalted = true;
          state.batches[key] = item;
          durableWrite(stateFile, state);
        }
      }
      for (const [index, title] of batch.entries())
        byTitle.set(title, {
          status: item?.status || "skipped",
          label: item?.values?.[index]?.label,
          confidence: item?.values?.[index]?.confidence,
        });
    }
    return {
      baselineVersion: version,
      model: corpus.config.model,
      provider: provider?.kind || "none",
      reservedUsd: state.reservedUsd,
      spendHalted: !!state.spendHalted,
      reportedCostUsd: Object.values(state.batches).reduce(
        (sum, item) => sum + (item.usage?.costUsd || 0),
        0,
      ),
      records: records.map((record) => ({
        id: record.id,
        inputRevision: record.inputRevision,
        ...byTitle.get(record.agendaTitle),
      })),
    };
  } finally {
    if (lock) fs.rmSync(lock, { recursive: true, force: true });
  }
}

if (require.main === module) {
  const opts = options(process.argv.slice(2));
  const corpus = read(opts.input);
  if (!opts.output || !opts.taxonomy)
    throw new Error("Use --input, --taxonomy and --output.");
  legacyBaseline({
    corpus,
    taxonomy: read(opts.taxonomy),
    provider: providerFor(opts, corpus.config),
    checkpoint: opts.checkpoint,
    budgetUsd: Number(opts.budget || 0),
  })
    .then((report) => write(opts.output, report))
    .catch((error) => {
      console.error(error.message);
      process.exitCode = 1;
    });
}
module.exports = { legacyBaseline };
