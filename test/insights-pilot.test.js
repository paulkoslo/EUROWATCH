const test = require("node:test");
const assert = require("node:assert/strict");
const {
  requestFor,
  resultFor,
  joinEvidence,
} = require("../src/insights/model");
const { pilot, evaluate, selectSample } = require("../src/insights/pilot");
const { openAIProvider, offlineProvider } = require("../src/insights/provider");

const config = {
  model: "explicit-model",
  promptVersion: "briefing-1",
  taxonomyVersion: "topics-1",
  topics: ["Climate", "Procedure"],
};
const record = {
  id: "debate:1",
  kind: "debate",
  inputRevision: "rev1",
  effectiveAt: "2026-03-09T00:00:00Z",
  term: 10,
  language: "en",
  agendaTitle: "Climate",
  officialIds: ["official:procedure:1"],
  legacyLabels: [{ label: "Climate", confidence: 0.8 }],
  passages: [
    {
      id: "speech:1",
      text: "We need cleaner energy.",
      speakerId: "mep:1",
      sourceUrl: "https://www.europarl.europa.eu/doceo/example.html",
      sourceRevision: "sha256:1",
    },
  ],
};
const candidate = {
  topics: [{ label: "Climate", confidence: 0.9, passageIds: ["speech:1"] }],
  claims: [
    {
      text: "The speaker supports cleaner energy.",
      quote: "We need cleaner energy.",
      passageId: "speech:1",
      speakerId: "mep:1",
    },
  ],
  uncertain: false,
};
const evidence = {
  id: "vote:1",
  kind: "vote",
  officialIds: ["official:procedure:1"],
  sourceUrl: "https://data.europarl.europa.eu/vote/1",
  sourceRevision: "v1",
  effectiveAt: record.effectiveAt,
  totals: { for: 100, against: 20, abstention: 5 },
};

test("official evidence requires exact identities; date/title coincidences do not join", () => {
  assert.equal(joinEvidence(record, [evidence]).length, 1);
  assert.equal(
    joinEvidence({ ...record, officialIds: [] }, [evidence]).length,
    0,
  );
  assert.throws(
    () => joinEvidence(record, [{ ...evidence, totals: null }]),
    /totals/,
  );
});
test("candidate citations bind quotation, speaker and source revision", () => {
  const request = requestFor(record, config, [evidence]);
  const result = resultFor(request, { candidate });
  assert.equal(result.candidate.claims[0].sourceRevision, "sha256:1");
  assert.deepEqual(result.missingEvidence, ["adopted_text"]);
  for (const bad of [
    { quote: "Not in the source" },
    { speakerId: "mep:2" },
    { passageId: "missing" },
  ]) {
    assert.throws(
      () =>
        resultFor(request, {
          candidate: {
            ...candidate,
            claims: [{ ...candidate.claims[0], ...bad }],
          },
        }),
      /mismatch/,
    );
  }
  assert.notEqual(
    request.key,
    requestFor({ ...record, inputRevision: "rev2" }, config, [evidence]).key,
  );
  assert.notEqual(
    request.key,
    requestFor(record, { ...config, taxonomyVersion: "topics-2" }, [evidence])
      .key,
  );
  assert.notEqual(
    request.key,
    requestFor(record, config, [{ ...evidence, sourceRevision: "v2" }]).key,
  );
});
test("offline pilot, explicit disabled state and independently reviewed evaluation", async () => {
  const key = requestFor(record, config).key;
  const report = await pilot({
    records: [record],
    config,
    provider: offlineProvider({ [key]: candidate }),
  });
  assert.equal(report.results[0].status, "candidate");
  const review = {
    key,
    reviewer: "reviewer",
    reviewedAt: "2026-11-07T12:00:00Z",
    split: "heldout",
    agenda: 1,
    candidateTopic: 1,
    baselineTopic: 0.5,
    factuality: 1,
    citation: 1,
    balance: 1,
  };
  assert.equal(evaluate(report, [review]).means.baselineTopic, 0.5);
  assert.throws(() => evaluate(report, [review, review]), /unique/);
  assert.equal(
    (await pilot({ records: [record], config })).results[0].reason,
    "provider_disabled",
  );
  assert.deepEqual(selectSample([record]), [record]);
});
test("OpenAI adapter is explicit, bounded and handles incomplete responses as failures", async () => {
  let body;
  const provider = openAIProvider({
    model: config.model,
    apiKey: "fake-key",
    inputUsdPerMillion: 1,
    outputUsdPerMillion: 2,
    maxOutputTokens: 300,
    fetchImpl: async (_url, options) => {
      body = JSON.parse(options.body);
      return {
        ok: true,
        json: async () => ({
          id: "r1",
          model: config.model,
          choices: [
            {
              finish_reason: "stop",
              message: { content: JSON.stringify(candidate) },
            },
          ],
          usage: { prompt_tokens: 100, completion_tokens: 50 },
        }),
      };
    },
  });
  const request = requestFor(record, config);
  assert.ok(provider.reserve(request) > 0);
  assert.equal((await provider.generate(request)).usage.costUsd, 0.0002);
  assert.equal(body.max_completion_tokens, 300);
  assert.equal(body.store, false);
  const blocked = await pilot({
    records: [record],
    config,
    provider,
    budgetUsd: 0,
  });
  assert.equal(blocked.results[0].reason, "budget");
});

module.exports = { config, record, candidate };
