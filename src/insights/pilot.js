const { hash, requestFor, attemptResult } = require("./model");

function sampler(perStratum = 2) {
  if (!Number.isInteger(perStratum) || perStratum < 1 || perStratum > 50)
    throw new Error("Use a sample size between 1 and 50 per stratum.");
  const groups = new Map();
  return {
    add(r) {
      const key = [
        r.term || "unknown",
        r.language || "unknown",
        r.kind,
        /question time|procedur/i.test(r.agendaTitle || "")
          ? "procedural"
          : "policy",
        (r.legacyLabels || []).some((l) => l.confidence < 0.6)
          ? "low"
          : "other",
      ].join("|");
      if (!groups.has(key)) groups.set(key, []);
      const group = groups.get(key);
      group.push({ rank: hash(r.id), record: r });
      group.sort((a, b) => a.rank.localeCompare(b.rank));
      if (group.length > perStratum) group.pop();
    },
    records() {
      return [...groups.values()]
        .flat()
        .map(({ record }) => record)
        .sort((a, b) => a.id.localeCompare(b.id));
    },
  };
}

function selectSample(records, perStratum) {
  const sample = sampler(perStratum);
  for (const record of records) sample.add(record);
  return sample.records();
}

async function pilot({
  records,
  officialEvidence = [],
  config,
  provider,
  budgetUsd = 0,
}) {
  if (!Number.isFinite(budgetUsd) || budgetUsd < 0)
    throw new Error("Nonnegative budget required");
  const prepared = records.map((record) =>
    requestFor(record, config, officialEvidence),
  );
  if (new Set(records.map((record) => record.id)).size !== records.length)
    throw new Error("Duplicate record ID");
  const results = [],
    requests = [];
  let reservedUsd = 0;
  for (const request of prepared) {
    const record = request.input.record;
    requests.push({
      key: request.key,
      recordId: record.id,
      messages: request.messages,
    });
    if (!provider) {
      results.push({
        key: request.key,
        recordId: record.id,
        status: "skipped",
        reason: "provider_disabled",
        legacyLabels: record.legacyLabels || [],
      });
      continue;
    }
    const reserve = provider.reserve(request);
    if (!Number.isFinite(reserve) || reserve < 0)
      throw new Error("Invalid provider reservation");
    if (reservedUsd + reserve > budgetUsd) {
      results.push({
        key: request.key,
        recordId: record.id,
        status: "skipped",
        reason: "budget",
      });
      continue;
    }
    reservedUsd += reserve;
    const result = await attemptResult(request, provider, reserve);
    results.push(result);
    if (result.reason === "usage_above_reservation") break;
  }
  return {
    mode: "pilot",
    config,
    inputFingerprint: hash(records),
    requests,
    results,
    reservedUsd,
    reportedCostUsd: results.reduce(
      (sum, r) => sum + (r.usage?.costUsd || 0),
      0,
    ),
    publicationAllowed: false,
  };
}

function evaluate(report, reviews) {
  const seen = new Set();
  const scores = {
    agenda: [],
    candidateTopic: [],
    baselineTopic: [],
    factuality: [],
    citation: [],
    balance: [],
  };
  for (const review of reviews) {
    const result = report.results.find((r) => r.key === review.key);
    if (
      !result?.candidate ||
      review.candidateHash !== hash(result.candidate) ||
      seen.has(review.key) ||
      !review.reviewer ||
      !review.reviewedAt ||
      !Number.isFinite(Date.parse(review.reviewedAt)) ||
      !["pilot", "heldout"].includes(review.split)
    )
      throw new Error(
        "Review needs a unique key, exact candidate hash, reviewer, timestamp and split",
      );
    for (const dimension of Object.keys(scores)) {
      if (
        !Number.isFinite(review[dimension]) ||
        review[dimension] < 0 ||
        review[dimension] > 1
      )
        throw new Error(`Invalid review score: ${dimension}`);
      scores[dimension].push(review[dimension]);
    }
    seen.add(review.key);
  }
  const means = Object.fromEntries(
    Object.entries(scores).map(([key, values]) => [
      key,
      values.length ? values.reduce((a, b) => a + b, 0) / values.length : null,
    ]),
  );
  return {
    inputFingerprint: report.inputFingerprint,
    reviewed: seen.size,
    total: report.results.length,
    coverage: report.results.length ? seen.size / report.results.length : 0,
    means,
    reviews,
    publicationAllowed: false,
    note: "Human scores do not automatically publish. Approve held-out quality and coverage before promotion.",
  };
}
module.exports = { selectSample, sampler, pilot, evaluate };
