const fs = require("node:fs");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const { hash, requestFor, validateCandidate } = require("./model");
const { durableWrite } = require("./backfill");

const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
function baseUrl(value) {
  const url = new URL(value);
  if (
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    throw new Error(
      "Public base URL must be HTTPS without credentials, query or fragment",
    );
  return url.href.replace(/\/$/, "");
}
function buildPublication({
  corpus,
  report,
  approvals,
  releaseId,
  weekStart,
  publicBaseUrl,
  publishedAt = new Date().toISOString(),
  kinds = ["debate", "sitting"],
}) {
  if (
    !releaseId ||
    typeof releaseId !== "string" ||
    !/^\d{4}-\d{2}-\d{2}$/.test(weekStart) ||
    !Number.isFinite(Date.parse(weekStart)) ||
    new Date(weekStart).toISOString().slice(0, 10) !== weekStart ||
    new Date(weekStart).getUTCDay() !== 1
  )
    throw new Error("Dataset release ID and a Monday weekStart are required");
  if (!Number.isFinite(Date.parse(publishedAt)))
    throw new Error("Invalid publication timestamp");
  const origin = baseUrl(publicBaseUrl);
  const weekEnd = new Date(Date.parse(weekStart) + 7 * 86400000)
    .toISOString()
    .slice(0, 10);
  const byKey = new Map();
  for (const result of report.results) {
    if (byKey.has(result.key)) throw new Error("Duplicate candidate key");
    byKey.set(result.key, result);
  }
  const reviewed = new Map();
  for (const approval of approvals) {
    if (
      reviewed.has(approval.key) ||
      !byKey.has(approval.key) ||
      approval.approved !== true ||
      !approval.reviewer?.trim() ||
      !Number.isFinite(Date.parse(approval.reviewedAt))
    )
      throw new Error("Explicit unique reviewed approval required");
    if (approval.candidateHash !== hash(byKey.get(approval.key).candidate))
      throw new Error("Approval does not bind the current candidate");
    reviewed.set(approval.key, approval);
  }
  const entries = [],
    audit = [];
  let eligible = 0;
  const ids = new Set();
  for (const record of corpus.records) {
    if (ids.has(record.id)) throw new Error("Duplicate corpus record ID");
    ids.add(record.id);
    if (
      !kinds.includes(record.kind) ||
      record.effectiveAt.slice(0, 10) < weekStart ||
      record.effectiveAt.slice(0, 10) >= weekEnd
    )
      continue;
    eligible++;
    const request = requestFor(
      record,
      corpus.config,
      corpus.officialEvidence || [],
    );
    const result = byKey.get(request.key),
      approval = reviewed.get(request.key);
    if (!approval) continue;
    if (
      result.status !== "candidate" ||
      result.recordId !== record.id ||
      result.inputRevision !== record.inputRevision ||
      result.candidate.uncertain
    )
      throw new Error("Only current, certain candidates can be published");
    const candidate = validateCandidate(result.candidate, request);
    if (record.scope?.complete === false && approval.allowPartial !== true)
      throw new Error(
        "Partial evidence requires explicit partial-scope approval",
      );
    const slug = hash(record.id).slice(0, 24);
    entries.push({
      id: record.id,
      slug,
      kind: record.kind,
      title: record.agendaTitle || record.id,
      effectiveAt: record.effectiveAt,
      inputRevision: record.inputRevision,
      sourceKey: request.key,
      scope: record.scope || { complete: true },
      topics: candidate.topics,
      claims: candidate.claims,
      officialEvidence: request.input.officialEvidence.map((e) => ({
        id: e.id,
        kind: e.kind,
        sourceUrl: e.sourceUrl,
        sourceRevision: e.sourceRevision,
        effectiveAt: e.effectiveAt,
        ...(e.kind === "vote" ? { totals: e.totals } : {}),
      })),
      missingEvidence: ["vote", "adopted_text"].filter(
        (kind) => !request.input.officialEvidence.some((e) => e.kind === kind),
      ),
    });
    audit.push({
      ...approval,
      recordId: record.id,
      inputRevision: record.inputRevision,
      versions: request.versions,
    });
  }
  if (!entries.length)
    throw new Error("No approved current candidates in this week");
  entries.sort(
    (a, b) =>
      a.effectiveAt.localeCompare(b.effectiveAt) || a.id.localeCompare(b.id),
  );
  const content = {
    schemaVersion: 1,
    releaseId,
    weekStart,
    weekEndExclusive: weekEnd,
    publishedAt,
    publicBaseUrl: origin,
    title: `EUROWATCH · Week of ${weekStart}`,
    coverage: {
      eligible,
      approved: entries.length,
      remaining: eligible - entries.length,
      partialEntries: entries.filter((e) => e.scope.complete === false).length,
    },
    entries,
  };
  const publicationId = hash(content);
  return {
    public: { ...content, publicationId },
    privateReview: { publicationId, releaseId, approvals: audit },
  };
}

function entryHtml(entry, publication) {
  const link = `${publication.publicBaseUrl}/briefings/${publication.publicationId}/${entry.slug}`;
  return `<article id="${escape(entry.slug)}"><h2><a href="${escape(link)}">${escape(entry.title)}</a></h2><p>${escape(entry.effectiveAt.slice(0, 10))} · ${escape(entry.kind)}${entry.scope.complete === false ? ` · Partial evidence, part ${escape(entry.scope.part)} of ${escape(entry.scope.parts)}` : ""}</p><p>${entry.topics.map((t) => escape(t.label)).join(" · ")}</p><ul>${entry.claims.map((c) => `<li>${escape(c.text)} <a href="${escape(c.sourceUrl)}">Source</a><blockquote>${escape(c.quote)}</blockquote>${c.speakerId ? `<small>Speaker: ${escape(c.speakerId)}</small>` : ""}</li>`).join("")}</ul>${entry.officialEvidence.map((e) => `<p><a href="${escape(e.sourceUrl)}">Official ${escape(e.kind.replace("_", " "))}</a>${e.kind === "vote" ? `: ${escape(e.totals.for)} for, ${escape(e.totals.against)} against, ${escape(e.totals.abstention)} abstentions` : ""}</p>`).join("")}${entry.missingEvidence.length ? `<p>Unavailable official links: ${entry.missingEvidence.map(escape).join(", ")}.</p>` : ""}</article>`;
}
function html(publication, entries = publication.entries) {
  return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escape(publication.title)}</title><body><main><h1>${escape(publication.title)}</h1><p>Reviewed source-backed summaries · Dataset ${escape(publication.releaseId)}</p><p>${publication.coverage.approved} of ${publication.coverage.eligible} eligible records reviewed; ${publication.coverage.partialEntries} use partial evidence.</p>${entries.map((e) => entryHtml(e, publication)).join("")}<footer>Source: European Parliament. Summaries are editorial interpretations; consult the cited record.</footer></main></body></html>`;
}
function newsletterText(publication) {
  return (
    `${publication.title}\nDataset ${publication.releaseId}\nReviewed coverage: ${publication.coverage.approved}/${publication.coverage.eligible}\n\n` +
    publication.entries
      .map(
        (e) =>
          `${e.title}${e.scope.complete === false ? ` [partial evidence ${e.scope.part}/${e.scope.parts}]` : ""}\n${e.claims.map((c) => `- ${c.text}\n  ${c.quote}\n  ${c.sourceUrl}`).join("\n")}\n${publication.publicBaseUrl}/briefings/${publication.publicationId}/${e.slug}`,
      )
      .join("\n\n")
  );
}
function rss(publication) {
  return `<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>${escape(publication.title)}</title><link>${escape(publication.publicBaseUrl)}/briefings/${publication.publicationId}</link><description>Reviewed European Parliament briefings</description>${publication.entries
    .map((e) => {
      const url = `${publication.publicBaseUrl}/briefings/${publication.publicationId}/${e.slug}`;
      return `<item><title>${escape(e.title)}${e.scope.complete === false ? " (partial evidence)" : ""}</title><link>${escape(url)}</link><guid isPermaLink="true">${escape(url)}</guid><pubDate>${new Date(publication.publishedAt).toUTCString()}</pubDate><description>${escape(e.claims.map((c) => `${c.text} Source: ${c.sourceUrl}`).join("\n"))}</description></item>`;
    })
    .join("")}</channel></rss>`;
}
function writePublication({ publication, directory, privateDirectory }) {
  directory = path.resolve(directory);
  privateDirectory = path.resolve(privateDirectory);
  if (
    directory === privateDirectory ||
    privateDirectory.startsWith(directory + path.sep) ||
    directory.startsWith(privateDirectory + path.sep)
  )
    throw new Error("Public and private artifact roots must be separate");
  fs.mkdirSync(directory, { recursive: true });
  fs.mkdirSync(privateDirectory, { recursive: true, mode: 0o700 });
  const id = publication.public.publicationId;
  const work = path.join(directory, `.staging-${randomUUID()}`),
    target = path.join(directory, id);
  fs.mkdirSync(work);
  try {
    durableWrite(path.join(work, "public.json"), publication.public);
    for (const [name, content] of Object.entries({
      "index.html": html(publication.public),
      "newsletter.html": html(publication.public),
      "newsletter.txt": newsletterText(publication.public),
      "feed.xml": rss(publication.public),
    }))
      fs.writeFileSync(path.join(work, name), content, { mode: 0o644 });
    durableWrite(
      path.join(privateDirectory, `${id}.json`),
      publication.privateReview,
    );
    if (fs.existsSync(target)) {
      const existing = JSON.parse(
        fs.readFileSync(path.join(target, "public.json"), "utf8"),
      );
      if (hash(existing) !== hash(publication.public))
        throw new Error("Publication ID collision");
    } else fs.renameSync(work, target);
    return { publicationId: id, directory: target };
  } finally {
    fs.rmSync(work, { recursive: true, force: true });
  }
}
function readPublication(directory, id) {
  if (!/^[a-f0-9]{64}$/.test(id)) throw new Error("Invalid publication ID");
  const publication = JSON.parse(
    fs.readFileSync(path.join(directory, id, "public.json"), "utf8"),
  );
  const { publicationId, ...content } = publication;
  if (publicationId !== id || hash(content) !== id)
    throw new Error("Publication integrity mismatch");
  return publication;
}

if (require.main === module) {
  try {
    const { options, read } = require("./cli");
    const opts = options(process.argv.slice(2));
    const publication = buildPublication({
      corpus: read(opts.corpus),
      report: read(opts.report),
      approvals: read(opts.approvals),
      releaseId: opts["release-id"],
      weekStart: opts.week,
      publicBaseUrl: opts["base-url"],
      publishedAt: opts["published-at"],
    });
    console.log(
      JSON.stringify(
        writePublication({
          publication,
          directory: opts.output,
          privateDirectory: opts["private-output"],
        }),
      ),
    );
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
module.exports = {
  buildPublication,
  writePublication,
  readPublication,
  html,
  rss,
  newsletterText,
};
