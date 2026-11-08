const { hash } = require("./model");
const { options, write } = require("./cli");

const origin = "https://data.europarl.europa.eu";
const patterns = {
  procedures: /^\d{4}-\d{4}[A-Z]?$/,
  questions: /^[A-Z]-\d+-\d{4}-\d{4,6}$/,
  committees: /^[A-Z0-9]+(?:-[A-Z0-9]+)+$/,
};
const resources = {
  procedures: "procedures",
  questions: "parliamentary-questions",
  committees: "committee-documents",
};
const isoDate = (value) =>
  typeof value === "string" &&
  /^\d{4}-\d{2}-\d{2}$/.test(value) &&
  Number.isFinite(Date.parse(value)) &&
  new Date(value).toISOString().slice(0, 10) === value;
const label = (value) => (typeof value === "string" ? value : value?.en);
const references = (value) =>
  Array.isArray(value)
    ? value
        .filter(
          (item) =>
            typeof item === "string" &&
            /^eli\/dl\/(doc|event|proc)\/[A-Z0-9-]+$/.test(item),
        )
        .sort()
    : [];

async function request(url) {
  const result = await fetch(url, {
    headers: { Accept: "application/ld+json" },
    signal: AbortSignal.timeout(25_000),
  });
  if (result.status === 204 || result.status === 404) return { data: [] };
  if (!result.ok) throw new Error(`Parliament returned HTTP ${result.status}.`);
  return result.json();
}

function identifiers(values, kind) {
  if (
    !Array.isArray(values) ||
    values.length > 20 ||
    values.some(
      (value) =>
        typeof value !== "string" ||
        value.length > 70 ||
        !patterns[kind].test(value),
    )
  )
    throw new Error(`Use up to 20 official ${kind} identifiers.`);
  return [...new Set(values)];
}

async function retrieve(get, pathname, query = {}) {
  const url = new URL(`/api/v2/${pathname}`, origin);
  for (const [key, value] of Object.entries(query))
    url.searchParams.set(key, value);
  const result = await get(url.href);
  if (!Array.isArray(result?.data))
    throw new Error("Unexpected official Parliament response.");
  return result.data;
}

function recordOf(row, kind, id, events) {
  const prefix = kind === "procedures" ? "eli/dl/proc/" : "eli/dl/doc/";
  if (row.id !== prefix + id)
    throw new Error(
      "The returned source identity differs from the requested record.",
    );
  const date =
    [row.document_date, row.activity_date, row.start_date].find(isoDate) ||
    null;
  const title =
    label(row.title_dcterms) ||
    label(row.activity_label) ||
    label(row.label) ||
    row.label ||
    id;
  const relations = [
    ...new Set(
      Object.entries(row)
        .filter(([key]) =>
          /^(inverse_|adopts$|is_about$|refers_to$|based_on|created_a_realization_of)/.test(
            key,
          ),
        )
        .flatMap(([, value]) => references(value)),
    ),
  ].sort();
  return {
    id,
    kind,
    sourceId: row.id,
    sourceUrl: `${origin}/${row.id}`,
    sourceRevision: hash({ id: row.id, date, title, relations, events }),
    date,
    title: typeof title === "string" ? title : id,
    type: typeof row.work_type === "string" ? row.work_type : null,
    relations,
    events,
  };
}

async function procedureEvents(get, id) {
  const events = new Map();
  for (let page = 0; page < 8; page++) {
    const rows = await retrieve(get, `procedures/${id}/events`, {
      limit: 50,
      offset: page * 50,
    });
    let added = 0;
    for (const row of rows) {
      if (
        typeof row?.id !== "string" ||
        !/^eli\/dl\/event\/[A-Z0-9-]+$/.test(row.id) ||
        !isoDate(row.activity_date)
      )
        continue;
      const value = {
        id: row.id,
        date: row.activity_date,
        type: row.had_activity_type || null,
        stage: row.occured_at_stage || null,
        sourceUrl: `${origin}/${row.id}`,
        documents: references(row.documented_by_a_realization_of),
      };
      if (!events.has(row.id)) {
        events.set(row.id, value);
        added++;
      }
    }
    if (rows.length < 50)
      return {
        coverage: "complete_for_request",
        events: [...events.values()].sort(
          (a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id),
        ),
      };
    if (!added) break;
  }
  return {
    coverage: "bounded",
    events: [...events.values()].sort(
      (a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id),
    ),
  };
}

async function collectOfficialContext({
  procedures = [],
  questions = [],
  committees = [],
  get = request,
} = {}) {
  const byKind = { procedures, questions, committees };
  const records = [];
  const issues = [];
  for (const [kind, values] of Object.entries(byKind)) {
    for (const id of identifiers(values, kind)) {
      try {
        const rows = await retrieve(get, `${resources[kind]}/${id}`);
        if (!rows.length) {
          issues.push({ kind, id, outcome: "unavailable" });
          continue;
        }
        if (rows.length !== 1)
          throw new Error("Multiple records for one identity.");
        const timeline =
          kind === "procedures"
            ? await procedureEvents(get, id)
            : { coverage: "single_record", events: [] };
        records.push({
          ...recordOf(rows[0], kind, id, timeline.events),
          coverage: timeline.coverage,
        });
      } catch {
        issues.push({ kind, id, outcome: "source_error" });
      }
    }
  }
  const data = {
    schemaVersion: 1,
    coverage: "targeted_official_records",
    records,
    issues,
  };
  return { ...data, contextId: hash(data) };
}

function validateOfficialContext(value) {
  if (
    !value ||
    value.schemaVersion !== 1 ||
    value.coverage !== "targeted_official_records" ||
    !Array.isArray(value.records) ||
    value.records.length > 60 ||
    !Array.isArray(value.issues)
  )
    throw new Error("Invalid official context bundle.");
  const { contextId, ...data } = value;
  if (contextId !== hash(data))
    throw new Error("Official context hash mismatch.");
  for (const record of value.records) {
    if (
      !Object.hasOwn(patterns, record.kind) ||
      !patterns[record.kind].test(record.id) ||
      record.sourceId !==
        `eli/dl/${record.kind === "procedures" ? "proc" : "doc"}/${record.id}` ||
      record.sourceUrl !== `${origin}/${record.sourceId}` ||
      !Array.isArray(record.events) ||
      record.events.length > 400 ||
      !record.events.every(
        (event) =>
          typeof event.id === "string" &&
          /^eli\/dl\/event\/[A-Z0-9-]+$/.test(event.id) &&
          event.sourceUrl === `${origin}/${event.id}`,
      )
    )
      throw new Error("Invalid official context source identity.");
  }
  return value;
}

if (require.main === module) {
  const opts = options(process.argv.slice(2));
  if (!opts.output)
    throw new Error("Provide --output and comma-separated official IDs.");
  collectOfficialContext({
    procedures: opts.procedures?.split(","),
    questions: opts.questions?.split(","),
    committees: opts.committees?.split(","),
  })
    .then((bundle) => {
      write(opts.output, bundle);
      console.log(
        JSON.stringify({
          contextId: bundle.contextId,
          records: bundle.records.length,
          issues: bundle.issues,
        }),
      );
    })
    .catch((error) => {
      console.error(error.message);
      process.exitCode = 1;
    });
}
module.exports = { collectOfficialContext, validateOfficialContext };
