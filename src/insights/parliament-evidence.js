const { hash } = require("./model");
const { options, write } = require("./cli");
const { isDate } = require("../data/plenary-import");

const origin = "https://data.europarl.europa.eu";
const pageSize = 50;
const docId = /^eli\/dl\/doc\/[A-Z0-9-]+$/;
const eventId = /^eli\/dl\/event\/[A-Z0-9-]+$/;
const unique = (values) => [...new Set(values)].sort();
const refs = (value, pattern) =>
  Array.isArray(value)
    ? value.filter((item) => typeof item === "string" && pattern.test(item))
    : [];

function stable(value) {
  if (Array.isArray(value))
    return value
      .map(stable)
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, stable(value[key])]),
    );
  return value;
}

async function request(url) {
  const response = await fetch(url, {
    headers: { Accept: "application/ld+json" },
    signal: AbortSignal.timeout(25_000),
  });
  if (response.status === 204) return { data: [] };
  if (!response.ok)
    throw new Error(`Parliament evidence returned HTTP ${response.status}.`);
  return response.json();
}

async function rows(resource, get, params = {}, { paginate = true } = {}) {
  const data = new Map();
  for (let page = 0; page < 30; page++) {
    const url = new URL(`/api/v2/${resource}`, origin);
    for (const [key, value] of Object.entries(params))
      url.searchParams.set(key, value);
    if (paginate) {
      url.searchParams.set("limit", pageSize);
      url.searchParams.set("offset", page * pageSize);
    }
    const payload = await get(url.href);
    if (!Array.isArray(payload?.data))
      throw new Error("Unexpected Parliament evidence response.");
    for (const row of payload.data) {
      if (typeof row?.id !== "string")
        throw new Error("Official evidence record has no ID.");
      if (
        data.has(row.id) &&
        hash(stable(data.get(row.id))) !== hash(stable(row))
      )
        throw new Error(
          `Conflicting official evidence for ${resource} record ${row.id}.`,
        );
      data.set(row.id, row);
    }
    if (!paginate || payload.data.length < pageSize) return [...data.values()];
  }
  throw new Error("Parliament evidence exceeds the bounded page limit.");
}

function normalize(
  day,
  meeting,
  decisions,
  voteGroups,
  adopted,
  activities = [],
) {
  const meetingId = meeting.id;
  const creIds = refs(meeting.recorded_in_a_realization_of, docId)
    .map((id) => id.split("/").at(-1))
    .filter((id) => /^CRE-\d+-\d{4}-\d{2}-\d{2}$/.test(id) && id.endsWith(day));
  if (!creIds.length) return [];
  const groups = new Map(
    voteGroups
      .filter((row) => eventId.test(row.id))
      .map((row) => [row.id, row]),
  );
  const validDecisions = decisions.filter(
    (row) =>
      eventId.test(row.id) &&
      row.activity_date === day &&
      row.id.startsWith(meetingId + "-"),
  );
  const activityIds = new Set([
    meetingId,
    ...refs(meeting.consists_of, eventId),
    ...validDecisions.map((row) => row.id),
    ...groups.keys(),
    ...activities
      .filter(
        (row) =>
          eventId.test(row.id) &&
          row.id.startsWith(meetingId + "-") &&
          row.activity_date === day,
      )
      .flatMap((row) => [row.id, ...refs(row.consists_of, eventId)]),
  ]);
  const base = (row, kind, officialIds) => ({
    id: row.id,
    kind,
    title:
      row.activity_label?.en ||
      row.title_dcterms?.en ||
      row.id.split("/").at(-1),
    officialIds: unique([...creIds, ...officialIds]),
    sourceUrl: `${origin}/${row.id}`,
    sourceRevision: `sha256:${hash(stable(row))}`,
    effectiveAt: `${day}T00:00:00.000Z`,
  });
  const votes = [];
  for (const row of validDecisions) {
    const totals = {
      for: row.number_of_votes_favor,
      against: row.number_of_votes_against,
      abstention: row.number_of_votes_abstention,
    };
    const missing = Object.keys(totals).filter((key) => totals[key] == null);
    if (
      missing.length === 1 &&
      Number.isInteger(row.number_of_attendees) &&
      Object.values(totals)
        .filter((value) => value != null)
        .every((value) => Number.isInteger(value) && value >= 0) &&
      Object.values(totals).reduce((sum, value) => sum + (value ?? 0), 0) ===
        row.number_of_attendees
    )
      totals[missing[0]] = 0;
    if (
      !Object.values(totals).every(
        (value) => Number.isInteger(value) && value >= 0,
      )
    )
      continue;
    const parentDocuments = refs(row.inverse_consists_of, eventId).flatMap(
      (id) => refs(groups.get(id)?.based_on_a_realization_of, docId),
    );
    votes.push({
      ...base(row, "vote", [
        row.id,
        ...parentDocuments,
        ...refs(row.decided_on_a_realization_of, docId),
      ]),
      totals,
    });
  }
  const texts = adopted
    .filter((row) => {
      if (
        !docId.test(row.id) ||
        !row.id.startsWith("eli/dl/doc/TA-") ||
        row.document_date !== day
      )
        return false;
      const relations = [
        ...refs(row.inverse_created_a_realization_of, eventId),
        ...refs(row.inverse_decided_on_a_realization_of, eventId),
      ];
      return relations.some((id) => activityIds.has(id));
    })
    .map((row) =>
      base(row, "adopted_text", [row.id, ...refs(row.adopts, docId)]),
    );
  return [...votes, ...texts].sort((a, b) => a.id.localeCompare(b.id));
}

async function loadEvidence({ from, to, get = request }) {
  if (
    !isDate(from) ||
    !isDate(to) ||
    from > to ||
    Date.parse(to) - Date.parse(from) > 7 * 86_400_000
  )
    throw new Error(
      "Use an inclusive official evidence window of up to eight dates.",
    );
  const yearData = new Map();
  const adoptedByYear = new Map();
  const evidence = [];
  for (let ts = Date.parse(from); ts <= Date.parse(to); ts += 86_400_000) {
    const date = new Date(ts).toISOString().slice(0, 10);
    const year = date.slice(0, 4);
    if (!yearData.has(year))
      yearData.set(year, await rows("meetings", get, { year }));
    const meetings = yearData
      .get(year)
      .filter(
        (row) =>
          row.activity_date === date &&
          row.had_activity_type === "def/ep-activities/PLENARY_SITTING" &&
          eventId.test(row.id),
      );
    if (meetings.length > 1)
      throw new Error(`Calendar has multiple plenary identities for ${date}.`);
    if (!meetings.length) continue;
    const meeting = meetings[0];
    const key = meeting.id.split("/").at(-1);
    if (!adoptedByYear.has(year))
      adoptedByYear.set(year, await rows("adopted-texts", get, { year }));
    const [decisions, voteGroups, activities] = await Promise.all([
      rows(`meetings/${key}/decisions`, get),
      rows(`meetings/${key}/vote-results`, get, {}, { paginate: false }),
      rows(`meetings/${key}/activities`, get),
    ]);
    evidence.push(
      ...normalize(
        date,
        meeting,
        decisions,
        voteGroups,
        adoptedByYear.get(year),
        activities,
      ),
    );
  }
  return evidence;
}

if (require.main === module) {
  const args = options(process.argv.slice(2));
  if (!args.output) throw new Error("Provide --from, --to and --output.");
  loadEvidence(args)
    .then((data) => {
      write(args.output, data);
      console.log(
        JSON.stringify({
          votes: data.filter((item) => item.kind === "vote").length,
          adoptedTexts: data.filter((item) => item.kind === "adopted_text")
            .length,
        }),
      );
    })
    .catch((error) => {
      console.error(error.message);
      process.exitCode = 1;
    });
}

module.exports = { loadEvidence, normalize };
