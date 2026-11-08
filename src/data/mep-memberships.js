const { createHash } = require("node:crypto");
const { isDate } = require("./plenary-import");

// The detail endpoint supplies dated memberships; term list/current-group fields do not.
function normalizeMemberships(person, retrievedAt = new Date().toISOString()) {
  if (!/^person\/\d+$/.test(person?.id) || !Array.isArray(person.hasMembership))
    throw new Error("Expected an official MEP detail record with memberships");
  const memberships = [];
  const unresolved = [];
  for (const entry of person.hasMembership) {
    const kind =
      entry.membershipClassification === "def/ep-entities/EU_POLITICAL_GROUP"
        ? "political_group"
        : entry.role === "def/ep-roles/MEMBER_PARLIAMENT"
          ? "parliament"
          : null;
    if (!kind) continue;
    const start = entry.memberDuring?.startDate;
    const end = entry.memberDuring?.endDate ?? null;
    if (
      !entry.id ||
      !/^org\/[a-zA-Z0-9-]+$/.test(entry.organization) ||
      !isDate(start) ||
      (end !== null && (!isDate(end) || end < start))
    ) {
      unresolved.push({
        id: entry.id || null,
        reason: "invalid_identity_or_period",
      });
      continue;
    }
    memberships.push({
      id: entry.id,
      personId: person.id,
      organizationId: entry.organization,
      kind,
      start,
      end,
      sourceUrl: `https://data.europarl.europa.eu/api/v2/meps/${person.id.split("/")[1]}`,
    });
  }
  memberships.sort(
    (a, b) => a.start.localeCompare(b.start) || a.id.localeCompare(b.id),
  );
  // Hash only the relevant official records, never contact details from the person response.
  return {
    personId: person.id,
    memberships,
    unresolved,
    retrievedAt,
    fingerprint: createHash("sha256")
      .update(JSON.stringify({ memberships, unresolved }))
      .digest("hex"),
  };
}

function affiliationAt(record, date) {
  if (!isDate(date)) throw new Error("Expected a valid affiliation date");
  const active = record.memberships.filter(
    (membership) =>
      membership.start <= date && (!membership.end || membership.end >= date),
  );
  const mandates = active.filter((m) => m.kind === "parliament");
  const groups = active.filter((m) => m.kind === "political_group");
  const organizations = [...new Set(groups.map((m) => m.organizationId))];
  if (record.unresolved.length)
    return { outcome: "unresolved", organizationId: null };
  if (!mandates.length)
    return { outcome: "outside_mandate", organizationId: null };
  if (organizations.length !== 1)
    return {
      outcome: organizations.length ? "ambiguous" : "missing",
      organizationId: null,
    };
  return {
    outcome: "matched",
    organizationId: organizations[0],
    membershipIds: groups.map((m) => m.id),
    mandateIds: mandates.map((m) => m.id),
  };
}

async function fetchMemberships(identifier, request) {
  if (!/^\d+$/.test(String(identifier)))
    throw new Error("Expected a numeric official person ID");
  const url = `https://data.europarl.europa.eu/api/v2/meps/${identifier}`;
  const payload = await request(url, {
    headers: { Accept: "application/ld+json" },
    responseType: "json",
  });
  if (
    !Array.isArray(payload?.data) ||
    payload.data.length !== 1 ||
    payload.data[0].id !== `person/${identifier}`
  )
    throw new Error(
      "Official MEP response does not match the requested identity",
    );
  return normalizeMemberships(payload.data[0]);
}

module.exports = { normalizeMemberships, affiliationAt, fetchMemberships };
