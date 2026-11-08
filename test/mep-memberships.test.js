const test = require("node:test");
const assert = require("node:assert/strict");
const {
  normalizeMemberships,
  affiliationAt,
  fetchMemberships,
} = require("../src/data/mep-memberships");
const member = (
  id,
  organization,
  startDate,
  endDate,
  kind = "political_group",
) => ({
  id,
  organization,
  memberDuring: { startDate, ...(endDate ? { endDate } : {}) },
  ...(kind === "parliament"
    ? { role: "def/ep-roles/MEMBER_PARLIAMENT" }
    : { membershipClassification: "def/ep-entities/EU_POLITICAL_GROUP" }),
});

test("official dated affiliations preserve transitions, open periods and ambiguity without today's group leaking backward", () => {
  const person = {
    id: "person/123",
    hasEmail: "private@example.test",
    hasMembership: [
      member("mandate", "org/ep-10", "2024-07-16", null, "parliament"),
      member("old", "org/1", "2024-07-16", "2025-01-31"),
      member("new", "org/2", "2025-02-01"),
    ],
  };
  const record = normalizeMemberships(person, "2026-11-07T00:00:00Z");
  assert.equal(affiliationAt(record, "2025-01-31").organizationId, "org/1");
  assert.equal(affiliationAt(record, "2025-02-01").organizationId, "org/2");
  assert.equal(affiliationAt(record, "2024-07-15").outcome, "outside_mandate");
  assert.equal(JSON.stringify(record).includes("private@example.test"), false);
  assert.equal(
    normalizeMemberships(person, "later").fingerprint,
    record.fingerprint,
  );
  person.hasMembership.push(member("conflict", "org/3", "2025-02-01"));
  assert.equal(
    affiliationAt(normalizeMemberships(person), "2025-02-01").outcome,
    "ambiguous",
  );
  person.hasMembership.push(member("broken", "org/4", "not-a-date"));
  assert.equal(
    affiliationAt(normalizeMemberships(person), "2025-02-01").outcome,
    "unresolved",
  );
});

test("detail fetch verifies the official person identity", async () => {
  await assert.rejects(
    fetchMemberships(123, async () => ({
      data: [{ id: "person/456", hasMembership: [] }],
    })),
    /identity/,
  );
});
