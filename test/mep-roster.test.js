const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { createFixture } = require("./fixtures/create");
const { get, open, close, run } = require("../src/data/sqlite");
const {
  syncMepRoster,
  officialMemberships,
} = require("../src/data/mep-roster");

test("official roster preserves dated group changes and avoids inferred ID collisions", async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-roster-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const filename = await createFixture(path.join(dir, "archive.db"));
  const person = (id, label) => ({
    id: `person/${id}`,
    identifier: String(id),
    label,
  });
  const membership = (id, start, end, organization) => ({
    id,
    organization,
    memberDuring: { startDate: start, endDate: end },
    membershipClassification: "def/ep-entities/EU_POLITICAL_GROUP",
  });
  let details = 0;
  const options = {
    date: "2026-03-12",
    async getData(url) {
      if (url.includes("?")) {
        assert.match(url, /parliamentary-term=10/);
        return {
          data: [
            person(2, "Different person"),
            person(4, "Official person"),
            person(999, "Unrelated official"),
          ],
        };
      }
      details++;
      assert.match(url, /\/4$/);
      return {
        data: [
          {
            ...person(4, "Official person"),
            hasEmail: "unused@example.test",
            hasMembership: [
              membership("old", "2024-07-16", "2025-01-31", "org/old"),
              membership("new", "2025-02-01", null, "org/new"),
            ],
          },
        ],
      };
    },
  };
  const result = await syncMepRoster(filename, options);
  assert.deepEqual(result, {
    listed: 3,
    inserted: 1,
    checked: 1,
    changed: 1,
    failed: 0,
    unresolvedIdentities: 2,
    linkedSpeeches: 0,
  });
  const db = await open(filename);
  try {
    const official = await officialMemberships(db, 4);
    assert.equal(
      JSON.stringify(official).includes("unused@example.test"),
      false,
    );
    assert.equal(official.memberships[0].end, "2025-01-31");
    assert.equal(official.memberships[1].organizationId, "org/new");
    assert.equal(await officialMemberships(db, 2), null);
    assert.equal(
      (await get(db, "SELECT label FROM meps WHERE id=2")).label,
      "Bruno Example",
    );
    await run(db, "UPDATE official_mep_current SET checked_at='2000-01-01'");
    await run(db, "UPDATE official_mep_attempts SET attempted_at='2000-01-01'");
  } finally {
    await close(db);
  }
  assert.equal((await syncMepRoster(filename, options)).changed, 0);
  assert.equal((await syncMepRoster(filename, options)).checked, 0);
  assert.equal(details, 2);
});
