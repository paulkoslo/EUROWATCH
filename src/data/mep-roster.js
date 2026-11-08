const { open, close, exec, get, all, run } = require("./sqlite");
const { fetchMemberships } = require("./mep-memberships");
const { getSessionNumber } = require("../core/parliament-fetch");

const api = "https://data.europarl.europa.eu/api/v2/meps";
const limit = 50;
const name = (value) =>
  value.normalize("NFC").trim().replace(/\s+/g, " ").toLowerCase();

async function request(url) {
  const result = await fetch(url, {
    headers: { Accept: "application/ld+json" },
    signal: AbortSignal.timeout(15000),
  });
  if (!result.ok)
    throw new Error(`Official MEP API returned HTTP ${result.status}.`);
  return result.json();
}

async function syncMepRoster(
  filename,
  { date, getData = request, maxDetails = 25, onEvent = () => {} } = {},
) {
  if (!Number.isInteger(maxDetails) || maxDetails < 0 || maxDetails > 50)
    throw new Error("MEP details per run must be between zero and 50.");
  const term = getSessionNumber(date);
  const people = new Map();
  for (let page = 0; page < 25; page++) {
    const payload = await getData(
      `${api}?parliamentary-term=${term}&limit=${limit}&offset=${page * limit}`,
    );
    if (!Array.isArray(payload?.data))
      throw new Error("Unexpected Parliament MEP list.");
    for (const item of payload.data) {
      if (
        !/^person\/\d+$/.test(item.id) ||
        !item.label ||
        item.id !== `person/${item.identifier}`
      )
        throw new Error("Invalid Parliament MEP list identity.");
      if (people.has(Number(item.identifier)))
        throw new Error("The Parliament MEP list repeated an identity.");
      people.set(Number(item.identifier), item);
    }
    if (payload.data.length < limit) break;
    if (page === 24)
      throw new Error("Parliament MEP roster exceeds the page limit.");
  }
  if (!people.size) throw new Error("Parliament returned an empty MEP roster.");
  const db = await open(filename);
  try {
    await exec(
      db,
      `
      CREATE TABLE IF NOT EXISTS official_mep_revisions (
        person_id INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        retrieved_at TEXT NOT NULL,
        source_url TEXT NOT NULL,
        unresolved_count INTEGER NOT NULL,
        PRIMARY KEY (person_id, sha256)
      );
      CREATE TABLE IF NOT EXISTS official_mep_current (
        person_id INTEGER PRIMARY KEY,
        sha256 TEXT NOT NULL,
        checked_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS official_mep_memberships (
        person_id INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        official_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        organization_id TEXT NOT NULL,
        start_date TEXT NOT NULL,
        end_date TEXT,
        PRIMARY KEY (person_id,sha256,official_id)
      );
    `,
    );
    const byId = new Map(
      (await all(db, "SELECT id,label,source FROM meps")).map((row) => [
        row.id,
        row,
      ]),
    );
    const checked = new Map(
      (await all(db, "SELECT * FROM official_mep_current")).map((row) => [
        row.person_id,
        row,
      ]),
    );
    const result = {
      listed: people.size,
      inserted: 0,
      checked: 0,
      changed: 0,
      failed: 0,
      unresolvedIdentities: 0,
    };
    const eligible = [];
    for (const [id, item] of people) {
      const existing = byId.get(id);
      if (
        existing &&
        existing.source !== "api" &&
        name(existing.label || "") !== name(item.label)
      ) {
        result.unresolvedIdentities++;
        continue;
      }
      if (!existing) {
        await run(
          db,
          "INSERT INTO meps (id,label,givenName,familyName,sortLabel,source) VALUES (?,?,?,?,?,'api')",
          [
            id,
            item.label,
            item.givenName ?? null,
            item.familyName ?? null,
            item.sortLabel ?? null,
          ],
        );
        result.inserted++;
      }
      eligible.push(id);
    }
    const earliest = Date.now() - 28 * 86_400_000;
    const candidates = eligible
      .filter(
        (id) =>
          !checked.has(id) || Date.parse(checked.get(id).checked_at) < earliest,
      )
      .sort(
        (a, b) =>
          (checked.get(a)?.checked_at || "").localeCompare(
            checked.get(b)?.checked_at || "",
          ) || a - b,
      )
      .slice(0, maxDetails);
    for (const id of candidates) {
      let record;
      try {
        record = await fetchMemberships(id, getData);
      } catch {
        result.failed++;
        onEvent({ personId: id, outcome: "detail_unavailable" });
        continue;
      }
      const before = checked.get(id);
      result.checked++;
      if (record.fingerprint !== before?.sha256) result.changed++;
      await exec(db, "BEGIN IMMEDIATE");
      try {
        await run(
          db,
          "INSERT OR IGNORE INTO official_mep_revisions VALUES (?,?,?,?,?)",
          [
            id,
            record.fingerprint,
            record.retrievedAt,
            `${api}/${id}`,
            record.unresolved.length,
          ],
        );
        for (const item of record.memberships)
          await run(
            db,
            "INSERT OR IGNORE INTO official_mep_memberships VALUES (?,?,?,?,?,?,?)",
            [
              id,
              record.fingerprint,
              item.id,
              item.kind,
              item.organizationId,
              item.start,
              item.end,
            ],
          );
        await run(
          db,
          `
          INSERT INTO official_mep_current VALUES (?,?,?)
          ON CONFLICT(person_id) DO UPDATE SET sha256=excluded.sha256,checked_at=excluded.checked_at
        `,
          [id, record.fingerprint, record.retrievedAt],
        );
        await exec(db, "COMMIT");
      } catch (error) {
        await exec(db, "ROLLBACK");
        throw error;
      }
    }
    return result;
  } finally {
    await close(db);
  }
}

async function officialMemberships(db, id) {
  if (
    !(await get(
      db,
      "SELECT 1 FROM sqlite_schema WHERE name='official_mep_current'",
    ))
  )
    return null;
  const record = await get(
    db,
    `
    SELECT r.source_url AS sourceUrl,r.sha256,r.retrieved_at AS retrievedAt,r.unresolved_count AS unresolved
    FROM official_mep_current c JOIN official_mep_revisions r
      ON r.person_id=c.person_id AND r.sha256=c.sha256 WHERE c.person_id=?
  `,
    [id],
  );
  if (!record) return null;
  return {
    ...record,
    memberships: await all(
      db,
      `
      SELECT official_id AS id,kind,organization_id AS organizationId,start_date AS start,end_date AS end
      FROM official_mep_memberships WHERE person_id=? AND sha256=? ORDER BY start_date,official_id
    `,
      [id, record.sha256],
    ),
  };
}

module.exports = { syncMepRoster, officialMemberships };
