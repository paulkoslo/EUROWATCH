const { all, get } = require("../data/sqlite");
const { HttpError } = require("./params");

function dateWhere({ start, end }, column = "s.activity_date") {
  const parts = [],
    values = [];
  if (start) {
    parts.push(`${column} >= ?`);
    values.push(start);
  }
  if (end) {
    parts.push(`${column} <= ?`);
    values.push(end);
  }
  return { parts, values };
}
function sourceUrl(date) {
  const terms = [
    ["2024-07-16", 10],
    ["2019-07-02", 9],
    ["2014-07-01", 8],
    ["2009-07-14", 7],
    ["2004-07-20", 6],
    ["1999-07-20", 5],
  ];
  const term = (terms.find(([start]) => date >= start) || ["", 5])[1];
  return `https://www.europarl.europa.eu/doceo/document/CRE-${term}-${date}_EN.html`;
}
async function sittings(dataset, filters, page, speaker = null) {
  const { parts, values } = dateWhere(filters);
  if (speaker) {
    parts.push(
      "EXISTS (SELECT 1 FROM individual_speeches i WHERE i.sitting_id=s.id AND i.mep_id=?)",
    );
    values.push(speaker);
  }
  const where = parts.length ? `WHERE ${parts.join(" AND ")}` : "";
  const total = (
    await get(
      dataset.db,
      `SELECT COUNT(*) AS n FROM sittings s ${where}`,
      values,
    )
  ).n;
  const data = await all(
    dataset.db,
    `SELECT s.id,s.label,s.activity_date AS date,(SELECT COUNT(*) FROM individual_speeches i WHERE i.sitting_id=s.id) AS speechCount FROM sittings s ${where} ORDER BY s.activity_date DESC,s.id DESC LIMIT ? OFFSET ?`,
    [...values, page.limit, page.offset],
  );
  return {
    data: data.map((s) => ({ ...s, sourceUrl: sourceUrl(s.date) })),
    meta: { total, ...page },
  };
}
async function sitting(dataset, id) {
  const row = await get(
    dataset.db,
    "SELECT id,label,activity_date AS date FROM sittings WHERE id=?",
    [id],
  );
  if (!row) throw new HttpError(404, "Sitting not found in this snapshot.");
  return { ...row, sourceUrl: sourceUrl(row.date) };
}
async function speakers(dataset, filters, page) {
  const parts = [],
    values = [];
  if (filters.q) {
    parts.push("label LIKE ? ESCAPE '\\'");
    values.push(`%${filters.q.replace(/[\\%_]/g, "\\$&")}%`);
  }
  if (filters.source) {
    parts.push("source=?");
    values.push(filters.source);
  }
  if (filters.country) {
    parts.push("COALESCE(country,'Unknown')=?");
    values.push(filters.country);
  }
  const where = parts.length ? `WHERE ${parts.join(" AND ")}` : "";
  const total = (
    await get(
      dataset.db,
      `SELECT COUNT(*) AS n FROM explorer_speakers ${where}`,
      values,
    )
  ).n;
  const data = await all(
    dataset.db,
    `SELECT id,label,country,source,speech_count AS speechCount FROM explorer_speakers ${where} ORDER BY speech_count DESC,label COLLATE NOCASE,id LIMIT ? OFFSET ?`,
    [...values, page.limit, page.offset],
  );
  return { data, meta: { total, ...page } };
}
async function speaker(dataset, id) {
  const row = await get(
    dataset.db,
    "SELECT id,label,country,source,speech_count AS speechCount FROM explorer_speakers WHERE id=?",
    [id],
  );
  if (!row) throw new HttpError(404, "Speaker not found in this snapshot.");
  return row;
}
async function speeches(dataset, filters, page) {
  const { parts, values } = dateWhere(filters);
  if (filters.sitting) {
    parts.push("i.sitting_id=?");
    values.push(filters.sitting);
  }
  if (filters.speaker) {
    parts.push("i.mep_id=?");
    values.push(filters.speaker);
  }
  const where = parts.length ? `WHERE ${parts.join(" AND ")}` : "";
  const from = `FROM individual_speeches i JOIN sittings s ON s.id=i.sitting_id LEFT JOIN explorer_speakers m ON m.id=i.mep_id ${where}`;
  const total = (await get(dataset.db, `SELECT COUNT(*) AS n ${from}`, values))
    .n;
  const data = await all(
    dataset.db,
    `SELECT i.id,i.sitting_id AS sittingId,i.mep_id AS speakerId,COALESCE(NULLIF(i.speaker_name,''),m.label,'Unknown speaker') AS speakerName,COALESCE(m.source,'unresolved') AS speakerSource,i.speech_content AS content,i.topic,i.macro_topic AS macroTopic,i.language,i.political_group AS rawGroup,i.political_group_std AS groupLabel,s.activity_date AS date ${from} ORDER BY s.activity_date DESC,s.id DESC,i.speech_order,i.id LIMIT ? OFFSET ?`,
    [...values, page.limit, page.offset],
  );
  return {
    data: data.map((row) => ({ ...row, sourceUrl: sourceUrl(row.date) })),
    meta: { total, ...page },
  };
}
function periodFor(month, interval) {
  return interval === "year"
    ? month.slice(0, 4)
    : interval === "quarter"
      ? `${month.slice(0, 4)}-Q${Math.ceil(Number(month.slice(5)) / 3)}`
      : month;
}
async function series(dataset, params) {
  if (!dataset.analytics)
    throw new HttpError(
      503,
      "Topic analytics is unavailable for this snapshot. Browsing speeches still works.",
    );
  const available = await all(
    dataset.analytics,
    "SELECT topic,count FROM topics ORDER BY count DESC,topic",
  );
  const selected =
    params.topics === null
      ? available.slice(0, params.top).map((row) => row.topic)
      : params.topics;
  const allowed = new Set(available.map((row) => row.topic));
  const topics = selected.filter((name) => allowed.has(name));
  const start = [params.start, dataset.release.stats.fromDate.slice(0, 7)]
    .filter(Boolean)
    .sort()
    .at(-1);
  const end = [params.end, dataset.release.stats.toDate.slice(0, 7)]
    .filter(Boolean)
    .sort()[0];
  const rows =
    topics.length && start <= end
      ? await all(
          dataset.analytics,
          `SELECT period,topic,count FROM topic_series WHERE period>=? AND period<=? AND topic IN (${topics.map(() => "?").join(",")})`,
          [start, end, ...topics],
        )
      : [];
  const coverage = await all(
    dataset.analytics,
    "SELECT period,count FROM coverage WHERE period>=? AND period<=?",
    [start, end],
  );
  const covered = new Map(coverage.map((row) => [row.period, row.count]));
  const buckets = new Map();
  if (topics.length && start <= end) {
    const cursor = new Date(`${start}-01T00:00:00Z`);
    let count = 0;
    while (cursor.toISOString().slice(0, 7) <= end && count++ < 1200) {
      const month = cursor.toISOString().slice(0, 7),
        key = periodFor(month, params.interval);
      if (!buckets.has(key))
        buckets.set(key, { coverage: 0, months: 0, observedMonths: 0 });
      const bucket = buckets.get(key);
      bucket.months++;
      if (covered.has(month)) {
        bucket.coverage += covered.get(month);
        bucket.observedMonths++;
      }
      cursor.setUTCMonth(cursor.getUTCMonth() + 1);
    }
  }
  const labels = [...buckets.keys()];
  const totals = new Map(topics.map((name) => [name, new Map()]));
  for (const row of rows) {
    const map = totals.get(row.topic),
      key = periodFor(row.period, params.interval);
    map.set(key, (map.get(key) || 0) + row.count);
  }
  return {
    datasetId: dataset.release.id,
    interval: params.interval,
    from: start,
    to: end,
    labels,
    datasets: topics.map((label) => ({
      label,
      data: labels.map((key) =>
        buckets.get(key).coverage ? totals.get(label).get(key) || 0 : null,
      ),
    })),
    coverage: labels.map((key) => ({ period: key, ...buckets.get(key) })),
    note: "Counts describe stored speeches with derived agenda-topic labels. A blank period has no stored speeches; partial periods include only observed months.",
  };
}
module.exports = {
  dateWhere,
  sittings,
  sitting,
  speakers,
  speaker,
  speeches,
  series,
  sourceUrl,
};
