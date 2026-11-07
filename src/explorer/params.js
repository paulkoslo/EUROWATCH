class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}
function scalar(value, name) {
  if (value != null && typeof value !== "string")
    throw new HttpError(400, `${name} must be a single value.`);
  return value;
}
function integer(value, name, fallback, max = 100) {
  if (value == null) return fallback;
  scalar(value, name);
  if (
    !/^\d+$/.test(value) ||
    !Number.isSafeInteger(Number(value)) ||
    Number(value) > max ||
    (name !== "offset" && Number(value) < 1)
  )
    throw new HttpError(
      400,
      `${name} must be ${name === "offset" ? "0" : "1"}–${max}.`,
    );
  return Number(value);
}
function date(value, name, month = false) {
  if (!value) return null;
  scalar(value, name);
  const pattern = month ? /^\d{4}-\d{2}$/ : /^\d{4}-\d{2}-\d{2}$/;
  const full = month ? `${value}-01` : value;
  const parsed = new Date(`${full}T00:00:00Z`);
  if (
    !pattern.test(value) ||
    !Number.isFinite(parsed.getTime()) ||
    parsed.toISOString().slice(0, 10) !== full
  )
    throw new HttpError(
      400,
      `${name} must be a valid ${month ? "YYYY-MM" : "YYYY-MM-DD"} date.`,
    );
  return value;
}
function range(query, { month = false } = {}) {
  const start = date(
    query[month ? "from" : "startDate"],
    month ? "from" : "startDate",
    month,
  );
  const end = date(
    query[month ? "to" : "endDate"],
    month ? "to" : "endDate",
    month,
  );
  if (start && end && start > end)
    throw new HttpError(400, "The start of the range must be before its end.");
  return { start, end };
}
function pagination(query, fallback = 25) {
  return {
    limit: integer(query.limit, "limit", fallback, 100),
    offset: integer(query.offset, "offset", 0, 1000000),
  };
}
function text(value, name = "q", max = 100) {
  const result = (scalar(value, name) || "").trim();
  if (result.length > max)
    throw new HttpError(
      400,
      `${name} is too long (maximum ${max} characters).`,
    );
  return result;
}
function speakerId(value) {
  return integer(value, "speaker ID", null, Number.MAX_SAFE_INTEGER);
}
function sittingId(value) {
  if (!value || value.length > 180 || !/^[a-zA-Z0-9._/-]+$/.test(value))
    throw new HttpError(400, "Invalid sitting ID.");
  return value;
}
function seriesParams(query) {
  const { start, end } = range(query, { month: true });
  const interval = scalar(query.interval, "interval") || "month";
  if (!["month", "quarter", "year"].includes(interval))
    throw new HttpError(400, "Choose month, quarter or year.");
  let topics = null;
  if (query.topics != null) {
    const raw = scalar(query.topics, "topics");
    if (raw.length > 3000)
      throw new HttpError(400, "Topic selection is too long.");
    try {
      topics =
        raw.startsWith("[") || raw.startsWith("{")
          ? JSON.parse(raw)
          : raw.split(",");
    } catch {
      throw new HttpError(400, "Invalid topic selection.");
    }
    if (
      !Array.isArray(topics) ||
      topics.length > 8 ||
      topics.some((topic) => typeof topic !== "string" || topic.length > 250)
    )
      throw new HttpError(400, "Choose up to 8 topics.");
    topics = [
      ...new Set(topics.map((topic) => topic.trim()).filter(Boolean)),
    ].sort();
  }
  return { start, end, interval, topics, top: integer(query.top, "top", 3, 8) };
}
module.exports = {
  HttpError,
  scalar,
  integer,
  date,
  range,
  pagination,
  text,
  speakerId,
  sittingId,
  seriesParams,
};
