const { get, run, exec, open, close } = require("../data/sqlite");
const { validateOfficialContext } = require("./official-context");

const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (key) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        key
      ],
  );
const display = (value) =>
  String(value ?? "")
    .split("/")
    .at(-1)
    .replaceAll("_", " ")
    .toLowerCase();
const categories = {
  procedures: "Legislative procedures",
  committees: "Committee documents",
  questions: "Parliamentary questions",
};

async function storeOfficialContext(filename, input) {
  const context = validateOfficialContext(input);
  if (!context.records.length)
    throw new Error("No verified Parliament context records to publish.");
  const db = await open(filename);
  try {
    await exec(
      db,
      `
      CREATE TABLE IF NOT EXISTS official_context_editions (
        id TEXT PRIMARY KEY, published_at TEXT NOT NULL, context_json TEXT NOT NULL CHECK(json_valid(context_json))
      );
    `,
    );
    return (
      await run(
        db,
        "INSERT OR IGNORE INTO official_context_editions VALUES (?,?,?)",
        [context.contextId, new Date().toISOString(), JSON.stringify(context)],
      )
    ).changes;
  } finally {
    await close(db);
  }
}

async function currentOfficialContext(db) {
  if (
    !(await get(
      db,
      "SELECT 1 FROM sqlite_schema WHERE name='official_context_editions'",
    ))
  )
    return null;
  const row = await get(
    db,
    "SELECT context_json FROM official_context_editions ORDER BY published_at DESC,id DESC LIMIT 1",
  );
  return row ? validateOfficialContext(JSON.parse(row.context_json)) : null;
}

function contextHtml(context) {
  const sections = Object.entries(categories).map(([kind, title]) => {
    const records = context?.records.filter((item) => item.kind === kind) ?? [];
    const items = records
      .map((record) => {
        const events = record.events
          .map(
            (event) =>
              `<li>${escape(event.date)} · <a href="${escape(event.sourceUrl)}">${escape(display(event.type) || event.id)}</a></li>`,
          )
          .join("");
        const coverage =
          record.coverage === "bounded"
            ? "<p>Partial timeline. The collection reached its page limit.</p>"
            : record.coverage === "partial"
              ? "<p>Partial timeline. Parliament returned events without a usable date or identity.</p>"
              : "";
        return `<li><a href="${escape(record.sourceUrl)}">${escape(record.title)}</a><p>${escape(record.id)}${record.date ? " · " + escape(record.date) : ""}</p>${coverage}${events ? "<ol>" + events + "</ol>" : ""}</li>`;
      })
      .join("");
    return `<section class="panel"><h2>${escape(title)}</h2>${items ? "<ul>" + items + "</ul>" : "<p>No records in this edition.</p>"}</section>`;
  });
  return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Parliament records · EUROWATCH</title><link rel="stylesheet" href="/style.css"><main class="container"><p><a href="/">EUROWATCH explorer</a></p><h1>Parliament records</h1><p>Selected official records and timelines. Each entry links to Parliament.</p>${context?.issues.length ? `<p>${context.issues.length} requested records were unavailable during collection.</p>` : ""}${sections.join("")}</main></html>`;
}

module.exports = { storeOfficialContext, currentOfficialContext, contextHtml };
