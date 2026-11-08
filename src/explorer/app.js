const express = require("express");
const path = require("node:path");
const { once } = require("node:events");
const { all } = require("../data/sqlite");
const params = require("./params");
const queries = require("./queries");

const fields = {
  id: "i.id",
  sitting_id: "i.sitting_id",
  date: "s.activity_date",
  speaker_name: "i.speaker_name",
  political_group: "i.political_group",
  title: "i.title",
  speech_content: "i.speech_content",
  language: "i.language",
  macro_topic: "i.macro_topic",
  specific_focus: "i.macro_specific_focus",
  topic: "i.topic",
  country: "m.country",
  mep_id: "i.mep_id",
};
function csv(value) {
  let text = value == null ? "" : String(value);
  if (/^\s*[=+\-@]/.test(text) || /^[\t\r]/.test(text)) text = `'${text}`;
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}
function limiter() {
  const clients = new Map();
  return (req, res, next) => {
    const now = Date.now(),
      key = req.ip;
    if (!clients.has(key) || clients.get(key).until <= now)
      clients.set(key, { count: 0, until: now + 60000 });
    if (clients.size > 10000) clients.delete(clients.keys().next().value);
    const client = clients.get(key);
    if (client && ++client.count > 300) {
      res.set("Retry-After", "60");
      return res
        .status(429)
        .json({ error: "Too many requests. Please try again in a minute." });
    }
    next();
  };
}
function createApp(
  dataset,
  { fullDownload = true, trustProxy = false, logger = console } = {},
) {
  const app = express();
  app.disable("x-powered-by");
  if (trustProxy) app.set("trust proxy", 1);
  app.use((req, res, next) => {
    res.set({
      "X-Content-Type-Options": "nosniff",
      "Referrer-Policy": "strict-origin-when-cross-origin",
      "Content-Security-Policy":
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
    });
    next();
  });
  app.get("/healthz", (req, res) => res.json({ status: "ok" }));
  app.get("/readyz", (req, res) =>
    res.json({
      ready: true,
      datasetId: dataset.release.id,
      analyticsAvailable: !!dataset.analytics,
    }),
  );
  app.use("/api", limiter(), (req, res, next) => {
    res.set("X-Dataset-ID", dataset.release.id);
    res.set("Cache-Control", "no-store");
    if (!["GET", "HEAD"].includes(req.method))
      return res.status(405).set("Allow", "GET, HEAD").json({
        error: "This explorer is read-only. Use offline maintenance commands.",
      });
    next();
  });
  app.get("/api/data-status", (req, res) =>
    res.json({
      ...dataset.release,
      coverage: {
        latestStoredSitting: dataset.release.stats.toDate,
        lastSuccessfulRefresh: dataset.release.refresh?.completedAt || null,
        pendingDates: dataset.release.refresh?.pendingDates ?? null,
        failedDates: dataset.release.refresh?.failedDates ?? null,
      },
      analyticsAvailable: !!dataset.analytics,
      analyticsError: dataset.analytics
        ? null
        : "Matching analytics is unavailable. Prepare the snapshot again.",
    }),
  );
  app.get("/api/localrun", (req, res) => res.json({ localrun: false }));
  app.get("/api/sittings", async (req, res) =>
    res.json(
      await queries.sittings(
        dataset,
        params.range(req.query),
        params.pagination(req.query),
      ),
    ),
  );
  app.get("/api/sittings/:id", async (req, res) =>
    res.json(await queries.sitting(dataset, params.sittingId(req.params.id))),
  );
  app.get("/api/sittings/:id/speeches", async (req, res) => {
    const id = params.sittingId(req.params.id);
    await queries.sitting(dataset, id);
    res.json(
      await queries.speeches(
        dataset,
        { sitting: id },
        params.pagination(req.query),
      ),
    );
  });
  app.get("/api/speakers", async (req, res) => {
    const source = params.text(req.query.source, "source");
    if (source && !["api", "historic", "unresolved"].includes(source))
      throw new params.HttpError(400, "Unknown speaker record source.");
    res.json(
      await queries.speakers(
        dataset,
        {
          q: params.text(req.query.q),
          source,
          country: params.text(req.query.country, "country", 20),
        },
        params.pagination(req.query),
      ),
    );
  });
  app.get("/api/speakers/:id", async (req, res) =>
    res.json(await queries.speaker(dataset, params.speakerId(req.params.id))),
  );
  app.get("/api/speakers/:id/speeches", async (req, res) => {
    const id = params.speakerId(req.params.id);
    await queries.speaker(dataset, id);
    res.json(
      await queries.speeches(
        dataset,
        { speaker: id, ...params.range(req.query) },
        params.pagination(req.query),
      ),
    );
  });
  app.get("/api/speeches", async (req, res) =>
    res.json(
      await queries.sittings(
        dataset,
        params.range(req.query),
        params.pagination(req.query),
        req.query.personId ? params.speakerId(req.query.personId) : null,
      ),
    ),
  );
  app.get("/api/topics", async (req, res) => {
    if (!dataset.analytics)
      throw new params.HttpError(
        503,
        "Matching topic analytics is unavailable.",
      );
    res.json({
      data: await all(
        dataset.analytics,
        "SELECT topic,count FROM topics ORDER BY count DESC,topic",
      ),
    });
  });
  const cache = new Map();
  async function topicSeries(req, res) {
    const filters = params.seriesParams(req.query),
      key = JSON.stringify(filters),
      cached = cache.get(key);
    let result;
    if (cached && cached.expires > Date.now()) {
      result = cached.value;
      res.set("X-Cache", "HIT");
    } else {
      result = await queries.series(dataset, filters);
      if (cache.size >= 100) cache.delete(cache.keys().next().value);
      cache.set(key, { value: result, expires: Date.now() + 180000 });
      res.set("X-Cache", "MISS");
    }
    return result;
  }
  app.get("/api/analytics/time-series", async (req, res) =>
    res.json(await topicSeries(req, res)),
  );
  app.get("/api/export/topics", async (req, res) => {
    const result = await topicSeries(req, res);
    res.type("text/csv").attachment("eurowatch-topic-series.csv");
    const rows = [
      [
        "period",
        ...result.datasets.map((d) => d.label),
        "stored_speech_count",
        "observed_months",
        "calendar_months",
        "classified_speech_count",
        "classification_rate",
        "classified_months",
      ],
    ];
    for (let i = 0; i < result.labels.length; i++)
      rows.push([
        result.labels[i],
        ...result.datasets.map((d) => d.data[i]),
        result.coverage[i].coverage,
        result.coverage[i].observedMonths,
        result.coverage[i].months,
        result.coverage[i].classified,
        result.coverage[i].classificationRate,
        result.coverage[i].classifiedMonths,
      ]);
    res.send(
      "\ufeff" +
        rows.map((row) => row.map(csv).join(",")).join("\r\n") +
        "\r\n",
    );
  });
  let exportsInFlight = 0;
  app.get("/api/export/speeches", async (req, res) => {
    const range = params.range(req.query),
      where = queries.dateWhere(range),
      filter = where.parts.length ? `WHERE ${where.parts.join(" AND ")}` : "";
    const selected =
      req.query.fields == null
        ? Object.keys(fields)
        : params.text(req.query.fields, "fields", 500).split(",");
    if (
      !selected.length ||
      selected.some((name) => !Object.hasOwn(fields, name)) ||
      new Set(selected).size !== selected.length
    )
      throw new params.HttpError(400, "Choose valid, unique export fields.");
    const count = await all(
      dataset.db,
      `SELECT COUNT(*) AS n FROM individual_speeches i JOIN sittings s ON s.id=i.sitting_id ${filter}`,
      where.values,
    );
    if (req.query.countOnly === "true")
      return res.json({ count: count[0].n, maxRows: 10000 });
    if (count[0].n > 10000)
      throw new params.HttpError(
        400,
        "This selection exceeds 10,000 speeches. Choose a narrower date range.",
      );
    if (exportsInFlight >= 2)
      throw new params.HttpError(
        429,
        "Two exports are already running. Please try again shortly.",
      );
    exportsInFlight++;
    try {
      res.type("text/csv").attachment("eurowatch-speeches.csv");
      res.write("\ufeff" + selected.join(",") + "\r\n");
      for (
        let offset = 0;
        offset < count[0].n && !res.destroyed;
        offset += 250
      ) {
        const rows = await all(
          dataset.db,
          `SELECT ${selected.map((field) => `${fields[field]} AS ${field}`).join(",")} FROM individual_speeches i JOIN sittings s ON s.id=i.sitting_id LEFT JOIN meps m ON m.id=i.mep_id ${filter} ORDER BY s.activity_date DESC,s.id DESC,i.speech_order,i.id LIMIT 250 OFFSET ?`,
          [...where.values, offset],
        );
        if (res.destroyed) break;
        if (
          !res.write(
            rows
              .map((row) => selected.map((field) => csv(row[field])).join(","))
              .join("\r\n") + "\r\n",
          )
        ) {
          const controller = new AbortController();
          const disconnect = () => controller.abort();
          res.once("close", disconnect);
          try {
            await once(res, "drain", { signal: controller.signal });
          } catch (error) {
            if (error.name !== "AbortError") throw error;
          } finally {
            res.removeListener("close", disconnect);
          }
        }
      }
      res.end();
    } finally {
      exportsInFlight--;
    }
  });
  app.get("/api/full-dataset/status", (req, res) =>
    res.json({
      available: fullDownload,
      filename: "eurowatch-snapshot.db",
      sizeBytes: dataset.files["ep_data.db"].bytes,
      sha256: dataset.files["ep_data.db"].sha256,
      datasetId: dataset.release.id,
    }),
  );
  app.get("/api/download/full-dataset", (req, res) => {
    if (!fullDownload)
      throw new params.HttpError(
        404,
        "Full snapshot download is not enabled. Use a filtered CSV export.",
      );
    res.download(
      path.join(dataset.directory, "ep_data.db"),
      "eurowatch-snapshot.db",
    );
  });
  app.use("/api", (req, res) =>
    res.status(404).json({
      error:
        "Endpoint not found. See the Data & About page for supported browsing routes.",
    }),
  );
  for (const route of ["/speech.html", "/mep-details.html", "/newest.html"])
    app.get(route, (req, res) => {
      const query = new URLSearchParams({
        view: route === "/mep-details.html" ? "speakers" : "sittings",
      });
      if (typeof req.query.id === "string")
        query.set(
          route === "/mep-details.html" ? "speaker" : "sitting",
          req.query.id,
        );
      res.redirect(302, `/?${query}`);
    });
  app.use(
    express.static(path.resolve(__dirname, "../../public"), {
      maxAge: 0,
      etag: true,
    }),
  );
  app.use((req, res) =>
    res.status(404).type("text").send("Page not found. Return to /."),
  );
  app.use((error, req, res, next) => {
    if (res.headersSent) return next(error);
    const status = error.status || 500;
    if (status >= 500)
      logger.error(`[HTTP] ${req.method} ${req.path}: ${error.message}`);
    res.status(status).json({
      error:
        status >= 500 && status !== 503
          ? "The request could not be completed. Please retry."
          : error.message,
    });
  });
  return app;
}
module.exports = { createApp, csv };
