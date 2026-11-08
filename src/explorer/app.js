const express = require("express");
const path = require("node:path");
const { once } = require("node:events");
const { all } = require("../data/sqlite");
const params = require("./params");
const queries = require("./queries");
const { officialMemberships } = require("../data/mep-roster");
const { createInsightsRouter } = require("../insights/router");
const { readReviewedPublication } = require("../insights/release");
const {
  currentOfficialContext,
  contextHtml,
} = require("../insights/context-view");

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
  source,
  {
    fullDownload = true,
    trustProxy = false,
    logger = console,
    updateControl,
    insightsOrigin = "http://127.0.0.1",
    watchlistRouter,
  } = {},
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
  if (updateControl) app.use("/operator", limiter(), updateControl);
  app.use((req, res, next) => {
    const lease =
      typeof source.acquire === "function"
        ? source.acquire()
        : { dataset: source, release() {} };
    req.dataset = lease.dataset;
    let ended = false,
      work = 0,
      released = false;
    const release = () => {
      if (ended && work === 0 && !released) {
        released = true;
        lease.release();
      }
    };
    const end = () => {
      ended = true;
      release();
    };
    res.once("finish", end);
    res.once("close", end);
    req.beginDatasetWork = () => {
      work++;
    };
    req.endDatasetWork = () => {
      work--;
      release();
    };
    next();
  });
  // A disconnected response can still have an async query in progress. Keep
  // its lease until both the response and the route's asynchronous work finish.
  function get(route, handler) {
    app.get(route, async (req, res, next) => {
      req.beginDatasetWork();
      try {
        await handler(req, res, next);
      } catch (error) {
        next(error);
      } finally {
        req.endDatasetWork();
      }
    });
  }
  if (watchlistRouter) app.use("/pilot", limiter(), watchlistRouter);
  app.use(
    ["/briefings", "/feed.xml", "/newsletter.txt", "/api/insights", "/mcp"],
    limiter(),
  );
  app.use(
    createInsightsRouter({
      publicOrigin: insightsOrigin,
      async getPublication(id, req) {
        req.beginDatasetWork();
        try {
          return await readReviewedPublication(req.dataset.db, id);
        } finally {
          req.endDatasetWork();
        }
      },
    }),
  );
  get("/healthz", (req, res) => res.json({ status: "ok" }));
  get("/readyz", (req, res) =>
    res.json({
      ready: true,
      datasetId: req.dataset.release.id,
      analyticsAvailable: !!req.dataset.analytics,
    }),
  );
  app.use("/api", limiter(), (req, res, next) => {
    res.set("X-Dataset-ID", req.dataset.release.id);
    res.set("Cache-Control", "no-store");
    if (!["GET", "HEAD"].includes(req.method))
      return res.status(405).set("Allow", "GET, HEAD").json({
        error: "This explorer is read-only. Use offline maintenance commands.",
      });
    next();
  });
  get("/api/data-status", (req, res) =>
    res.json({
      ...req.dataset.release,
      coverage: {
        latestStoredSitting: req.dataset.release.stats.toDate,
        lastSuccessfulRefresh: req.dataset.release.refresh?.completedAt || null,
        pendingDates: req.dataset.release.refresh?.pendingDates ?? null,
        failedDates: req.dataset.release.refresh?.failedDates ?? null,
      },
      analyticsAvailable: !!req.dataset.analytics,
      analyticsError: req.dataset.analytics
        ? null
        : "Matching analytics is unavailable. Prepare the snapshot again.",
    }),
  );
  get("/api/official-context", async (req, res) =>
    res.json({
      datasetId: req.dataset.release.id,
      context: await currentOfficialContext(req.dataset.db),
    }),
  );
  get("/parliament", async (req, res) =>
    res
      .type("html")
      .send(contextHtml(await currentOfficialContext(req.dataset.db))),
  );
  get("/api/localrun", (req, res) => res.json({ localrun: false }));
  get("/api/sittings", async (req, res) =>
    res.json(
      await queries.sittings(
        req.dataset,
        params.range(req.query),
        params.pagination(req.query),
      ),
    ),
  );
  get("/api/sittings/:id", async (req, res) =>
    res.json(
      await queries.sitting(req.dataset, params.sittingId(req.params.id)),
    ),
  );
  get("/api/sittings/:id/speeches", async (req, res) => {
    const id = params.sittingId(req.params.id);
    await queries.sitting(req.dataset, id);
    res.json(
      await queries.speeches(
        req.dataset,
        { sitting: id },
        params.pagination(req.query),
      ),
    );
  });
  get("/api/speakers", async (req, res) => {
    const source = params.text(req.query.source, "source");
    if (source && !["api", "historic", "unresolved"].includes(source))
      throw new params.HttpError(400, "Unknown speaker record source.");
    res.json(
      await queries.speakers(
        req.dataset,
        {
          q: params.text(req.query.q),
          source,
          country: params.text(req.query.country, "country", 20),
        },
        params.pagination(req.query),
      ),
    );
  });
  get("/api/speakers/:id", async (req, res) =>
    res.json(
      await queries.speaker(req.dataset, params.speakerId(req.params.id)),
    ),
  );
  get("/api/speakers/:id/affiliations", async (req, res) => {
    const id = params.speakerId(req.params.id);
    await queries.speaker(req.dataset, id);
    const official = await officialMemberships(req.dataset.db, id);
    res.json({ official });
  });
  get("/api/speakers/:id/speeches", async (req, res) => {
    const id = params.speakerId(req.params.id);
    await queries.speaker(req.dataset, id);
    res.json(
      await queries.speeches(
        req.dataset,
        { speaker: id, ...params.range(req.query) },
        params.pagination(req.query),
      ),
    );
  });
  get("/api/speeches", async (req, res) =>
    res.json(
      await queries.sittings(
        req.dataset,
        params.range(req.query),
        params.pagination(req.query),
        req.query.personId ? params.speakerId(req.query.personId) : null,
      ),
    ),
  );
  get("/api/topics", async (req, res) => {
    if (!req.dataset.analytics)
      throw new params.HttpError(
        503,
        "Matching topic analytics is unavailable.",
      );
    res.json({
      data: await all(
        req.dataset.analytics,
        "SELECT topic,count FROM topics ORDER BY count DESC,topic",
      ),
    });
  });
  const cache = new Map();
  async function topicSeries(req, res) {
    const filters = params.seriesParams(req.query),
      key = JSON.stringify([req.dataset.release.id, filters]),
      cached = cache.get(key);
    let result;
    if (cached && cached.expires > Date.now()) {
      result = cached.value;
      res.set("X-Cache", "HIT");
    } else {
      result = await queries.series(req.dataset, filters);
      if (cache.size >= 100) cache.delete(cache.keys().next().value);
      cache.set(key, { value: result, expires: Date.now() + 180000 });
      res.set("X-Cache", "MISS");
    }
    return result;
  }
  get("/api/analytics/time-series", async (req, res) =>
    res.json(await topicSeries(req, res)),
  );
  get("/api/export/topics", async (req, res) => {
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
  get("/api/export/speeches", async (req, res) => {
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
      req.dataset.db,
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
          req.dataset.db,
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
  get("/api/full-dataset/status", (req, res) =>
    res.json({
      available: fullDownload,
      filename: "eurowatch-snapshot.db",
      sizeBytes: req.dataset.files["ep_data.db"].bytes,
      sha256: req.dataset.files["ep_data.db"].sha256,
      datasetId: req.dataset.release.id,
    }),
  );
  get("/api/download/full-dataset", (req, res) => {
    if (!fullDownload)
      throw new params.HttpError(
        404,
        "Full snapshot download is not enabled. Use a filtered CSV export.",
      );
    res.download(
      path.join(req.dataset.directory, "ep_data.db"),
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
    get(route, (req, res) => {
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
