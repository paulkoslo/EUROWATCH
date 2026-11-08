const express = require("express");
const path = require("node:path");
const { createHash, randomUUID, timingSafeEqual } = require("node:crypto");

const runPattern = /^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/i;
const digest = (value) => createHash("sha256").update(value).digest();
const uuid = (value) => typeof value === "string" && runPattern.test(value);

function renderLauncher({ serviceId, apiKey, request = fetch }) {
  if (!/^crn-[a-z0-9]+$/.test(serviceId) || !apiKey)
    throw new Error(
      "Configure the Render cron service ID and API key for the update control.",
    );
  const endpoint = `https://api.render.com/v1/services/${serviceId}/jobs`;
  return async ({ runId, retryRunId }) => {
    if (!uuid(runId) || (retryRunId && !uuid(retryRunId)))
      throw new Error("Invalid run identifier.");
    const startCommand =
      `npm run data:update:hosted -- --trigger ${retryRunId ? "retry" : "manual"} --run-id ${runId}` +
      (retryRunId ? ` --retry ${retryRunId}` : "");
    const result = await request(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ startCommand }),
      signal: AbortSignal.timeout(15000),
    });
    if (!result.ok)
      throw new Error(`Render job launch returned HTTP ${result.status}.`);
    const job = await result.json();
    return job.id;
  };
}

function createUpdateControl({ storage, launch, password, logger = console }) {
  if (!password || password.length < 24)
    throw new Error("The operator password must be at least 24 characters.");
  const credential = digest(
    `Basic ${Buffer.from(`operator:${password}`).toString("base64")}`,
  );
  const router = express.Router();
  const held = (lease) =>
    lease && Date.parse(lease.value.expiresAt) > Date.now();
  function currentReport(report, lease) {
    if (!report) return null;
    if (
      ["queued", "running"].includes(report.status) &&
      (!held(lease) || lease.value.runId !== report.runId) &&
      Date.now() - Date.parse(report.startedAt) > 120_000
    )
      return { ...report, status: "interrupted" };
    return report;
  }
  router.use((req, res, next) => {
    res.set("Cache-Control", "no-store");
    res.set("X-Robots-Tag", "noindex");
    if (!timingSafeEqual(credential, digest(req.get("Authorization") || "")))
      return res
        .set("WWW-Authenticate", 'Basic realm="EUROWATCH updates"')
        .status(401)
        .send("Operator authentication required.");
    next();
  });
  router.get("/", (req, res) =>
    res.sendFile(path.join(__dirname, "../../private/update-control.html")),
  );
  router.get("/control.js", (req, res) =>
    res.sendFile(path.join(__dirname, "../../private/update-control.js")),
  );
  router.get("/runs", async (req, res) => {
    const { items } = await storage.listRunReports({ limit: 12 });
    const reports = await Promise.all(
      items.map(async ({ runId }) => {
        const [report, launch] = await Promise.all([
          storage.readRunReport(runId),
          storage.readLaunch(runId),
        ]);
        return report && { ...report.value, jobId: launch?.value.jobId };
      }),
    );
    const lease = await storage.readLease();
    res.json({
      running: Boolean(held(lease)),
      runs: reports.filter(Boolean).map((value) => currentReport(value, lease)),
    });
  });
  router.get("/runs/:runId", async (req, res) => {
    if (!uuid(req.params.runId))
      return res.status(400).json({ error: "Invalid run ID." });
    const report = await storage.readRunReport(req.params.runId);
    if (!report)
      return res.status(404).json({ error: "The run has not reported yet." });
    res.json(currentReport(report.value, await storage.readLease()));
  });
  router.post(
    "/runs",
    (req, res, next) => {
      // Cross-origin forms cannot set application/json. A browser preflight
      // is denied, and requests from other origins are rejected explicitly.
      const site = req.get("Sec-Fetch-Site");
      if (!req.is("application/json") || (site && site !== "same-origin"))
        return res
          .status(403)
          .json({ error: "Start updates from the private dashboard." });
      next();
    },
    express.json({ limit: "1kb" }),
    async (req, res) => {
      const retryRunId = req.body?.retryRunId;
      if (retryRunId !== undefined && !uuid(retryRunId))
        return res.status(400).json({ error: "Invalid retry run ID." });
      const lease = await storage.readLease();
      if (held(lease))
        return res.status(409).json({ error: "An update is already running." });
      if (retryRunId) {
        const report = currentReport(
          (await storage.readRunReport(retryRunId))?.value,
          lease,
        );
        if (
          !report ||
          (!(
            ["failed", "interrupted"].includes(report.status) &&
            report.start &&
            report.end
          ) &&
            ![...report.failedDates, ...report.pendingDates].length)
        )
          return res
            .status(404)
            .json({ error: "No sitting dates are available to retry." });
      }
      const runId = randomUUID();
      let reservation;
      try {
        reservation = await storage.writeLease(
          {
            runId,
            phase: "queued",
            expiresAt: new Date(Date.now() + 120_000).toISOString(),
          },
          { expectedETag: lease?.etag ?? null },
        );
      } catch (error) {
        if (error.code === "RELEASE_CONFLICT")
          return res
            .status(409)
            .json({ error: "An update is already queued." });
        throw error;
      }
      try {
        await storage.writeRunReport(
          runId,
          {
            runId,
            retryRunId,
            trigger: retryRunId ? "retry" : "manual",
            status: "queued",
            startedAt: new Date().toISOString(),
            events: [],
            failedDates: [],
            pendingDates: [],
          },
          { expectedETag: null },
        );
        const jobId = await launch({ runId, retryRunId });
        await storage.writeLaunch(runId, jobId);
        res.status(202).json({ runId, jobId });
      } catch (error) {
        // A transport timeout can occur after Render accepted the launch. Keep
        // its reservation until the worker claims it or the queued run expires.
        logger.error(`[data-update] ${error.message}`);
        if (!reservation) throw error;
        res.status(502).json({
          error:
            "Render did not confirm the launch. Check the queued run or Render before retrying.",
        });
      }
    },
  );
  return router;
}

function createUpdateControlFromEnv(storage, env = process.env) {
  if (!env.DATA_UPDATE_OPERATOR_PASSWORD) return undefined;
  if (!storage)
    throw new Error("The update control requires shared release storage.");
  return createUpdateControl({
    storage,
    password: env.DATA_UPDATE_OPERATOR_PASSWORD,
    launch: renderLauncher({
      serviceId: env.DATA_UPDATE_RENDER_CRON_ID,
      apiKey: env.DATA_UPDATE_RENDER_API_KEY,
    }),
  });
}

module.exports = {
  createUpdateControl,
  createUpdateControlFromEnv,
  renderLauncher,
};
