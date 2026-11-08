const express = require("express");
const { hash } = require("./model");
const { html, rss, newsletterText } = require("./publication");

function search(
  publication,
  { query = "", topic = "", limit = 20, offset = 0 } = {},
) {
  if (
    typeof query !== "string" ||
    query.length > 200 ||
    typeof topic !== "string" ||
    topic.length > 100 ||
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > 50 ||
    !Number.isInteger(offset) ||
    offset < 0
  )
    throw new Error("Invalid search parameters");
  const needle = query.toLocaleLowerCase();
  const matches = publication.entries.filter(
    (e) =>
      (!topic || e.topics.some((t) => t.label === topic)) &&
      (!needle ||
        `${e.title} ${e.claims.map((c) => c.text).join(" ")}`
          .toLocaleLowerCase()
          .includes(needle)),
  );
  return {
    publicationId: publication.publicationId,
    releaseId: publication.releaseId,
    total: matches.length,
    offset,
    limit,
    entries: matches.slice(offset, offset + limit),
  };
}

function createInsightsRouter({ getPublication, getDatasetId, publicOrigin }) {
  if (typeof getPublication !== "function")
    throw new Error("getPublication required");
  const allowedOrigin = new URL(publicOrigin).origin;
  const router = express.Router();
  async function snapshot(id) {
    if (id && !/^[a-f0-9]{64}$/.test(id)) return null;
    const publication = await getPublication(id);
    if (!publication) return null;
    const { publicationId, ...content } = publication;
    if (hash(content) !== publicationId || (id && id !== publicationId))
      throw new Error("Publication integrity mismatch");
    // Historical URLs retain their own release identity. Current views must be
    // synchronized to the active downloadable/API dataset by the integration.
    if (!id && getDatasetId && publication.releaseId !== (await getDatasetId()))
      return null;
    return publication;
  }
  const headers = (res, p) =>
    res.set({
      "X-Dataset-Id": p.releaseId,
      "X-Publication-Id": p.publicationId,
      "X-Content-Type-Options": "nosniff",
      "Cache-Control": "no-cache",
    });
  router.get("/api/insights", async (req, res) => {
    const p = await snapshot();
    if (!p) return res.sendStatus(503);
    try {
      headers(res, p).json(
        search(p, {
          query: req.query.q || "",
          topic: req.query.topic || "",
          limit: req.query.limit === undefined ? 20 : Number(req.query.limit),
          offset: req.query.offset === undefined ? 0 : Number(req.query.offset),
        }),
      );
    } catch {
      res.status(400).json({ error: "Invalid search parameters" });
    }
  });
  router.get(
    [
      "/briefings",
      "/briefings/:publicationId",
      "/briefings/:publicationId/:slug",
    ],
    async (req, res) => {
      const p = await snapshot(req.params.publicationId);
      if (!p) return res.sendStatus(req.params.publicationId ? 404 : 503);
      const entries = req.params.slug
        ? p.entries.filter((e) => e.slug === req.params.slug)
        : p.entries;
      if (!entries.length) return res.sendStatus(404);
      headers(res, p)
        .set(
          "Content-Security-Policy",
          "default-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        )
        .type("html")
        .send(html(p, entries));
    },
  );
  for (const [route, type, render] of [
    ["/feed.xml", "application/rss+xml", rss],
    ["/newsletter.txt", "text/plain", newsletterText],
  ])
    router.get(route, async (_req, res) => {
      const p = await snapshot();
      if (!p) return res.sendStatus(503);
      headers(res, p).type(type).send(render(p));
    });

  const protocols = ["2025-03-26", "2025-06-18", "2025-11-25"];
  router.use("/mcp", (req, res, next) => {
    if (req.get("origin") && req.get("origin") !== allowedOrigin)
      return res.sendStatus(403);
    const version = req.get("mcp-protocol-version");
    if (version && !protocols.includes(version)) return res.sendStatus(400);
    next();
  });
  router.get("/mcp", (_req, res) => res.set("Allow", "POST").sendStatus(405));
  router.post("/mcp", express.json({ limit: "16kb" }), async (req, res) => {
    if (!req.is("application/json")) return res.sendStatus(415);
    if (
      !req.get("accept")?.includes("application/json") ||
      !req.get("accept")?.includes("text/event-stream")
    )
      return res.sendStatus(406);
    const message = req.body;
    const error = (code, text) =>
      res.json({
        jsonrpc: "2.0",
        id: message?.id ?? null,
        error: { code, message: text },
      });
    if (
      !message ||
      Array.isArray(message) ||
      message.jsonrpc !== "2.0" ||
      typeof message.method !== "string" ||
      (message.id !== undefined &&
        !["string", "number"].includes(typeof message.id))
    )
      return error(-32600, "Invalid Request");
    if (message.id === undefined) return res.status(202).end();
    const reply = (result) =>
      res.json({ jsonrpc: "2.0", id: message.id, result });
    if (message.method === "initialize")
      return reply({
        protocolVersion: protocols.includes(message.params?.protocolVersion)
          ? message.params.protocolVersion
          : protocols.at(-1),
        capabilities: { tools: { listChanged: false } },
        serverInfo: { name: "eurowatch-reviewed-insights", version: "1.0.0" },
      });
    if (message.method === "ping") return reply({});
    if (message.method === "tools/list")
      return reply({
        tools: [
          {
            name: "search_briefings",
            description:
              "Search reviewed European Parliament briefings and return their source citations and dataset release identity.",
            inputSchema: {
              type: "object",
              properties: {
                query: { type: "string", maxLength: 200 },
                topic: { type: "string", maxLength: 100 },
                limit: { type: "integer", minimum: 1, maximum: 50 },
                offset: { type: "integer", minimum: 0 },
              },
              additionalProperties: false,
            },
            annotations: {
              readOnlyHint: true,
              destructiveHint: false,
              idempotentHint: true,
              openWorldHint: false,
            },
          },
          {
            name: "get_briefing",
            description:
              "Read one reviewed briefing by its shareable slug, optionally from an archived publication.",
            inputSchema: {
              type: "object",
              properties: {
                slug: { type: "string" },
                publicationId: { type: "string" },
              },
              required: ["slug"],
              additionalProperties: false,
            },
            annotations: {
              readOnlyHint: true,
              destructiveHint: false,
              idempotentHint: true,
              openWorldHint: false,
            },
          },
        ],
      });
    if (message.method !== "tools/call")
      return error(-32601, "Method not found");
    const args = message.params?.arguments || {};
    if (!args || typeof args !== "object" || Array.isArray(args))
      return error(-32602, "Invalid arguments");
    const name = message.params?.name;
    const allowed =
      name === "search_briefings"
        ? ["query", "topic", "limit", "offset"]
        : ["slug", "publicationId"];
    if (Object.keys(args).some((k) => !allowed.includes(k)))
      return error(-32602, "Unexpected argument");
    if (!["search_briefings", "get_briefing"].includes(name))
      return error(-32602, "Unknown read-only tool");
    if (
      name === "get_briefing" &&
      (typeof args.slug !== "string" ||
        (args.publicationId !== undefined &&
          typeof args.publicationId !== "string"))
    )
      return error(-32602, "Invalid briefing identity");
    const p = await snapshot(
      name === "get_briefing" ? args.publicationId : undefined,
    );
    if (!p)
      return reply({
        isError: true,
        content: [{ type: "text", text: "Reviewed publication unavailable" }],
      });
    try {
      const result =
        name === "search_briefings"
          ? search(p, args)
          : {
              publicationId: p.publicationId,
              releaseId: p.releaseId,
              entry: p.entries.find((e) => e.slug === args.slug) || null,
            };
      headers(res, p);
      return reply({
        content: [{ type: "text", text: JSON.stringify(result) }],
      });
    } catch {
      return error(-32602, "Invalid search parameters");
    }
  });
  return router;
}
module.exports = { createInsightsRouter, search };
