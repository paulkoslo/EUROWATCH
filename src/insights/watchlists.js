const express = require("express");
const fs = require("node:fs");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const { open, all, get, exec, run, close } = require("../data/sqlite");

function validateWatchlist(value) {
  if (
    !value ||
    typeof value.name !== "string" ||
    !value.name.trim() ||
    value.name.length > 100 ||
    !Array.isArray(value.topics) ||
    !value.topics.length ||
    value.topics.length > 20 ||
    !value.topics.every(
      (t) => typeof t === "string" && t.trim() && t.length <= 100,
    )
  )
    throw new Error("Name and 1–20 topic labels required");
  return {
    name: value.name.trim(),
    topics: [...new Set(value.topics.map((t) => t.trim()))],
  };
}
async function createWatchlistStore(filename) {
  fs.mkdirSync(path.dirname(path.resolve(filename)), {
    recursive: true,
    mode: 0o700,
  });
  const db = await open(filename);
  try {
    fs.chmodSync(filename, 0o600);
    await exec(
      db,
      `PRAGMA foreign_keys=ON;
      CREATE TABLE IF NOT EXISTS insight_watchlists(id TEXT PRIMARY KEY,owner TEXT NOT NULL,name TEXT NOT NULL,topics_json TEXT NOT NULL,created_at TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS insight_watchlists_owner ON insight_watchlists(owner);
      CREATE TABLE IF NOT EXISTS insight_watchlist_previews(watchlist_id TEXT NOT NULL REFERENCES insight_watchlists(id) ON DELETE CASCADE,publication_id TEXT NOT NULL,prepared_at TEXT NOT NULL,status TEXT NOT NULL CHECK(status='prepared'),PRIMARY KEY(watchlist_id,publication_id));`,
    );
  } catch (error) {
    await close(db);
    throw error;
  }
  const decode = (row) =>
    row
      ? {
          id: row.id,
          name: row.name,
          topics: JSON.parse(row.topics_json),
          createdAt: row.created_at,
        }
      : null;
  return {
    async list(owner) {
      return (
        await all(
          db,
          "SELECT * FROM insight_watchlists WHERE owner=? ORDER BY created_at,id",
          [owner],
        )
      ).map(decode);
    },
    async create(owner, value) {
      const data = validateWatchlist(value),
        id = randomUUID(),
        createdAt = new Date().toISOString();
      const result = await run(
        db,
        "INSERT INTO insight_watchlists SELECT ?,?,?,?,? WHERE (SELECT COUNT(*) FROM insight_watchlists WHERE owner=?)<50",
        [id, owner, data.name, JSON.stringify(data.topics), createdAt, owner],
      );
      if (!result.changes) throw new Error("Watchlist limit reached");
      return { id, ...data, createdAt };
    },
    async remove(owner, id) {
      return (
        (
          await run(
            db,
            "DELETE FROM insight_watchlists WHERE owner=? AND id=?",
            [owner, id],
          )
        ).changes > 0
      );
    },
    async prepare(owner, id, publication) {
      const watchlist = decode(
        await get(
          db,
          "SELECT * FROM insight_watchlists WHERE owner=? AND id=?",
          [owner, id],
        ),
      );
      if (!watchlist) return null;
      const entries = publication.entries.filter((e) =>
        e.topics.some((t) => watchlist.topics.includes(t.label)),
      );
      const claimed = await run(
        db,
        "INSERT OR IGNORE INTO insight_watchlist_previews SELECT id,?,?,'prepared' FROM insight_watchlists WHERE owner=? AND id=?",
        [publication.publicationId, new Date().toISOString(), owner, id],
      );
      return {
        watchlist,
        publicationId: publication.publicationId,
        releaseId: publication.releaseId,
        status: "prepared",
        previouslyPrepared: !claimed.changes,
        entries,
      };
    },
    close: () => close(db),
  };
}

function createWatchlistRouter({
  authorize,
  store,
  getPublication,
  publicOrigin,
}) {
  if (
    typeof authorize !== "function" ||
    typeof getPublication !== "function" ||
    !store
  )
    throw new Error(
      "Explicit authorization, store and publication provider required",
    );
  const origin = new URL(publicOrigin).origin;
  const router = express.Router();
  router.use(async (req, res, next) => {
    res.set("Cache-Control", "no-store");
    const actor = await authorize(req);
    if (
      !actor ||
      typeof actor.id !== "string" ||
      !actor.id ||
      actor.id.length > 200
    )
      return res.sendStatus(401);
    if (actor.watchlistPilot !== true) return res.sendStatus(403);
    if (!["GET", "HEAD"].includes(req.method) && req.get("origin") !== origin)
      return res.sendStatus(403);
    req.insightsOwner = actor.id;
    next();
  });
  router.use(express.json({ limit: "8kb" }));
  router.get("/watchlists", async (req, res) =>
    res.json({ watchlists: await store.list(req.insightsOwner) }),
  );
  router.post("/watchlists", async (req, res) => {
    try {
      validateWatchlist(req.body);
    } catch {
      return res
        .status(400)
        .json({ error: "Name and 1–20 topic labels required" });
    }
    if ((await store.list(req.insightsOwner)).length >= 50)
      return res.sendStatus(409);
    res.status(201).json(await store.create(req.insightsOwner, req.body));
  });
  router.delete("/watchlists/:id", async (req, res) =>
    res.sendStatus(
      (await store.remove(req.insightsOwner, req.params.id)) ? 204 : 404,
    ),
  );
  router.post("/watchlists/:id/preview", async (req, res) => {
    const publication = await getPublication();
    if (!publication) return res.sendStatus(503);
    const result = await store.prepare(
      req.insightsOwner,
      req.params.id,
      publication,
    );
    if (!result) return res.sendStatus(404);
    res.json(result);
  });
  return router;
}
module.exports = {
  createWatchlistStore,
  createWatchlistRouter,
  validateWatchlist,
};
