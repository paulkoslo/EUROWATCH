const fs = require("node:fs/promises");
const { createHash, timingSafeEqual } = require("node:crypto");
const { createWatchlistStore, createWatchlistRouter } = require("./watchlists");
const { readReviewedPublication } = require("./release");

async function openPilotWatchlists(source, publicOrigin, env = process.env) {
  if (!env.WATCHLIST_PILOT_TOKEN_FILE && !env.WATCHLIST_PILOT_DB)
    return undefined;
  if (!env.WATCHLIST_PILOT_TOKEN_FILE || !env.WATCHLIST_PILOT_DB)
    throw new Error(
      "Watchlist pilot requires a token file and persistent database.",
    );
  const records = JSON.parse(
    await fs.readFile(env.WATCHLIST_PILOT_TOKEN_FILE, "utf8"),
  );
  if (
    !Array.isArray(records) ||
    !records.length ||
    records.some(
      (record) =>
        typeof record.id !== "string" || !/^[a-f0-9]{64}$/.test(record.sha256),
    ) ||
    new Set(records.map((record) => record.sha256)).size !== records.length
  )
    throw new Error("Invalid watchlist pilot token configuration.");
  const store = await createWatchlistStore(env.WATCHLIST_PILOT_DB);
  const router = createWatchlistRouter({
    store,
    publicOrigin,
    authorize(req) {
      const header = req.get("authorization") || "";
      if (!header.startsWith("Bearer ")) return null;
      const hash = createHash("sha256").update(header.slice(7)).digest();
      const record = records.find((entry) =>
        timingSafeEqual(Buffer.from(entry.sha256, "hex"), hash),
      );
      return record ? { id: record.id, watchlistPilot: true } : null;
    },
    async getPublication() {
      const lease =
        typeof source.acquire === "function"
          ? source.acquire()
          : { dataset: source, release() {} };
      try {
        return await readReviewedPublication(lease.dataset.db);
      } finally {
        lease.release();
      }
    },
  });
  return { router, close: store.close };
}

module.exports = { openPilotWatchlists };
