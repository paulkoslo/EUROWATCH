# Render Demo Deployment

This guide explains how to deploy EUROWATCH on Render using the **demo dataset** (sittings from 2020 onwards) for a lighter, faster deployment.

### Code changes that enable demo mode

The app reads `DB_PATH` and `ANALYTICS_DB_PATH` from the environment when set:

- **`src/core/db.js`** — `DB_PATH = process.env.DB_PATH || path.join(...data/ep_data.db)`
- **`src/core/analytics-db.js`** — `ANALYTICS_DB_PATH = process.env.ANALYTICS_DB_PATH || path.join(...data/analytics.db)`

No further code edits are required; set the env vars on Render to use the demo database.

---

## 1. Build demo data locally

From your project root:

```bash
# Ep data only (sittings 2020+)
npm run demo-data

# Ep data + precomputed analytics (recommended)
npm run demo-data:full
```

This creates `data/demo-data/` with:
- `ep_data.db` — sittings and speeches from 2020 onwards
- `analytics.db` — precomputed analytics (only with `demo-data:full`)

---

## 2. Render disk setup

### Mount path

Use a persistent disk with:

- **Mount Path:** `/opt/render/project/src/data`
- **Size:** 10 GB is usually enough for demo data (~1–2 GB)

### Upload demo data to the disk

After your first deploy (or when the disk is available), copy the demo data to the disk.

**Option A — SCP (from your machine):**

```bash
# Create remote dir and upload (use your Render SSH host, e.g. ssh.frankfurt.render.com)
ssh render@ssh.frankfurt.render.com "mkdir -p /opt/render/project/src/data/demo-data"
scp data/demo-data/ep_data.db data/demo-data/analytics.db render@ssh.frankfurt.render.com:/opt/render/project/src/data/demo-data/
# (Ignore ep_data.db-wal and ep_data.db-shm — they are recreated automatically)
```

**Option B — Render Shell:**

1. Open your service → Shell
2. Create dir and upload files, or paste from local machine

Add your Render SSH public key in the Render dashboard first (Settings → SSH Keys).

---

## 3. Environment variables (Render dashboard)

In **Environment** → Add Environment Variable, add:

```
DB_PATH=/opt/render/project/src/data/demo-data/ep_data.db
ANALYTICS_DB_PATH=/opt/render/project/src/data/demo-data/analytics.db
```

| Variable | Value |
|----------|-------|
| `DB_PATH` | `/opt/render/project/src/data/demo-data/ep_data.db` |
| `ANALYTICS_DB_PATH` | `/opt/render/project/src/data/demo-data/analytics.db` |

Use your actual disk mount path if different from `/opt/render/project/src/data`.

---

## 4. Build and start commands

- **Build Command:** `npm install` (or leave default)
- **Start Command:** `npm start`

If you prefer to build demo data on Render (and you already have `ep_data.db` on the disk):

```bash
# Build
npm install
npm run demo-data:full

# Start
npm start
```

Note: `demo-data:full` needs `data/ep_data.db` to exist. So either upload the full DB first, or upload the pre-built `data/demo-data/` folder and skip the build step.

---

## 5. Summary

| Step | Action |
|------|--------|
| 1 | Run `npm run demo-data:full` locally |
| 2 | Add persistent disk at `/opt/render/project/src/data` |
| 3 | Upload `data/demo-data/*` to the disk (SCP or Render Shell) |
| 4 | Set `DB_PATH` and `ANALYTICS_DB_PATH` in Render env |
| 5 | Deploy |

---

## 6. Alternative: use full data on Render

If you want the full dataset instead of demo:

- Do **not** set `DB_PATH` or `ANALYTICS_DB_PATH` (defaults will be used).
- Upload the full `data/` folder (ep_data.db + analytics.db) to the disk at `/opt/render/project/src/data/`.
- Ensure your disk and instance size (Starter/Standard) can handle the full DB.
