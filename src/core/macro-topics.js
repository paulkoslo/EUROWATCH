/**
 * Storage for macro topics. The agent can add new topics when none fit.
 * Uses file locking for safe concurrent access (multiple pipeline runs, workers).
 * Path overridable via MACRO_TOPICS_FILE env var.
 */

const path = require('path');
const fs = require('fs');
const lockfile = require('proper-lockfile');

const DATA_DIR = path.join(__dirname, '..', '..', 'data');
const DEFAULT_FILE = path.join(DATA_DIR, 'macro-topics.json');

function getFilePath() {
  const env = process.env.MACRO_TOPICS_FILE;
  return env ? path.resolve(env) : DEFAULT_FILE;
}

function ensureDataDir(filePath) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function load() {
  const filePath = getFilePath();
  ensureDataDir(filePath);
  if (!fs.existsSync(filePath)) {
    return [];
  }
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function save(topics) {
  const filePath = getFilePath();
  ensureDataDir(filePath);
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(topics, null, 2), 'utf8');
  fs.renameSync(tmpPath, filePath);
}

const LOCK_OPTS = { stale: 30000, retries: { retries: 10, factor: 1.5, minTimeout: 50, maxTimeout: 500 } };

/**
 * Add a new macro topic if it doesn't exist. Returns the list (updated or unchanged).
 */
async function addIfNew(name) {
  const trimmed = (name || '').trim();
  if (!trimmed) return load();

  const filePath = getFilePath();
  ensureDataDir(filePath);
  if (!fs.existsSync(filePath)) fs.writeFileSync(filePath, '[]', 'utf8');

  let release;
  try {
    release = await lockfile.lock(filePath, LOCK_OPTS);
    const topics = load();
    const exists = topics.some(t => t.toLowerCase() === trimmed.toLowerCase());
    if (!exists) {
      topics.push(trimmed);
      save(topics);
    }
    return topics;
  } finally {
    if (release) await release();
  }
}

/**
 * Add multiple new macro topics. Returns count added.
 * Safe for concurrent calls (file locking).
 */
async function addAllIfNew(names) {
  if (!names || names.length === 0) return 0;

  const filePath = getFilePath();
  ensureDataDir(filePath);
  if (!fs.existsSync(filePath)) fs.writeFileSync(filePath, '[]', 'utf8');

  let release;
  try {
    release = await lockfile.lock(filePath, LOCK_OPTS);
    let added = 0;
    const topics = load();
    const lower = new Set(topics.map(t => t.toLowerCase()));

    for (const name of names) {
      const trimmed = (name || '').trim();
      if (!trimmed) continue;
      if (!lower.has(trimmed.toLowerCase())) {
        topics.push(trimmed);
        lower.add(trimmed.toLowerCase());
        added++;
      }
    }

    if (added > 0) save(topics);
    return added;
  } finally {
    if (release) await release();
  }
}

module.exports = {
  FILE_PATH: DEFAULT_FILE,
  getFilePath,
  DATA_DIR,
  load,
  save,
  addIfNew,
  addAllIfNew
};
