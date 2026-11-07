const sqlite3 = require("sqlite3");

function open(filename, { readonly = false } = {}) {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(
      filename,
      readonly
        ? sqlite3.OPEN_READONLY
        : sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
      (error) => (error ? reject(error) : resolve(db)),
    );
    db.configure("busyTimeout", 5000);
  });
}

const all = (db, sql, params = []) =>
  new Promise((resolve, reject) =>
    db.all(sql, params, (error, rows) =>
      error ? reject(error) : resolve(rows),
    ),
  );
const get = (db, sql, params = []) =>
  new Promise((resolve, reject) =>
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row))),
  );
const exec = (db, sql) =>
  new Promise((resolve, reject) =>
    db.exec(sql, (error) => (error ? reject(error) : resolve())),
  );
const run = (db, sql, params = []) =>
  new Promise((resolve, reject) =>
    db.run(sql, params, function (error) {
      return error
        ? reject(error)
        : resolve({ changes: this.changes, lastID: this.lastID });
    }),
  );
const close = (db) =>
  new Promise((resolve, reject) =>
    db.close((error) => (error ? reject(error) : resolve())),
  );

async function backup(db, filename) {
  await new Promise((resolve, reject) => {
    const copy = db.backup(filename);
    copy.step(-1, (error) => {
      copy.finish((finishError) =>
        error || finishError ? reject(error || finishError) : resolve(),
      );
    });
  });
}

module.exports = { open, all, get, exec, run, close, backup };
