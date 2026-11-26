// WordPress sites and update jobs (the activity log). Agent keys are stored encrypted.
let db;
let cipher; // { encrypt(text) -> base64, decrypt(base64) -> text }
const q = (sql) => db.prepare(sql);
const n = (v) => (v === undefined ? null : v);
const SITE_STATE = ['status', 'error', 'info', 'last_check', 'last_update'];
const JOB_FIELDS = ['status', 'message', 'steps', 'to_version', 'started_at', 'finished_at'];

function init(handle, c) {
  db = handle;
  cipher = c;
  db.exec(`
    CREATE TABLE IF NOT EXISTS wp_sites (
      id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL, key_enc TEXT NOT NULL,
      auto_plugins INTEGER NOT NULL DEFAULT 1, auto_themes INTEGER NOT NULL DEFAULT 0,
      excluded TEXT NOT NULL DEFAULT '[]', status TEXT NOT NULL DEFAULT 'unknown', error TEXT,
      info TEXT, last_check INTEGER, last_update INTEGER, created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS wp_jobs (
      id TEXT PRIMARY KEY, site_id INTEGER REFERENCES wp_sites(id) ON DELETE CASCADE,
      kind TEXT NOT NULL, item TEXT, name TEXT, from_version TEXT, to_version TEXT,
      status TEXT NOT NULL, message TEXT, steps TEXT NOT NULL DEFAULT '[]', source TEXT NOT NULL DEFAULT 'manual',
      batch TEXT, created_at INTEGER NOT NULL, started_at INTEGER, finished_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_wp_jobs_site ON wp_jobs(site_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_wp_jobs_status ON wp_jobs(status);
    CREATE INDEX IF NOT EXISTS idx_wp_jobs_batch ON wp_jobs(batch);
  `);
}

// Public shape: no key, parsed JSON
function shape(row) {
  if (!row) return null;
  const { key_enc, ...s } = row;
  return { ...s, info: s.info ? JSON.parse(s.info) : null, excluded: JSON.parse(s.excluded), has_key: !!key_enc };
}

const sites = {
  list: () => q('SELECT * FROM wp_sites ORDER BY name COLLATE NOCASE').all().map(shape),
  get: (id) => shape(q('SELECT * FROM wp_sites WHERE id = ?').get(id)),
  // Connection details for the client only (never sent to the dashboard)
  secret(id) {
    const row = q('SELECT url, key_enc, info FROM wp_sites WHERE id = ?').get(id);
    if (!row) return null;
    const info = row.info ? JSON.parse(row.info) : {};
    return { url: row.url, key: cipher.decrypt(row.key_enc), rescue: info.rescue || null };
  },
  add(s) {
    const r = q('INSERT INTO wp_sites (name, url, key_enc, created_at) VALUES (?, ?, ?, ?)').run(s.name, s.url, cipher.encrypt(s.key), Date.now());
    return sites.get(Number(r.lastInsertRowid));
  },
  update(id, s) {
    q('UPDATE wp_sites SET name = ?, url = ? WHERE id = ?').run(s.name, s.url, id);
    if (s.key) q('UPDATE wp_sites SET key_enc = ? WHERE id = ?').run(cipher.encrypt(s.key), id);
    return sites.get(id);
  },
  remove: (id) => q('DELETE FROM wp_sites WHERE id = ?').run(id),
  setState(id, fields) {
    const keys = Object.keys(fields).filter((k) => SITE_STATE.includes(k));
    if (!keys.length) return;
    const val = (k) => (k === 'info' && fields[k] ? JSON.stringify(fields[k]) : n(fields[k]));
    q(`UPDATE wp_sites SET ${keys.map((k) => `${k} = ?`).join(', ')} WHERE id = ?`).run(...keys.map(val), id);
  },
  setAuto: (id, plugins, themes) => q('UPDATE wp_sites SET auto_plugins = ?, auto_themes = ? WHERE id = ?').run(plugins ? 1 : 0, themes ? 1 : 0, id),
  setExcluded: (id, list) => q('UPDATE wp_sites SET excluded = ? WHERE id = ?').run(JSON.stringify(list), id),
};

const jobs = {
  create(j) {
    q(`INSERT INTO wp_jobs (id, site_id, kind, item, name, from_version, to_version, status, message, source, batch, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(j.id, n(j.site_id), j.kind, n(j.item), n(j.name), n(j.from_version), n(j.to_version), j.status, n(j.message), j.source || 'manual', n(j.batch), Date.now());
    return jobs.get(j.id);
  },
  get: (id) => q('SELECT * FROM wp_jobs WHERE id = ?').get(id),
  update(id, fields) {
    const keys = Object.keys(fields).filter((k) => JOB_FIELDS.includes(k));
    const val = (k) => (k === 'steps' ? JSON.stringify(fields[k] || []) : n(fields[k]));
    q(`UPDATE wp_jobs SET ${keys.map((k) => `${k} = ?`).join(', ')} WHERE id = ?`).run(...keys.map(val), id);
  },
  byStatus: (status) => q('SELECT * FROM wp_jobs WHERE status = ? ORDER BY created_at, rowid').all(status),
  active: (siteId, kind, item) => q("SELECT id FROM wp_jobs WHERE site_id = ? AND kind = ? AND item = ? AND status IN ('pending', 'running')").get(siteId, kind, item),
  openInBatch: (batch) => q("SELECT COUNT(*) AS c FROM wp_jobs WHERE batch = ? AND status IN ('pending', 'running')").get(batch).c,
  batch: (batch) => q("SELECT * FROM wp_jobs WHERE batch = ? AND kind IN ('plugin', 'theme')").all(batch),
  // Cancels queued jobs only; a job that already started is left to finish
  cancel(id) {
    return q("UPDATE wp_jobs SET status = 'cancelled', message = 'Cancelled before it started', finished_at = ? WHERE id = ? AND status = 'pending'").run(Date.now(), id).changes;
  },
  cancelPending: (siteId) => (siteId
    ? q("UPDATE wp_jobs SET status = 'cancelled', message = 'Cancelled before it started', finished_at = ? WHERE site_id = ? AND status = 'pending'").run(Date.now(), siteId)
    : q("UPDATE wp_jobs SET status = 'cancelled', message = 'Cancelled before it started', finished_at = ? WHERE status = 'pending'").run(Date.now())).changes,
  pendingBySite: () => Object.fromEntries(q("SELECT site_id, COUNT(*) AS c FROM wp_jobs WHERE status = 'pending' GROUP BY site_id").all().map((r) => [r.site_id, r.c])),
  openBatches: () => q("SELECT DISTINCT batch FROM wp_jobs WHERE batch IS NOT NULL AND status IN ('pending', 'running')").all().map((r) => r.batch),
  busySchedule: () => q("SELECT COUNT(*) AS c FROM wp_jobs WHERE source = 'schedule' AND status IN ('pending', 'running')").get().c,
  list({ siteId, status, limit = 300 } = {}) {
    const where = [];
    const args = [];
    if (siteId) { where.push('j.site_id = ?'); args.push(siteId); }
    if (status) { where.push('j.status = ?'); args.push(status); }
    return q(`SELECT j.*, s.name AS site_name FROM wp_jobs j LEFT JOIN wp_sites s ON s.id = j.site_id
              ${where.length ? 'WHERE ' + where.join(' AND ') : ''} ORDER BY j.created_at DESC, j.rowid DESC LIMIT ?`)
      .all(...args, limit).map((j) => ({ ...j, steps: JSON.parse(j.steps) }));
  },
  // Latest finished job per item, for the "last update" column
  latest: (siteId) => q(`SELECT id, kind, item, status, finished_at FROM wp_jobs WHERE site_id = ? AND kind IN ('plugin', 'theme')
                          AND rowid IN (SELECT MAX(rowid) FROM wp_jobs WHERE site_id = ? GROUP BY kind, item)`).all(siteId, siteId),
  // Removes finished entries (running and queued jobs are kept)
  clear: (siteId) => (siteId
    ? q("DELETE FROM wp_jobs WHERE site_id = ? AND status NOT IN ('pending', 'running')").run(siteId)
    : q("DELETE FROM wp_jobs WHERE status NOT IN ('pending', 'running')").run()).changes,
  prune: (days) => q("DELETE FROM wp_jobs WHERE created_at < ? AND status NOT IN ('pending', 'running')").run(Date.now() - days * 864e5),
};

module.exports = { init, sites, jobs };