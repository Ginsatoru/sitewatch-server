// SiteWatch team server storage: SQLite via Node's built-in node:sqlite (no dependencies)
const { DatabaseSync } = require('node:sqlite');

// Team settings shared by everyone (personal settings stay in each desktop app)
const DEFAULTS = { interval: 60, timeout: 10, threshold: 3, retention: 30, remindAfter: 10, pausedAll: false };
const MONITOR_STATE = ['status', 'fails', 'fail_since', 'last_code', 'last_ms', 'last_error', 'last_checked', 'active', 'status_since'];

let db;
const q = (sql) => db.prepare(sql);
const n = (v) => (v === undefined ? null : v);

function init(file) {
  db = new DatabaseSync(file);
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS monitors (
      id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL,
      method TEXT NOT NULL DEFAULT 'GET', interval INTEGER NOT NULL DEFAULT 60,
      timeout INTEGER NOT NULL DEFAULT 10, expected TEXT NOT NULL DEFAULT '200-299',
      active INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'pending',
      fails INTEGER NOT NULL DEFAULT 0, fail_since INTEGER, last_code INTEGER,
      last_ms INTEGER, last_error TEXT, last_checked INTEGER, created_at INTEGER NOT NULL,
      status_since INTEGER
    );
    CREATE TABLE IF NOT EXISTS checks (
      id INTEGER PRIMARY KEY,
      monitor_id INTEGER NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
      ts INTEGER NOT NULL, ok INTEGER NOT NULL, code INTEGER, ms INTEGER, error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_checks_monitor_ts ON checks(monitor_id, ts);
    CREATE INDEX IF NOT EXISTS idx_checks_ts ON checks(ts);
    CREATE TABLE IF NOT EXISTS incidents (
      id INTEGER PRIMARY KEY,
      monitor_id INTEGER NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
      started_at INTEGER NOT NULL, resolved_at INTEGER, reason TEXT,
      ack_status TEXT NOT NULL DEFAULT 'open', ack_by TEXT, ack_at INTEGER, note TEXT, reminded INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS incident_events (
      id INTEGER PRIMARY KEY,
      incident_id INTEGER NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
      ts INTEGER NOT NULL, type TEXT NOT NULL, member TEXT, text TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_events_incident ON incident_events(incident_id, ts);
    CREATE TABLE IF NOT EXISTS members (
      id INTEGER PRIMARY KEY, name TEXT NOT NULL, device TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE,
      created_at INTEGER NOT NULL, last_seen INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_incidents_monitor ON incidents(monitor_id, resolved_at);
    CREATE INDEX IF NOT EXISTS idx_incidents_started ON incidents(started_at);
    CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
  `);
}

const close = () => db && db.close();

const settings = {
  get() {
    const out = { ...DEFAULTS };
    for (const { key, value } of q('SELECT key, value FROM settings').all()) {
      if (key in DEFAULTS) out[key] = JSON.parse(value);
    }
    return out;
  },
  set(patch) {
    const stmt = q('INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value');
    for (const [k, v] of Object.entries(patch)) if (k in DEFAULTS) stmt.run(k, JSON.stringify(v));
    return settings.get();
  },
};

const UPTIME_WINDOW = 30 * 864e5;

const monitors = {
  list() {
    return q(`SELECT m.*, (SELECT ROUND(100.0 * SUM(ok) / COUNT(*), 2) FROM checks c
              WHERE c.monitor_id = m.id AND c.ts > ?) AS uptime
              FROM monitors m ORDER BY m.name COLLATE NOCASE`).all(Date.now() - UPTIME_WINDOW);
  },
  get: (id) => q('SELECT * FROM monitors WHERE id = ?').get(id),
  byUrl: (url) => q('SELECT * FROM monitors WHERE url = ?').get(url),
  add(m) {
    const r = q(`INSERT INTO monitors (name, url, method, interval, timeout, expected, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`).run(m.name, m.url, m.method, m.interval, m.timeout, m.expected, Date.now());
    return monitors.get(Number(r.lastInsertRowid));
  },
  update(id, m) {
    q('UPDATE monitors SET name = ?, url = ?, method = ?, interval = ?, timeout = ?, expected = ? WHERE id = ?')
      .run(m.name, m.url, m.method, m.interval, m.timeout, m.expected, id);
    return monitors.get(id);
  },
  remove: (id) => q('DELETE FROM monitors WHERE id = ?').run(id),
  setState(id, fields) {
    const keys = Object.keys(fields).filter((k) => MONITOR_STATE.includes(k));
    if (!keys.length) return;
    q(`UPDATE monitors SET ${keys.map((k) => `${k} = ?`).join(', ')} WHERE id = ?`)
      .run(...keys.map((k) => n(fields[k])), id);
  },
};

const checks = {
  add: (id, r, ts) => q('INSERT INTO checks (monitor_id, ts, ok, code, ms, error) VALUES (?, ?, ?, ?, ?, ?)')
    .run(id, ts, r.ok ? 1 : 0, n(r.code), n(r.ms), n(r.error)),
  recent: (id, limit = 30) => q('SELECT ts, ok, code, ms, error FROM checks WHERE monitor_id = ? ORDER BY ts DESC LIMIT ?').all(id, limit),
};

const incidents = {
  get: (id) => q('SELECT i.*, m.name, m.url FROM incidents i JOIN monitors m ON m.id = i.monitor_id WHERE i.id = ?').get(id),
  open: (id) => q('SELECT * FROM incidents WHERE monitor_id = ? AND resolved_at IS NULL').get(id),
  allOpen: () => q('SELECT i.*, m.name, m.url FROM incidents i JOIN monitors m ON m.id = i.monitor_id WHERE i.resolved_at IS NULL ORDER BY i.started_at DESC').all(),
  start: (id, ts, reason) => Number(q('INSERT INTO incidents (monitor_id, started_at, reason) VALUES (?, ?, ?)').run(id, ts, n(reason)).lastInsertRowid),
  setAck: (id, status, by, note) => q('UPDATE incidents SET ack_status = ?, ack_by = ?, ack_at = ?, note = ? WHERE id = ?').run(status, n(by), Date.now(), n(note), id),
  setReminded: (id) => q('UPDATE incidents SET reminded = 1 WHERE id = ?').run(id),
  event: (id, type, member, text) => q('INSERT INTO incident_events (incident_id, ts, type, member, text) VALUES (?, ?, ?, ?, ?)').run(id, Date.now(), type, n(member), n(text)),
  events: (id) => q('SELECT ts, type, member, text FROM incident_events WHERE incident_id = ? ORDER BY ts, id').all(id),
  resolve: (incidentId, ts) => q('UPDATE incidents SET resolved_at = ? WHERE id = ?').run(ts, incidentId),
  list({ monitorId, status } = {}) {
    const where = [];
    const args = [];
    if (monitorId) { where.push('i.monitor_id = ?'); args.push(Number(monitorId)); }
    if (status === 'ongoing') where.push('i.resolved_at IS NULL');
    if (status === 'resolved') where.push('i.resolved_at IS NOT NULL');
    return q(`SELECT i.*, m.name, m.url FROM incidents i JOIN monitors m ON m.id = i.monitor_id
              ${where.length ? 'WHERE ' + where.join(' AND ') : ''} ORDER BY i.started_at DESC LIMIT 500`).all(...args);
  },
};

// Remove history older than the retention period (ongoing incidents are always kept)
function prune(days) {
  const cutoff = Date.now() - days * 864e5;
  q('DELETE FROM checks WHERE ts < ?').run(cutoff);
  q('DELETE FROM incidents WHERE resolved_at IS NOT NULL AND resolved_at < ?').run(cutoff);
}

const members = {
  add: (name, device, hash) => q('INSERT INTO members (name, device, token_hash, created_at, last_seen) VALUES (?, ?, ?, ?, ?)').run(name, device, hash, Date.now(), Date.now()),
  byToken: (hash) => q('SELECT * FROM members WHERE token_hash = ?').get(hash),
  rename: (id, name) => q('UPDATE members SET name = ? WHERE id = ?').run(name, id),
  seen: (id) => q('UPDATE members SET last_seen = ? WHERE id = ?').run(Date.now(), id),
  list: () => q('SELECT id, name, device, created_at, last_seen FROM members ORDER BY name COLLATE NOCASE').all(),
  // One record per PC: joining again from the same device replaces its token
  removeDevice: (device) => q('DELETE FROM members WHERE device = ?').run(device),
};

module.exports = { init, close, settings, monitors, checks, incidents, members, prune, DEFAULTS };
