// Monitoring engine (server side). The server is the single source of truth for the team.
const { EventEmitter } = require('node:events');
const db = require('./db');
const http = require('./check');

class Engine extends EventEmitter {
  constructor() {
    super();
    this.timers = new Map();
    this.running = new Set();
    this.stopped = true;
    this.offline = false;
    this.isOnline = http.isOnline; // overridable for tests
  }

  get pausedAll() { return db.settings.get().pausedAll; }

  start() {
    if (!this.pausedAll) this.resumeAll(false);
    this.prune();
    this.pruneTimer = setInterval(() => this.prune(), 36e5);
    this.remindTimer = setInterval(() => this.remind(), 30000);
  }

  shutdown() {
    this.closed = true; // in-flight checks must not touch the database after this
    this.halt();
    clearInterval(this.pruneTimer);
    clearInterval(this.remindTimer);
  }

  halt() {
    this.stopped = true;
    for (const t of this.timers.values()) clearTimeout(t);
    this.timers.clear();
  }

  pauseAll() {
    this.halt();
    db.settings.set({ pausedAll: true });
    this.changed();
  }

  resumeAll(persist = true) {
    this.stopped = false;
    if (persist) db.settings.set({ pausedAll: false });
    // Stagger the first checks so many monitors don't fire at once
    db.monitors.list().filter((m) => m.active).forEach((m, i) => this.schedule(m.id, 500 + i * 300));
    this.changed();
  }

  schedule(id, delay) {
    clearTimeout(this.timers.get(id));
    if (this.stopped) return;
    this.timers.set(id, setTimeout(() => this.run(id), delay));
  }

  unschedule(id) {
    clearTimeout(this.timers.get(id));
    this.timers.delete(id);
  }

  async run(id) {
    if (this.running.has(id)) return; // never overlap checks for the same monitor
    const m = db.monitors.get(id);
    if (!m) return this.unschedule(id);
    this.running.add(id);
    this.changed();
    try {
      await this.check(m);
    } catch (err) {
      console.error('Check failed', id, err);
    } finally {
      this.running.delete(id);
      if (this.closed) return;
      const cur = db.monitors.get(id);
      if (cur && cur.active && !this.stopped) this.schedule(id, cur.interval * 1000);
      else this.unschedule(id);
      this.changed();
    }
  }

  async check(m) {
    const r = await http.checkUrl(m);
    if (this.closed) return;
    const now = Date.now();

    // A failure while the machine itself is offline says nothing about the website
    if (!r.ok && !(await this.isOnline())) {
      if (this.closed) return;
      this.setOffline(true);
      db.monitors.setState(m.id, { last_checked: now, last_error: 'Skipped: no internet connection' });
      return;
    }
    this.setOffline(false);
    const cur = db.monitors.get(m.id);
    if (!cur) return; // deleted while the request was in flight
    const active = cur.active; // may have been paused mid-request

    db.checks.add(m.id, r, now);
    const open = db.incidents.open(m.id);
    const base = { last_code: r.code, last_ms: r.ms, last_error: r.error, last_checked: now };

    if (r.ok) {
      const status = active ? 'up' : 'paused';
      db.monitors.setState(m.id, { ...base, status, fails: 0, fail_since: null, ...this.since(cur, status, now) });
      if (open) {
        db.incidents.resolve(open.id, now);
        db.incidents.event(open.id, 'recovered', null, 'Website is back online');
        this.emit('alert', { type: 'up', monitor: m, incidentId: open.id, duration: now - open.started_at });
      }
      return;
    }

    const fails = m.fails + 1;
    const failSince = m.fail_since || now;
    const isDown = fails >= db.settings.get().threshold;
    const status = !active ? 'paused' : isDown || cur.status === 'down' ? 'down' : cur.status;
    db.monitors.setState(m.id, { ...base, status, fails, fail_since: failSince, ...this.since(cur, status, open ? open.started_at : failSince) });
    if (isDown && !open) {
      const incidentId = db.incidents.start(m.id, failSince, r.error);
      db.incidents.event(incidentId, 'down', null, r.error);
      this.emit('alert', { type: 'down', monitor: m, incidentId, reason: r.error });
    }
  }

  // Stamp when a monitor entered its current up/down state (only on a change, or to backfill)
  since(cur, status, from) {
    if (status !== 'up' && status !== 'down') return {};
    return status === cur.status && cur.status_since != null ? {} : { status_since: from };
  }

  setOffline(value) {
    if (this.offline === value) return;
    this.offline = value;
    this.emit('connectivity', value);
    this.changed();
  }

  // ---- Monitor management (keeps the schedule in sync with the database)
  add(data) {
    const m = db.monitors.add(data);
    this.schedule(m.id, 300);
    this.changed();
    return m;
  }

  update(id, data) {
    const m = db.monitors.update(id, data);
    if (m.active) this.schedule(id, 300);
    this.changed();
    return m;
  }

  remove(id) {
    this.unschedule(id);
    db.monitors.remove(id);
    this.changed();
  }

  pause(id) {
    this.unschedule(id);
    db.monitors.setState(id, { active: 0, status: 'paused', status_since: null });
    this.changed();
  }

  resume(id) {
    db.monitors.setState(id, { active: 1, status: 'pending', fails: 0, fail_since: null, status_since: null });
    this.schedule(id, 300);
    this.changed();
  }

  checkNow(id) {
    this.unschedule(id);
    return this.run(id);
  }

  // Nobody picked up an incident in time: remind everyone once
  remind() {
    const after = db.settings.get().remindAfter * 60000;
    for (const i of db.incidents.allOpen()) {
      if (i.ack_status !== 'open' || i.reminded || Date.now() - i.started_at < after) continue;
      db.incidents.setReminded(i.id);
      db.incidents.event(i.id, 'reminder', null, 'Nobody has picked this up yet');
      this.emit('alert', { type: 'reminder', monitor: { id: i.monitor_id, name: i.name, url: i.url }, incidentId: i.id, reason: i.reason });
      this.changed();
    }
  }

  prune() {
    try { db.prune(db.settings.get().retention); } catch (err) { console.error('Prune failed', err); }
  }

  summary() {
    const list = db.monitors.list();
    const active = list.filter((m) => m.active);
    const timed = active.filter((m) => m.status === 'up' && m.last_ms != null);
    return {
      total: list.length,
      up: active.filter((m) => m.status === 'up').length,
      down: active.filter((m) => m.status === 'down').length,
      avgMs: timed.length ? Math.round(timed.reduce((s, m) => s + m.last_ms, 0) / timed.length) : null,
      offline: this.offline,
      pausedAll: this.pausedAll,
      checking: [...this.running],
    };
  }

  changed() { this.emit('change'); }
}

module.exports = new Engine();
module.exports.Engine = Engine;
