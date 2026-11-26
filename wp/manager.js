// WordPress update manager: job queue, recovery after interruptions, schedule and maintenance checks.
// Jobs for one site run one at a time; up to 3 sites update in parallel.
const { EventEmitter } = require('node:events');
const crypto = require('node:crypto');

const PARALLEL = 3;
const FINAL = ['successful', 'failed', 'restored', 'recovery_required'];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const label = (job, siteName) => `${job.name || job.item} on ${siteName || 'site'}`;

class WpManager extends EventEmitter {
  constructor({ store, client, settings }) {
    super();
    Object.assign(this, { store, client, settings });
    this.running = new Map(); // site id -> job id
    this.checking = new Set();
    this.progress = new Map(); // job id -> percent, polled while an update runs
    this.pollMs = 10000;
    this.pollLimit = 20 * 60000; // give up collecting a result after 20 minutes
  }

  start() {
    // Jobs left "running" by a shutdown or crash: find out what happened on the server
    for (const job of this.store.jobs.byStatus('running')) {
      this.running.set(job.site_id, job.id);
      this.recover(job).finally(() => { this.running.delete(job.site_id); this.pump(); });
    }
    this.pump(); // pending jobs never started, so they simply continue
    this.timer = setInterval(() => this.tick(), 60000);
    this.firstTick = setTimeout(() => this.tick(), 5000);
  }

  stop() {
    this.stopped = true;
    clearInterval(this.timer);
    clearTimeout(this.firstTick);
  }

  changed() { this.emit('change'); }
  say(level, title, body) { this.emit('event', { level, title, body }); }

  // ---- Connection checks
  async checkSite(id, refresh = false) {
    if (this.checking.has(id)) return;
    const site = this.store.sites.secret(id);
    if (!site) return;
    this.checking.add(id);
    this.changed();
    try {
      const info = await this.client.status(site, refresh);
      this.store.sites.setState(id, { status: 'connected', error: null, info, last_check: Date.now() });
      if (await this.handleMaintenance(id, info.maintenance)) {
        this.store.sites.setState(id, { info: await this.client.status(site) });
      }
    } catch (err) {
      const maintenance = err.code === 'maintenance';
      this.store.sites.setState(id, { status: maintenance ? 'maintenance' : 'error', error: err.message, last_check: Date.now() });
      if (maintenance) {
        try { await this.handleMaintenance(id, (await this.client.rescueStatus(site)).maintenance); } catch { /* rescue unreachable */ }
      }
    } finally {
      this.checking.delete(id);
      this.changed();
    }
  }

  // Stale .maintenance: only when older than the timeout and no update holds a lock
  async handleMaintenance(id, m) {
    const s = this.settings.get();
    if (!m?.exists || m.locks > 0 || m.age < s.wpMaintTimeout * 60) return false;
    if (!s.wpAutoMaint) {
      const site = this.store.sites.get(id);
      this.say('error', 'Website stuck in maintenance mode', `${site.name} has been in maintenance mode for ${Math.round(m.age / 60)} minutes`);
      return false;
    }
    const r = await this.clearMaintenance(id, 'system');
    return r.status === 'successful';
  }

  async clearMaintenance(id, source = 'manual') {
    const site = this.store.sites.get(id);
    const job = this.store.jobs.create({ id: crypto.randomUUID(), site_id: id, kind: 'maintenance', name: 'Maintenance mode', status: 'running', source });
    this.store.jobs.update(job.id, { started_at: Date.now() });
    let status = 'failed';
    let message;
    try {
      const r = await this.client.clearMaintenance(this.store.sites.secret(id), this.settings.get().wpMaintTimeout);
      message = r.message;
      if (r.status === 'cleared') status = 'successful';
      else if (r.status === 'cleared_unhealthy') status = 'recovery_required';
    } catch (err) {
      message = `Could not reach the rescue endpoint: ${err.message}`;
    }
    this.store.jobs.update(job.id, { status, message, steps: [message], finished_at: Date.now() });
    if (status === 'successful') this.say('success', 'Maintenance mode cleared', `${site.name}: ${message}`);
    else if (source === 'manual' || status === 'recovery_required') this.say('error', 'Maintenance recovery needs attention', `${site.name}: ${message}`);
    this.changed();
    return { status, message };
  }

  // ---- Queue
  queue(siteId, type, items, { source = 'manual', batch = crypto.randomUUID() } = {}) {
    const site = this.store.sites.get(siteId);
    const list = (type === 'theme' ? site?.info?.themes : site?.info?.plugins) || [];
    let count = 0;
    for (const file of items) {
      const it = list.find((x) => x.file === file);
      if (!it || it.agent || !it.active || this.store.jobs.active(siteId, type, file)) continue; // inactive plugins and themes are skipped
      this.store.jobs.create({ id: crypto.randomUUID(), site_id: siteId, kind: type, item: file, name: it.name,
        from_version: it.version, to_version: it.update, status: 'pending', source, batch });
      count++;
    }
    if (count) { this.changed(); this.pump(); }
    return count;
  }

  pump() {
    if (this.stopped) return;
    for (const job of this.store.jobs.byStatus('pending')) {
      if (this.running.size >= PARALLEL) break;
      if (this.running.has(job.site_id)) continue;
      this.running.set(job.site_id, job.id);
      this.runJob(job).catch((err) => console.error('Job failed', err))
        .finally(() => { this.running.delete(job.site_id); this.pump(); });
    }
  }

  async runJob(job) {
    const site = this.store.sites.secret(job.site_id);
    if (!site) return this.finish(job, { status: 'failed', message: 'Website was removed' });
    this.store.jobs.update(job.id, { status: 'running', started_at: Date.now() });
    this.progress.set(job.id, 0);
    this.changed();
    const stopPolling = this.pollProgress(site, job.id);
    let r;
    try {
      r = await this.client.update(site, { type: job.kind, item: job.item, job: job.id });
    } catch (err) {
      r = ['auth', 'agent'].includes(err.code)
        ? { status: 'failed', message: err.message, steps: [err.message] }
        : await this.collect(site, job, `Connection lost during the update: ${err.message}`);
    } finally {
      stopPolling();
      this.progress.delete(job.id);
    }
    this.finish(job, r);
    await this.checkSite(job.site_id);
  }

  // Asks the rescue endpoint (works during maintenance mode) how far the update is
  pollProgress(site, jobId) {
    let busy = false;
    const timer = setInterval(async () => {
      if (busy) return;
      busy = true;
      try {
        const { pct } = await this.client.progress(site, jobId);
        if (pct != null && this.progress.has(jobId) && pct !== this.progress.get(jobId)) {
          this.progress.set(jobId, pct);
          this.changed();
        }
      } catch { /* older agent or busy server: keep the last value */ }
      busy = false;
    }, 2000);
    return () => clearInterval(timer);
  }

  // After a dropped connection or restart: ask the agent (or the rescue endpoint) for the outcome
  async collect(site, job, why) {
    const until = Date.now() + this.pollLimit;
    let last = 'Could not reach the site';
    while (!this.stopped && Date.now() < until) {
      await sleep(this.pollMs);
      let r;
      try { r = await this.client.resolve(site, job.id); } catch (err) {
        try { r = await this.client.rescueResolve(site, job.id); } catch (err2) { last = err2.message; continue; }
      }
      if (r.status === 'running') continue;
      if (r.status === 'unknown') return { status: 'failed', message: r.message, steps: [why, r.message] };
      return { ...r, steps: [why, ...(r.steps || [])] };
    }
    if (this.stopped) return null;
    return { status: 'recovery_required', message: `Result unknown: ${last}`, steps: [why, `Result unknown: ${last}`] };
  }

  async recover(job) {
    const site = this.store.sites.secret(job.site_id);
    if (!site) return this.finish(job, { status: 'failed', message: 'Website was removed' });
    this.finish(job, await this.collect(site, job, 'SiteWatch was closed during this update'));
    await this.checkSite(job.site_id);
  }

  finish(job, r) {
    if (!r) return; // app shutting down, job stays "running" and is recovered on next start
    const status = FINAL.includes(r.status) ? r.status : 'failed';
    const site = this.store.sites.get(job.site_id);
    const error = (r.steps || []).find((x) => x.startsWith('Update reported an error:'));
    const message = status === 'successful' ? `Updated to ${r.to}`
      : status === 'restored' ? 'Update broke the site, previous version restored'
      : status === 'failed' && error ? error.replace('Update reported an error: ', '') : r.message;
    this.store.jobs.update(job.id, { status, message: message || null, steps: r.steps || [], to_version: r.to || job.from_version, finished_at: Date.now() });
    if (status === 'successful' && site) this.store.sites.setState(site.id, { last_update: Date.now() });

    const what = label(job, site?.name);
    if (status === 'failed') this.say('error', 'Update failed', `${what}: ${r.message || 'unknown error'}`);
    if (status === 'restored') this.say('error', `${job.kind === 'theme' ? 'Theme' : 'Plugin'} automatically restored`, `${what}: update broke the site, previous version restored`);
    if (status === 'recovery_required') this.say('error', 'Manual attention required', `${what}: ${r.message || 'restore could not be confirmed'}`);
    if (job.batch && !this.store.jobs.openInBatch(job.batch)) this.summarize(job.batch);
    this.changed();
  }

  // One notification per batch so bulk and scheduled runs don't flood the screen
  summarize(batch) {
    const done = this.store.jobs.batch(batch);
    const ok = done.filter((j) => j.status === 'successful');
    if (!ok.length) return;
    if (done.length === 1) {
      const j = ok[0];
      const site = this.store.sites.get(j.site_id);
      return this.say('success', `${j.kind === 'theme' ? 'Theme' : 'Plugin'} updated`, `${label(j, site?.name)} is now ${j.to_version}`);
    }
    const failed = done.filter((j) => ['failed', 'restored', 'recovery_required'].includes(j.status)).length;
    this.say(failed ? 'error' : 'success', 'Updates finished', `${ok.length} updated${failed ? `, ${failed} need attention` : ''}`);
  }

  // Cancel queued updates (never one that is already running)
  cancel(jobId) {
    const job = this.store.jobs.get(jobId);
    if (!job || !this.store.jobs.cancel(jobId)) throw new Error('This update already started and will finish on its own');
    if (job.batch && !this.store.jobs.openInBatch(job.batch)) this.summarize(job.batch);
    this.changed();
  }

  cancelAll(siteId) {
    const batches = this.store.jobs.openBatches();
    const n = this.store.jobs.cancelPending(siteId);
    for (const b of batches) if (!this.store.jobs.openInBatch(b)) this.summarize(b);
    this.changed();
    return n;
  }

  // ---- Schedule
  nextRun(s = this.settings.get()) {
    if (!s.wpAuto) return null;
    const [h, m] = s.wpTime.split(':').map(Number);
    const after = s.wpLastRun || Date.now();
    const d = new Date(after);
    d.setHours(h, m, 0, 0);
    for (let i = 0; i < 9; i++) {
      if (d.getTime() > after && (s.wpFrequency === 'daily' || d.getDay() === s.wpDay)) return d.getTime();
      d.setDate(d.getDate() + 1);
    }
    return null;
  }

  async runSchedule(trigger = 'schedule') {
    if (this.scheduling) return;
    this.settings.set({ wpLastRun: Date.now() });
    const record = (status, message) => this.store.jobs.create({ id: crypto.randomUUID(), kind: 'run', name: trigger === 'schedule' ? 'Scheduled run' : 'Manual run', status, message, source: trigger });
    if (this.store.jobs.busySchedule()) {
      record('failed', 'Skipped: previous updates are still running');
      return this.changed();
    }
    this.scheduling = true;
    try {
      const s = this.settings.get();
      const batch = crypto.randomUUID();
      let queued = 0;
      let siteCount = 0;
      for (const site of this.store.sites.list()) {
        const doPlugins = s.wpAutoPlugins && site.auto_plugins;
        const doThemes = s.wpAutoThemes && site.auto_themes;
        if (!doPlugins && !doThemes) continue;
        await this.checkSite(site.id, true);
        const fresh = this.store.sites.get(site.id);
        if (fresh.status !== 'connected') continue;
        const pick = (list, type) => (list || []).filter((i) => i.update && !i.agent && i.active && !fresh.excluded.includes(`${type}:${i.file}`)).map((i) => i.file);
        const n = (doPlugins ? this.queue(site.id, 'plugin', pick(fresh.info.plugins, 'plugin'), { source: 'schedule', batch }) : 0)
          + (doThemes ? this.queue(site.id, 'theme', pick(fresh.info.themes, 'theme'), { source: 'schedule', batch }) : 0);
        if (n) { queued += n; siteCount++; }
      }
      record('successful', queued ? `Queued ${queued} updates on ${siteCount} sites` : 'Everything is up to date');
    } finally {
      this.scheduling = false;
      this.changed();
    }
  }

  async tick() {
    if (this.stopped) return;
    const s = this.settings.get();
    const next = this.nextRun(s);
    if (next && next <= Date.now()) await this.runSchedule('schedule');
    for (const site of this.store.sites.list()) {
      if (!site.last_check || Date.now() - site.last_check > s.wpCheckEvery * 60000) await this.checkSite(site.id);
    }
    if (!this.pruned || Date.now() - this.pruned > 864e5) {
      this.pruned = Date.now();
      this.store.jobs.prune(365);
    }
  }

  // Overall progress per site for its open batches: finished jobs plus the running job's percent
  siteProgress() {
    const out = {};
    for (const batch of this.store.jobs.openBatches()) {
      for (const j of this.store.jobs.batch(batch)) {
        const p = (out[j.site_id] ||= { done: 0, total: 0, sum: 0 });
        p.total++;
        if (FINAL.includes(j.status) || j.status === 'cancelled') { p.done++; p.sum += 100; } else if (j.status === 'running') p.sum += this.progress.get(j.id) || 0;
      }
    }
    for (const p of Object.values(out)) { p.pct = Math.round(p.sum / p.total); delete p.sum; }
    return out;
  }

  summary() {
    const list = this.store.sites.list();
    const updates = list.reduce((sum, s) => sum + [...(s.info?.plugins || []), ...(s.info?.themes || [])].filter((i) => i.update && !i.agent && i.active).length, 0);
    return { count: list.length, updates, running: this.running.size, next: this.nextRun(), checking: [...this.checking], progress: Object.fromEntries(this.progress), sites: this.siteProgress(), queued: this.store.jobs.pendingBySite() };
  }
}

module.exports = WpManager;