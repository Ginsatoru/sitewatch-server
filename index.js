// SiteWatch team server. No dependencies: node:http, node:sqlite and Server-Sent Events for live updates.
const http = require('node:http');
const crypto = require('node:crypto');
const path = require('node:path');
const fs = require('node:fs');
const db = require('./db');
const engine = require('./engine');
const v = require('./validate');
const auth = require('./auth');
const wpStore = require('./wp/store');
const wpClient = require('./wp/client');
const WpManager = require('./wp/manager');
const logo = require('./logo');

const PORT = Number(process.env.PORT || 4100);
const SECRET = process.env.SECRET || '';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
if (SECRET.length < 16) {
  console.error('Set SECRET (at least 16 characters) in .env');
  process.exit(1);
}
fs.mkdirSync(DATA_DIR, { recursive: true });
db.init(path.join(DATA_DIR, 'sitewatch.db'));
wpStore.init(db.raw(), auth.cipher(SECRET));
logo.init();
const wp = new WpManager({ store: wpStore, client: wpClient, settings: db.settings });

const sha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
class HttpError extends Error { constructor(status, message) { super(message); this.status = status; } }

// ---- Live updates: every app keeps one event stream open
const clients = new Set(); // { res, member } where member is the signed-in user
const send = (res, event, data) => res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
const broadcast = (event, data) => { for (const c of clients) send(c.res, event, data); };
let pending = null;
function changed() {
  if (pending) return;
  pending = setTimeout(() => { pending = null; broadcast('changed', { at: Date.now() }); }, 200);
}
setInterval(() => { for (const c of clients) c.res.write(': ping\n\n'); }, 25000); // keeps proxies from closing idle streams
const onlineIds = () => new Set([...clients].map((c) => c.member.id));

engine.on('change', changed);
wp.on('change', changed);
wp.on('event', (e) => broadcast('wp', e));
// Ends the live streams of a removed user so their app signs out right away
const dropUser = (id) => { for (const c of clients) if (c.member.id === id) c.res.end(); };
engine.on('alert', (a) => broadcast('alert', {
  type: a.type, incidentId: a.incidentId, reason: a.reason || null, duration: a.duration || null,
  monitor: { id: a.monitor.id, name: a.monitor.name, url: a.monitor.url },
}));

// ---- Routing
const routes = [];
// access: 'open' (no login), 'member' (any signed-in user) or 'admin'
const route = (method, pattern, fn, access = 'member') => routes.push({
  method, fn, access, re: new RegExp(`^${pattern.replace(/:(\w+)/g, '(?<$1>[^/]+)')}$`),
});
const num = (p) => v.int(p, 1, Number.MAX_SAFE_INTEGER, 'Id');

const publicUser = (u) => ({ id: u.id, name: u.name, email: u.email, role: u.role });

function state(member) {
  const online = onlineIds();
  const open = new Map(db.incidents.allOpen().map((i) => [i.monitor_id, i]));
  const monitors = db.monitors.list().map((m) => {
    const i = open.get(m.id);
    return { ...m, incident: i ? { id: i.id, started_at: i.started_at, ack_status: i.ack_status, ack_by: i.ack_by, ack_at: i.ack_at, note: i.note } : null };
  });
  return {
    me: publicUser(member),
    monitors,
    summary: engine.summary(),
    settings: db.settings.get(),
    members: db.users.list().map((u) => ({ ...u, online: online.has(u.id) })),
    wp: wp.summary(),
  };
}

// ---- Sign in: email + password, a few wrong tries per minute per address
const attempts = new Map();
function limit(key) {
  const now = Date.now();
  const recent = (attempts.get(key) || []).filter((t) => now - t < 60000);
  if (recent.length >= 5) throw new HttpError(429, 'Too many attempts, wait a minute');
  attempts.set(key, [...recent, now]);
}

route('POST', '/api/login', ({ body, ip }) => {
  limit(ip);
  const user = db.users.byEmail(String(body.email || '').trim().toLowerCase());
  if (!user || !auth.checkPassword(body.password, user.pass_hash)) throw new HttpError(403, 'Wrong email or password');
  const device = String(body.device || '');
  if (!/^[A-Za-z0-9-]{8,64}$/.test(device)) throw new HttpError(400, 'Invalid device');
  const token = crypto.randomBytes(32).toString('hex');
  db.sessions.add(user.id, sha(token), device);
  return { token, user: publicUser(user) };
}, 'open');
route('POST', '/api/logout', ({ token }) => { db.sessions.remove(sha(token)); return true; });

route('PUT', '/api/me', ({ member, body }) => {
  const fields = {};
  if (body.name !== undefined) fields.name = v.cleanName(body.name);
  if (body.password !== undefined) {
    const user = db.users.byEmail(member.email);
    if (!auth.checkPassword(body.current, user.pass_hash)) throw new HttpError(403, 'Current password is wrong');
    fields.pass_hash = auth.hashPassword(auth.validPassword(body.password));
  }
  db.users.update(member.id, fields);
  changed();
  return publicUser(db.users.get(member.id));
});

// ---- Accounts (admins only)
function cleanUser(b, isNew) {
  const out = {};
  if (isNew || b.name !== undefined) out.name = v.cleanName(b.name);
  if (isNew || b.email !== undefined) {
    out.email = String(b.email || '').trim().toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(out.email)) throw new HttpError(400, 'Enter a valid email');
  }
  if (isNew || b.role !== undefined) out.role = b.role === 'admin' ? 'admin' : 'member';
  if (isNew || b.password) out.pass_hash = auth.hashPassword(auth.validPassword(b.password));
  return out;
}
route('GET', '/api/users', () => db.users.list(), 'admin');
route('POST', '/api/users', ({ body }) => {
  const u = cleanUser(body, true);
  if (db.users.byEmail(u.email)) throw new HttpError(409, 'That email already has an account');
  const id = db.users.add(u);
  changed();
  return db.users.get(id);
}, 'admin');
route('PUT', '/api/users/:id', ({ params, body, member }) => {
  const id = num(params.id);
  const u = cleanUser(body, false);
  const target = db.users.get(id);
  if (!target) throw new HttpError(404, 'User not found');
  if (u.email && u.email !== target.email && db.users.byEmail(u.email)) throw new HttpError(409, 'That email already has an account');
  if (u.role === 'member' && target.role === 'admin' && db.users.admins() <= 1) throw new HttpError(409, 'Keep at least one admin');
  db.users.update(id, u);
  if (u.pass_hash && id !== member.id) { db.sessions.removeUser(id); dropUser(id); } // new password: sign them out
  changed();
  return db.users.get(id);
}, 'admin');
route('DELETE', '/api/users/:id', ({ params, member }) => {
  const id = num(params.id);
  if (id === member.id) throw new HttpError(409, "You can't remove your own account");
  const target = db.users.get(id);
  if (target?.role === 'admin' && db.users.admins() <= 1) throw new HttpError(409, 'Keep at least one admin');
  db.users.remove(id);
  dropUser(id);
  changed();
  return true;
}, 'admin');

route('GET', '/api/health', () => ({ ok: true }), 'open');
route('GET', '/api/state', ({ member }) => state(member));

route('POST', '/api/monitors', ({ body }) => engine.add(v.cleanMonitor(body)), 'admin');
route('PUT', '/api/monitors/:id', ({ params, body }) => engine.update(num(params.id), v.cleanMonitor(body)), 'admin');
route('DELETE', '/api/monitors/:id', ({ params }) => { engine.remove(num(params.id)); return true; }, 'admin');
route('POST', '/api/monitors/:id/pause', ({ params }) => { engine.pause(num(params.id)); return true; }, 'admin');
route('POST', '/api/monitors/:id/resume', ({ params }) => { engine.resume(num(params.id)); return true; }, 'admin');
route('POST', '/api/monitors/:id/check', async ({ params }) => { await engine.checkNow(num(params.id)); return db.monitors.get(num(params.id)); });
route('GET', '/api/monitors/:id/history', ({ params }) => db.checks.recent(num(params.id), 30));
// One-time import of a desktop app's existing monitors; URLs already on the server are skipped
route('POST', '/api/monitors/import', ({ body }) => {
  let added = 0;
  let skipped = 0;
  for (const m of (Array.isArray(body.monitors) ? body.monitors : []).slice(0, 500)) {
    try {
      const clean = v.cleanMonitor(m);
      if (db.monitors.byUrl(clean.url)) { skipped++; continue; }
      engine.add(clean);
      added++;
    } catch { skipped++; }
  }
  return { added, skipped };
}, 'admin');

route('GET', '/api/incidents', ({ query }) => db.incidents.list({
  monitorId: query.get('monitorId') ? num(query.get('monitorId')) : null,
  status: ['ongoing', 'resolved'].includes(query.get('status')) ? query.get('status') : null,
}));
route('GET', '/api/incidents/:id/events', ({ params }) => db.incidents.events(num(params.id)));
const ACK_TEXT = { open: 'Cleared their status', investigating: 'Is looking into it', contacted: 'Contacted the customer or host' };
route('POST', '/api/incidents/:id/ack', ({ params, body, member }) => {
  const id = num(params.id);
  const inc = db.incidents.get(id);
  if (!inc) throw new HttpError(404, 'Incident not found');
  if (inc.resolved_at) throw new HttpError(409, 'This website is already back online');
  const { status, note } = v.cleanAck(body);
  db.incidents.setAck(id, status, status === 'open' ? null : member.name, note);
  db.incidents.event(id, 'status', member.name, note ? `${ACK_TEXT[status]}: ${note}` : ACK_TEXT[status]);
  broadcast('ack', { incidentId: id, status, by: member.name, note, monitor: { id: inc.monitor_id, name: inc.name, url: inc.url } });
  changed();
  return db.incidents.get(id);
});

// Notes can be added any time, including after the site recovered (e.g. what fixed it)
route('POST', '/api/incidents/:id/note', ({ params, body, member }) => {
  const id = num(params.id);
  if (!db.incidents.get(id)) throw new HttpError(404, 'Incident not found');
  const note = String(body.note || '').trim();
  if (!note || note.length > 300) throw new HttpError(400, 'Enter a note (up to 300 characters)');
  db.incidents.setNote(id, note);
  db.incidents.event(id, 'note', member.name, `Added a note: ${note}`);
  changed();
  return db.incidents.get(id);
});

// Site logo for the cards (data URI or null), cached on the server
route('GET', '/api/logo', async ({ query }) => {
  let url;
  try { url = new URL(String(query.get('url') || '')); } catch { throw new HttpError(400, 'Invalid url'); }
  if (!['http:', 'https:'].includes(url.protocol)) throw new HttpError(400, 'Invalid url');
  return logo.get(url.origin + '/');
});

route('GET', '/api/settings', () => db.settings.get());
route('PUT', '/api/settings', ({ body }) => {
  const clean = v.cleanSettings(body);
  const s = db.settings.set(clean);
  if ('retention' in clean) engine.prune();
  changed();
  return s;
}, 'admin');
route('POST', '/api/pause-all', () => { engine.pauseAll(); return true; }, 'admin');
route('POST', '/api/resume-all', () => { engine.resumeAll(); return true; }, 'admin');

// ---- WordPress (sites, updates and schedule run on the server; changes are admin only)
const STATUSES = ['pending', 'running', 'successful', 'failed', 'restored', 'recovery_required', 'cancelled'];
function cleanSite(s = {}, requireKey) {
  let url = String(s.url || '').trim();
  if (!/^https?:\/\//i.test(url)) url = `https://${url}`;
  let parsed;
  try { parsed = new URL(url); } catch { throw new HttpError(400, 'Enter a valid website URL'); }
  if (!['http:', 'https:'].includes(parsed.protocol)) throw new HttpError(400, 'Enter a valid http or https URL');
  const name = String(s.name || '').trim() || parsed.hostname.replace(/^www\./, '');
  if (name.length > 100) throw new HttpError(400, 'Name must be 100 characters or fewer');
  const key = String(s.key || '').trim();
  if ((requireKey || key) && !/^[a-f0-9]{64}$/i.test(key)) throw new HttpError(400, 'The agent key is the 64 character code from Settings > SiteWatch Agent');
  return { name, url: parsed.origin + parsed.pathname.replace(/\/+$/, ''), key };
}
function cleanSchedule(p = {}) {
  const out = {};
  for (const k of ['wpAuto', 'wpAutoPlugins', 'wpAutoThemes', 'wpAutoMaint']) if (k in p) out[k] = !!p[k];
  if ('wpFrequency' in p) out.wpFrequency = p.wpFrequency === 'weekly' ? 'weekly' : 'daily';
  if ('wpDay' in p) out.wpDay = v.int(p.wpDay, 0, 6, 'Day');
  if ('wpTime' in p) {
    if (!/^([01]\d|2[0-3]):[0-5]\d$/.test(p.wpTime)) throw new HttpError(400, 'Time must look like 03:00');
    out.wpTime = p.wpTime;
  }
  if ('wpCheckEvery' in p) out.wpCheckEvery = v.int(p.wpCheckEvery, 5, 1440, 'Check interval');
  if ('wpMaintTimeout' in p) out.wpMaintTimeout = v.int(p.wpMaintTimeout, 5, 240, 'Maintenance timeout');
  if (out.wpAuto && !db.settings.get().wpAuto) out.wpLastRun = Date.now(); // no catch-up run when switching on
  return out;
}
const schedule = () => ({ ...db.settings.get(), next: wp.nextRun() });

route('GET', '/api/wp/sites', () => wpStore.sites.list().map((s) => ({ ...s, latest: wpStore.jobs.latest(s.id) })));
route('POST', '/api/wp/sites', ({ body }) => {
  const site = wpStore.sites.add(cleanSite(body, true));
  wp.checkSite(site.id, true);
  return site;
}, 'admin');
route('PUT', '/api/wp/sites/:id', ({ params, body }) => {
  const site = wpStore.sites.update(num(params.id), cleanSite(body, false));
  wp.checkSite(site.id, true);
  return site;
}, 'admin');
route('DELETE', '/api/wp/sites/:id', ({ params }) => {
  if (wp.running.has(num(params.id))) throw new HttpError(409, 'Wait for the running update to finish first');
  wpStore.sites.remove(num(params.id));
  wp.changed();
  return true;
}, 'admin');
route('POST', '/api/wp/sites/:id/test', async ({ params }) => { await wp.checkSite(num(params.id), true); return wpStore.sites.get(num(params.id)); });
route('POST', '/api/wp/probe', async ({ body }) => {
  const s = cleanSite(body, false);
  const k = s.key || (body.siteId ? wpStore.sites.secret(num(body.siteId)).key : '');
  if (!k) {
    if (!(await wpClient.ping(s.url))) throw new HttpError(400, 'Agent not found on this site');
    return { agent: true, connected: false };
  }
  try {
    const info = await wpClient.status({ url: s.url, key: k });
    return { agent: true, connected: true, name: info.name, home: info.home };
  } catch (err) {
    throw new HttpError(400, err.code === 'auth' ? 'Invalid agent key' : err.code === 'agent' ? 'Agent not found on this site' : err.message);
  }
}, 'admin');
route('POST', '/api/wp/update', ({ body }) => {
  if (!['plugin', 'theme'].includes(body.type) || !Array.isArray(body.items)) throw new HttpError(400, 'Invalid request');
  const n = wp.queue(num(body.siteId), body.type, body.items.map(String).slice(0, 500));
  if (!n) throw new HttpError(409, 'Nothing to update (already queued or up to date)');
  return n;
}, 'admin');
route('POST', '/api/wp/sites/:id/auto', ({ params, body }) => { wpStore.sites.setAuto(num(params.id), body.plugins, body.themes); wp.changed(); return true; }, 'admin');
route('POST', '/api/wp/sites/:id/excluded', ({ params, body }) => {
  if (!/^(plugin|theme):.{1,300}$/.test(String(body.key))) throw new HttpError(400, 'Invalid item');
  const site = wpStore.sites.get(num(params.id));
  const list = site.excluded.filter((k) => k !== body.key);
  wpStore.sites.setExcluded(site.id, body.excluded ? [...list, body.key] : list);
  wp.changed();
  return true;
}, 'admin');
route('POST', '/api/wp/sites/:id/maintenance', async ({ params }) => {
  const r = await wp.clearMaintenance(num(params.id), 'manual');
  await wp.checkSite(num(params.id));
  return r;
}, 'admin');
route('GET', '/api/wp/jobs', ({ query }) => wpStore.jobs.list({
  siteId: query.get('siteId') ? num(query.get('siteId')) : null,
  status: STATUSES.includes(query.get('status')) ? query.get('status') : null,
}));
route('POST', '/api/wp/jobs/clear', ({ body }) => { const n = wpStore.jobs.clear(body.siteId ? num(body.siteId) : null); wp.changed(); return n; }, 'admin');
route('POST', '/api/wp/jobs/:id/cancel', ({ params }) => { wp.cancel(String(params.id)); return true; }, 'admin');
route('POST', '/api/wp/cancel-all', ({ body }) => wp.cancelAll(body.siteId ? num(body.siteId) : null), 'admin');
route('POST', '/api/wp/run', () => { wp.runSchedule('manual'); return true; }, 'admin');
route('GET', '/api/wp/schedule', schedule);
route('PUT', '/api/wp/schedule', ({ body }) => { db.settings.set(cleanSchedule(body)); wp.changed(); return schedule(); }, 'admin');
// One-time import of a desktop app's local WordPress sites (keys included); URLs already shared are skipped
route('POST', '/api/wp/import', ({ body }) => {
  const existing = new Set(wpStore.sites.list().map((s) => s.url));
  let added = 0;
  for (const s of (Array.isArray(body.sites) ? body.sites : []).slice(0, 200)) {
    try {
      const clean = cleanSite(s, true);
      if (existing.has(clean.url)) continue;
      const site = wpStore.sites.add(clean);
      wpStore.sites.setAuto(site.id, s.auto_plugins !== 0, !!s.auto_themes);
      if (Array.isArray(s.excluded)) wpStore.sites.setExcluded(site.id, s.excluded.map(String).slice(0, 500));
      existing.add(clean.url);
      added++;
      wp.checkSite(site.id, true);
    } catch { /* skip invalid */ }
  }
  wp.changed();
  return { added };
}, 'admin');

// ---- HTTP plumbing
function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > 200 * 1024) { reject(new HttpError(413, 'Request too large')); req.destroy(); } else chunks.push(c);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch { reject(new HttpError(400, 'Invalid JSON')); }
    });
    req.on('error', reject);
  });
}

const tokenOf = (req) => (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
function authenticate(req) {
  const token = tokenOf(req);
  const user = /^[a-f0-9]{64}$/.test(token) ? db.sessions.user(sha(token)) : null;
  if (!user) throw new HttpError(401, 'Please sign in again');
  if (!lastSeen.has(user.id) || Date.now() - lastSeen.get(user.id) > 60000) { lastSeen.set(user.id, Date.now()); db.users.seen(user.id); }
  return user;
}
const lastSeen = new Map();

const json = (res, status, data) => {
  res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(data));
};

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  try {
    if (req.method === 'GET' && url.pathname === '/api/events') {
      const member = authenticate(req);
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      const client = { res, member };
      clients.add(client);
      send(res, 'changed', { at: Date.now() });
      changed(); // others see this member come online
      req.on('close', () => { clients.delete(client); changed(); });
      return;
    }
    const r = routes.find((x) => x.method === req.method && x.re.test(url.pathname));
    if (!r) throw new HttpError(404, 'Not found');
    const member = r.access === 'open' ? null : authenticate(req);
    if (r.access === 'admin' && member.role !== 'admin') throw new HttpError(403, 'Only admins can do this');
    const body = ['POST', 'PUT'].includes(req.method) ? await readBody(req) : {};
    const data = await r.fn({ member, body, params: url.pathname.match(r.re).groups || {}, query: url.searchParams, ip, token: tokenOf(req) });
    json(res, 200, { data });
  } catch (err) {
    if (!err.status) console.error(err);
    json(res, err.status || (err.message ? 400 : 500), { error: err.message || 'Server error' });
  }
});

engine.start();
wp.start();
server.listen(PORT, '127.0.0.1', () => console.log(`SiteWatch team server on 127.0.0.1:${PORT}`));

function stop() {
  engine.shutdown();
  wp.stop();
  for (const c of clients) c.res.end();
  server.close(() => { db.close(); process.exit(0); });
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
