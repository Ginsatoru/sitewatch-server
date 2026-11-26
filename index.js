// SiteWatch team server. No dependencies: node:http, node:sqlite and Server-Sent Events for live updates.
const http = require('node:http');
const crypto = require('node:crypto');
const path = require('node:path');
const fs = require('node:fs');
const db = require('./db');
const engine = require('./engine');
const v = require('./validate');

const PORT = Number(process.env.PORT || 4100);
const TEAM_CODE = process.env.TEAM_CODE || '';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
if (TEAM_CODE.length < 8) {
  console.error('Set TEAM_CODE (at least 8 characters) in .env');
  process.exit(1);
}
fs.mkdirSync(DATA_DIR, { recursive: true });
db.init(path.join(DATA_DIR, 'sitewatch.db'));

const sha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
class HttpError extends Error { constructor(status, message) { super(message); this.status = status; } }

// ---- Live updates: every app keeps one event stream open
const clients = new Set(); // { res, member }
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
engine.on('alert', (a) => broadcast('alert', {
  type: a.type, incidentId: a.incidentId, reason: a.reason || null, duration: a.duration || null,
  monitor: { id: a.monitor.id, name: a.monitor.name, url: a.monitor.url },
}));

// ---- Routing
const routes = [];
const route = (method, pattern, fn, open = false) => routes.push({
  method, fn, open, re: new RegExp(`^${pattern.replace(/:(\w+)/g, '(?<$1>[^/]+)')}$`),
});
const num = (p) => v.int(p, 1, Number.MAX_SAFE_INTEGER, 'Id');

function state(member) {
  const online = onlineIds();
  const open = new Map(db.incidents.allOpen().map((i) => [i.monitor_id, i]));
  const monitors = db.monitors.list().map((m) => {
    const i = open.get(m.id);
    return { ...m, incident: i ? { id: i.id, started_at: i.started_at, ack_status: i.ack_status, ack_by: i.ack_by, ack_at: i.ack_at, note: i.note } : null };
  });
  return {
    me: { id: member.id, name: member.name },
    monitors,
    summary: engine.summary(),
    settings: db.settings.get(),
    members: db.members.list().map((m) => ({ ...m, online: online.has(m.id) })),
  };
}

// Joining needs the team code; a few wrong tries per minute per address are allowed
const attempts = new Map();
route('POST', '/api/join', ({ body, ip }) => {
  const now = Date.now();
  const recent = (attempts.get(ip) || []).filter((t) => now - t < 60000);
  if (recent.length >= 5) throw new HttpError(429, 'Too many attempts, wait a minute');
  attempts.set(ip, [...recent, now]);
  if (!crypto.timingSafeEqual(Buffer.from(sha(body.code)), Buffer.from(sha(TEAM_CODE)))) throw new HttpError(403, 'Wrong team code');
  const name = v.cleanName(body.name);
  const device = String(body.device || '');
  if (!/^[A-Za-z0-9-]{8,64}$/.test(device)) throw new HttpError(400, 'Invalid device');
  const token = crypto.randomBytes(32).toString('hex');
  db.members.removeDevice(device);
  db.members.add(name, device, sha(token));
  changed();
  return { token, name };
}, true);

route('GET', '/api/health', () => ({ ok: true }), true);
route('GET', '/api/state', ({ member }) => state(member));
route('PUT', '/api/me', ({ member, body }) => {
  db.members.rename(member.id, v.cleanName(body.name));
  changed();
  return { name: v.cleanName(body.name) };
});

route('POST', '/api/monitors', ({ body }) => engine.add(v.cleanMonitor(body)));
route('PUT', '/api/monitors/:id', ({ params, body }) => engine.update(num(params.id), v.cleanMonitor(body)));
route('DELETE', '/api/monitors/:id', ({ params }) => { engine.remove(num(params.id)); return true; });
route('POST', '/api/monitors/:id/pause', ({ params }) => { engine.pause(num(params.id)); return true; });
route('POST', '/api/monitors/:id/resume', ({ params }) => { engine.resume(num(params.id)); return true; });
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
});

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

route('GET', '/api/settings', () => db.settings.get());
route('PUT', '/api/settings', ({ body }) => {
  const clean = v.cleanSettings(body);
  const s = db.settings.set(clean);
  if ('retention' in clean) engine.prune();
  changed();
  return s;
});
route('POST', '/api/pause-all', () => { engine.pauseAll(); return true; });
route('POST', '/api/resume-all', () => { engine.resumeAll(); return true; });

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

function authenticate(req) {
  const token = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  const member = /^[a-f0-9]{64}$/.test(token) ? db.members.byToken(sha(token)) : null;
  if (!member) throw new HttpError(401, 'Not signed in to the team');
  if (!member.last_seen || Date.now() - member.last_seen > 60000) db.members.seen(member.id);
  return member;
}

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
    const member = r.open ? null : authenticate(req);
    const body = ['POST', 'PUT'].includes(req.method) ? await readBody(req) : {};
    const data = await r.fn({ member, body, params: url.pathname.match(r.re).groups || {}, query: url.searchParams, ip });
    json(res, 200, { data });
  } catch (err) {
    if (!err.status) console.error(err);
    json(res, err.status || (err.message ? 400 : 500), { error: err.message || 'Server error' });
  }
});

engine.start();
server.listen(PORT, '127.0.0.1', () => console.log(`SiteWatch team server on 127.0.0.1:${PORT}`));

function stop() {
  engine.shutdown();
  for (const c of clients) c.res.end();
  server.close(() => { db.close(); process.exit(0); });
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
