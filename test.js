// Server test with real HTTP: join, monitors, live events, down/claim/recover. Run: node test.js
const assert = require('node:assert');
const http = require('node:http');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');
process.env.SECRET = 'test-secret-1234567890';
process.env.PORT = '4199';
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'swsrv-'));
require('./engine').isOnline = async () => true; // the sandbox has no general internet
// first admin is created from the command line on the VPS; do the same here
const db0 = require('./db');
db0.init(path.join(process.env.DATA_DIR, 'sitewatch.db'));
db0.users.add({ name: 'Gin', email: 'gin@aaapos.com', pass_hash: require('./auth').hashPassword('admin-pass-1'), role: 'admin' });
db0.close();
require('./index');

const BASE = 'http://127.0.0.1:4199/api';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let mode = 200;
const site = http.createServer((q, r) => { r.writeHead(mode); r.end('ok'); }).listen(0);

async function api(method, p, body, token) {
  const res = await fetch(BASE + p, { method, body: body ? JSON.stringify(body) : undefined,
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) } });
  const j = await res.json();
  if (j.error) { const e = new Error(j.error); e.status = res.status; throw e; }
  return j.data;
}

// Minimal SSE reader collecting events
async function listen(token, bag) {
  const res = await fetch(`${BASE}/events`, { headers: { Authorization: `Bearer ${token}` } });
  const reader = res.body.getReader();
  let buf = '';
  (async () => {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buf += Buffer.from(value).toString();
      let i;
      while ((i = buf.indexOf('\n\n')) >= 0) {
        const block = buf.slice(0, i); buf = buf.slice(i + 2);
        const ev = /event: (.*)/.exec(block)?.[1];
        const data = /data: (.*)/.exec(block)?.[1];
        if (ev) bag.push({ ev, data: data ? JSON.parse(data) : null });
      }
    }
  })();
  return () => reader.cancel();
}

(async () => {
  await sleep(300);
  await assert.rejects(api('POST', '/login', { email: 'gin@aaapos.com', password: 'nope', device: 'device-0001' }), /Wrong email or password/);
  await assert.rejects(api('GET', '/state'), /sign in/);
  const gin = await api('POST', '/login', { email: 'GIN@aaapos.com', password: 'admin-pass-1', device: 'device-gin-1' });
  assert.equal(gin.user.role, 'admin');
  await api('POST', '/users', { name: 'Tom', email: 'tom@aaapos.com', password: 'member-pass', role: 'member' }, gin.token);
  const tom = await api('POST', '/login', { email: 'tom@aaapos.com', password: 'member-pass', device: 'device-tom-1' });
  await assert.rejects(api('POST', '/monitors', { name: 'X', url: 'x.com', interval: 60, timeout: 5 }, tom.token), /Only admins/);
  await assert.rejects(api('PUT', '/settings', { threshold: 2 }, tom.token), /Only admins/);
  await assert.rejects(api('POST', '/users', { name: 'Y', email: 'y@a.com', password: 'xxxxxxxx' }, tom.token), /Only admins/);
  console.log('ok - email login, admin creates accounts, members limited');

  const ginEvents = [];
  const tomEvents = [];
  const stopGin = await listen(gin.token, ginEvents);
  const stopTom = await listen(tom.token, tomEvents);
  await sleep(400);
  let st = await api('GET', '/state', null, gin.token);
  assert.equal(st.members.filter((m) => m.online).length, 2);
  console.log('ok - live stream and online members');

  await api('PUT', '/settings', { threshold: 2, remindAfter: 1 }, gin.token);
  const url = `http://127.0.0.1:${site.address().port}/`;
  const m = await api('POST', '/monitors', { name: 'Shop', url, interval: 60, timeout: 5 }, gin.token);
  const imp = await api('POST', '/monitors/import', { monitors: [{ name: 'Dup', url, interval: 60, timeout: 5 }, { name: 'Bad', url: 'not a url' }] }, gin.token);
  assert.deepEqual(imp, { added: 0, skipped: 2 });
  await sleep(800);
  st = await api('GET', '/state', null, tom.token);
  assert.equal(st.monitors[0].status, 'up');
  console.log('ok - monitor added by one member, seen by another');

  mode = 503;
  await api('POST', `/monitors/${m.id}/check`, null, tom.token); // check now is allowed for members
  await api('POST', `/monitors/${m.id}/check`, null, gin.token);
  await sleep(300);
  assert.ok(tomEvents.some((e) => e.ev === 'alert' && e.data.type === 'down'), 'down alert reached Tom');
  st = await api('GET', '/state', null, tom.token);
  const inc = st.monitors[0].incident;
  assert.equal(inc.ack_status, 'open');
  console.log('ok - down alert broadcast to everyone');

  await api('POST', `/incidents/${inc.id}/ack`, { status: 'investigating', note: 'Checking hosting' }, gin.token);
  await sleep(300);
  const ack = tomEvents.find((e) => e.ev === 'ack');
  assert.equal(ack.data.by, 'Gin');
  st = await api('GET', '/state', null, tom.token);
  assert.equal(st.monitors[0].incident.ack_by, 'Gin');
  assert.equal(st.monitors[0].incident.note, 'Checking hosting');
  console.log('ok - claim by Gin visible to Tom');

  mode = 200;
  await api('POST', `/monitors/${m.id}/check`, null, tom.token);
  await sleep(300);
  assert.ok(ginEvents.some((e) => e.ev === 'alert' && e.data.type === 'up'));
  await assert.rejects(api('POST', `/incidents/${inc.id}/ack`, { status: 'investigating' }, tom.token), /already back online/);
  await api('POST', `/incidents/${inc.id}/note`, { note: 'Host restarted PHP' }, gin.token);
  const events = await api('GET', `/incidents/${inc.id}/events`, null, gin.token);
  assert.deepEqual(events.map((e) => e.type), ['down', 'status', 'recovered', 'note']);
  const [closed] = await api('GET', '/incidents?status=resolved', null, tom.token);
  assert.equal(closed.note, 'Host restarted PHP');
  console.log('ok - recovery closes the incident with a timeline');

  // Removing a user signs them out everywhere and ends their live stream
  const tomId = (await api('GET', '/users', null, gin.token)).find((u) => u.email === 'tom@aaapos.com').id;
  await assert.rejects(api('DELETE', `/users/${(await api('GET', '/state', null, gin.token)).me.id}`, null, gin.token), /own account/);
  await api('DELETE', `/users/${tomId}`, null, gin.token);
  await assert.rejects(api('GET', '/state', null, tom.token), /sign in/);
  console.log('ok - removed user signed out');

  // WordPress sites live on the server, keys never returned
  const wpSite = await api('POST', '/wp/sites', { name: 'Shop WP', url: 'http://127.0.0.1:9', key: 'a'.repeat(64) }, gin.token);
  const sites = await api('GET', '/wp/sites', null, gin.token);
  assert.equal(sites[0].name, 'Shop WP');
  assert.equal(sites[0].key_enc, undefined);
  const imported = await api('POST', '/wp/import', { sites: [{ name: 'Dup', url: 'http://127.0.0.1:9', key: 'b'.repeat(64) }, { name: 'Two', url: 'http://127.0.0.1:8', key: 'c'.repeat(64) }] }, gin.token);
  assert.equal(imported.added, 1);
  const sched = await api('PUT', '/wp/schedule', { wpAuto: true, wpTime: '04:30' }, gin.token);
  assert.ok(sched.next > Date.now());
  console.log('ok - shared WordPress sites and schedule');

  await assert.rejects(api('POST', '/login', { email: 'x@x.com', password: 'bad-pass-1', device: 'device-x-0001' }).then(() => api('POST', '/login', { email: 'x@x.com', password: 'bad', device: 'device-x-0001' })), /.+/);
  for (let i = 0; i < 5; i++) await api('POST', '/login', { email: 'x@x.com', password: 'bad', device: 'device-x-0001' }).catch(() => {});
  await assert.rejects(api('POST', '/login', { email: 'gin@aaapos.com', password: 'admin-pass-1', device: 'device-x-0001' }), /Too many attempts/);
  console.log('ok - login rate limited');

  stopGin(); stopTom(); site.close();
  console.log('all passed');
  process.exit(0);
})().catch((e) => { console.error('FAILED', e); process.exit(1); });
