// Server test with real HTTP: join, monitors, live events, down/claim/recover. Run: node test.js
const assert = require('node:assert');
const http = require('node:http');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');
process.env.TEAM_CODE = 'test-team-code';
process.env.PORT = '4199';
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'swsrv-'));
require('./engine').isOnline = async () => true; // the sandbox has no general internet
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
  await assert.rejects(api('POST', '/join', { name: 'X', code: 'nope', device: 'device-0001' }), /Wrong team code/);
  await assert.rejects(api('GET', '/state'), /Not signed in/);
  const gin = await api('POST', '/join', { name: 'Gin', code: 'test-team-code', device: 'device-gin-1' });
  const tom = await api('POST', '/join', { name: 'Tom', code: 'test-team-code', device: 'device-tom-1' });
  console.log('ok - join with team code');

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
  const imp = await api('POST', '/monitors/import', { monitors: [{ name: 'Dup', url, interval: 60, timeout: 5 }, { name: 'Bad', url: 'not a url' }] }, tom.token);
  assert.deepEqual(imp, { added: 0, skipped: 2 });
  await sleep(800);
  st = await api('GET', '/state', null, tom.token);
  assert.equal(st.monitors[0].status, 'up');
  console.log('ok - monitor added by one member, seen by another');

  mode = 503;
  await api('POST', `/monitors/${m.id}/check`, null, gin.token);
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
  const events = await api('GET', `/incidents/${inc.id}/events`, null, gin.token);
  assert.deepEqual(events.map((e) => e.type), ['down', 'status', 'recovered']);
  console.log('ok - recovery closes the incident with a timeline');

  // Rejoining from the same PC replaces the old token
  const gin2 = await api('POST', '/join', { name: 'Gin B', code: 'test-team-code', device: 'device-gin-1' });
  await assert.rejects(api('GET', '/state', null, gin.token), /Not signed in/);
  assert.equal((await api('GET', '/state', null, gin2.token)).me.name, 'Gin B');
  console.log('ok - one membership per device');

  for (let i = 0; i < 5; i++) await api('POST', '/join', { name: 'X', code: 'bad', device: 'device-x-0001' }).catch(() => {});
  await assert.rejects(api('POST', '/join', { name: 'X', code: 'test-team-code', device: 'device-x-0001' }), /Too many attempts/);
  console.log('ok - join rate limited');

  stopGin(); stopTom(); site.close();
  console.log('all passed');
  process.exit(0);
})().catch((e) => { console.error('FAILED', e); process.exit(1); });
