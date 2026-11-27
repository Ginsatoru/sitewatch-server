// Finds and caches a website's logo for the cards: WordPress custom logo, then other "logo" images,
// then the apple touch icon or largest favicon. Stored as a data URI so apps need no extra requests.
const db = require('./db');

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130 Safari/537.36 SiteWatch';
const DAY = 864e5;
const pending = new Map();

function init() {
  db.raw().exec('CREATE TABLE IF NOT EXISTS logos (host TEXT PRIMARY KEY, data TEXT, fetched_at INTEGER NOT NULL)');
}

const attr = (tag, name) => (new RegExp(`\\b${name}\\s*=\\s*["']([^"']+)["']`, 'i').exec(tag) || [])[1];

// Logo candidates from the home page, best first
function candidates(html, base) {
  const out = [];
  const add = (src) => { try { if (src && !src.startsWith('data:')) out.push(new URL(src, base).href); } catch { /* bad url */ } };
  const imgs = html.match(/<img\b[^>]*>/gi) || [];
  for (const tag of imgs) if (/class\s*=\s*["'][^"']*custom-logo/i.test(tag)) add(attr(tag, 'src'));
  for (const tag of imgs.slice(0, 40)) {
    if (/logo/i.test(`${attr(tag, 'class') || ''} ${attr(tag, 'id') || ''} ${attr(tag, 'alt') || ''} ${attr(tag, 'src') || ''}`)) add(attr(tag, 'src') || attr(tag, 'data-src'));
  }
  const links = html.match(/<link\b[^>]*>/gi) || [];
  for (const tag of links) if (/rel\s*=\s*["'][^"']*apple-touch-icon/i.test(tag)) add(attr(tag, 'href'));
  const icons = links.filter((t) => /rel\s*=\s*["'](?:shortcut )?icon["']/i.test(t))
    .sort((a, b) => parseInt(attr(b, 'sizes') || '0', 10) - parseInt(attr(a, 'sizes') || '0', 10));
  for (const tag of icons) add(attr(tag, 'href'));
  add('/favicon.ico');
  return [...new Set(out)];
}

async function download(url) {
  const res = await fetch(url, { headers: { 'User-Agent': UA }, signal: AbortSignal.timeout(10000), redirect: 'follow' });
  const type = (res.headers.get('content-type') || '').split(';')[0].trim();
  if (!res.ok || !/^image\//.test(type)) return null;
  const buf = Buffer.from(await res.arrayBuffer());
  if (!buf.length || buf.length > 1.5 * 1024 * 1024) return null;
  return `data:${type};base64,${buf.toString('base64')}`;
}

async function find(siteUrl) {
  const res = await fetch(siteUrl, { headers: { 'User-Agent': UA }, signal: AbortSignal.timeout(15000), redirect: 'follow' });
  const html = (await res.text()).slice(0, 400000);
  for (const url of candidates(html, res.url || siteUrl)) {
    try { const data = await download(url); if (data) return data; } catch { /* try the next one */ }
  }
  return null;
}

// Cached 7 days when found, 1 day when not, so a missing logo is retried now and then
async function get(siteUrl) {
  const host = new URL(siteUrl).host;
  const row = db.raw().prepare('SELECT data, fetched_at FROM logos WHERE host = ?').get(host);
  if (row && Date.now() - row.fetched_at < (row.data ? 7 : 1) * DAY) return row.data;
  if (!pending.has(host)) {
    pending.set(host, find(siteUrl).catch(() => null).then((data) => {
      db.raw().prepare('INSERT INTO logos (host, data, fetched_at) VALUES (?, ?, ?) ON CONFLICT(host) DO UPDATE SET data = excluded.data, fetched_at = excluded.fetched_at')
        .run(host, data, Date.now());
      pending.delete(host);
      return data;
    }));
  }
  return pending.get(host);
}

module.exports = { init, get, candidates };
