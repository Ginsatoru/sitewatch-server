// Signed requests to the SiteWatch Agent plugin (REST API) and its rescue endpoint
const crypto = require('node:crypto');

const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clean = (s) => String(s).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 200);

class AgentError extends Error {
  constructor(message, code) { super(clean(message)); this.code = code; }
}

function describe(err) {
  if (err instanceof AgentError) return err;
  if (err.name === 'TimeoutError' || err.name === 'AbortError') return new AgentError('Request timed out', 'timeout');
  const c = err.cause || {};
  if (c.code === 'ENOTFOUND' || c.code === 'EAI_AGAIN') return new AgentError('DNS lookup failed', 'network');
  if (/CERT|SSL|TLS/i.test(c.code || '')) return new AgentError(`SSL error: ${c.message || c.code}`, 'network');
  return new AgentError(c.message || err.message || 'Connection failed', 'network');
}

// site: { url, key, rescue? }. Rescue calls work even when WordPress is broken or in maintenance.
async function request(site, action, { method = 'GET', body, query = '', timeout = 30000, rescue = false } = {}) {
  const json = body ? JSON.stringify(body) : '';
  const route = rescue ? `rescue:${action}` : `/sitewatch/v1/${action}`;
  const ts = String(Math.floor(Date.now() / 1000));
  const nonce = crypto.randomBytes(16).toString('hex');
  const signature = crypto.createHmac('sha256', site.key).update([ts, nonce, method, route, sha256(json)].join('\n')).digest('hex');
  const base = site.url.replace(/\/+$/, '');
  const url = rescue
    ? `${site.rescue || `${base}/wp-content/plugins/sitewatch-agent/rescue.php`}?action=${action}&_sw=${nonce}`
    : `${base}/?rest_route=/sitewatch/v1/${action}${query}&_sw=${nonce}`;

  let res;
  try {
    res = await fetch(url, {
      method,
      body: json || undefined,
      signal: AbortSignal.timeout(timeout),
      headers: {
        'Content-Type': 'application/json', 'User-Agent': 'SiteWatch/1.0', 'Cache-Control': 'no-cache', Pragma: 'no-cache',
        'X-SiteWatch-Time': ts, 'X-SiteWatch-Nonce': nonce, 'X-SiteWatch-Signature': signature,
      },
    });
  } catch (err) {
    throw describe(err);
  }
  const text = await res.text();
  let data = null;
  try { data = JSON.parse(text); } catch { /* not JSON */ }
  if (res.status === 503 && /maintenance|briefly unavailable/i.test(text)) throw new AgentError('Site is in maintenance mode', 'maintenance');
  if (res.status === 401 || res.status === 403) throw new AgentError(data?.message || data?.error || 'Agent key rejected', 'auth');
  if (res.status === 404 && !rescue && data?.code === 'rest_no_route') throw new AgentError('SiteWatch Agent plugin is not active', 'agent');
  if (!res.ok || !data) throw new AgentError(data?.message || data?.error || `HTTP ${res.status}`, 'http');
  return data;
}

// Read-only calls are retried on connection problems
async function withRetry(fn, tries = 3) {
  for (let i = 1; ; i++) {
    try { return await fn(); } catch (err) {
      if (i >= tries || !['network', 'timeout'].includes(err.code)) throw err;
      await sleep(2000 * i);
    }
  }
}

// Public check (no key): is the agent plugin installed and active on this URL?
async function ping(url) {
  try {
    const res = await fetch(`${url.replace(/\/+$/, '')}/?rest_route=/sitewatch/v1/ping&_sw=${Date.now()}`, {
      signal: AbortSignal.timeout(15000), headers: { 'User-Agent': 'SiteWatch/1.0', 'Cache-Control': 'no-cache', Pragma: 'no-cache' },
    });
    return res.ok && (await res.text()).includes('"sitewatch":"ok"');
  } catch (err) {
    throw describe(err);
  }
}

module.exports = {
  AgentError,
  ping,
  status: (site, refresh = false) => withRetry(() => request(site, 'status', { query: refresh ? '&refresh=1' : '', timeout: refresh ? 90000 : 30000 })),
  update: (site, body, timeout = 900000) => request(site, 'update', { method: 'POST', body, timeout }),
  resolve: (site, job) => request(site, 'resolve', { method: 'POST', body: { job } }),
  rescueStatus: (site) => withRetry(() => request(site, 'status', { rescue: true })),
  rescueResolve: (site, job) => request(site, 'resolve', { method: 'POST', body: { job }, rescue: true }),
  progress: (site, job) => request(site, 'progress', { method: 'POST', body: { job }, rescue: true, timeout: 8000 }),
  clearMaintenance: (site, timeout) => request(site, 'clear-maintenance', { method: 'POST', body: { timeout }, rescue: true }),
};