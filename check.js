// HTTP(S) checks and local internet connectivity detection
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) SiteWatch/1.0';

// "200-299, 301" -> true for any status inside a listed code or range
function matchStatus(code, expected = '200-299') {
  return expected.split(',').some((part) => {
    const [a, b = a] = part.split('-').map((s) => Number(s.trim()));
    return code >= a && code <= b;
  });
}

function describeError(err, timeout) {
  if (err.name === 'TimeoutError' || err.name === 'AbortError') return `Timed out after ${timeout}s`;
  const c = err.cause || {};
  const code = c.code || err.code || '';
  if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') return 'DNS lookup failed';
  if (code === 'ECONNREFUSED') return 'Connection refused';
  if (code === 'ECONNRESET' || code === 'UND_ERR_SOCKET') return 'Connection reset';
  if (code === 'ETIMEDOUT' || code === 'UND_ERR_CONNECT_TIMEOUT') return 'Connection timed out';
  if (code === 'EHOSTUNREACH' || code === 'ENETUNREACH') return 'Host unreachable';
  if (/CERT|SSL|TLS|SELF_SIGNED|UNABLE_TO_VERIFY|DEPTH_ZERO/i.test(code)) return `SSL error: ${c.message || code}`;
  return c.message || err.message || 'Request failed';
}

async function checkUrl({ url, method = 'GET', timeout = 10, expected }) {
  const started = performance.now();
  try {
    const res = await fetch(url, {
      method,
      redirect: 'follow',
      cache: 'no-store',
      signal: AbortSignal.timeout(timeout * 1000),
      headers: { 'User-Agent': UA, Accept: '*/*' },
    });
    const ms = Math.round(performance.now() - started);
    res.body?.cancel().catch(() => {}); // headers are enough, don't download the page
    const ok = matchStatus(res.status, expected);
    const error = ok ? null : `HTTP ${res.status}${res.statusText ? ' ' + res.statusText : ''}`;
    return { ok, code: res.status, ms, error };
  } catch (err) {
    return { ok: false, code: null, ms: null, error: describeError(err, timeout) };
  }
}

// Online if any well-known endpoint answers. Cached briefly so a burst of failures shares one probe.
const PROBES = [
  'https://www.google.com/generate_204',
  'https://1.1.1.1/cdn-cgi/trace',
  'https://www.msftconnecttest.com/connecttest.txt',
];
let cache = { at: 0, value: true, pending: null };

function isOnline() {
  if (Date.now() - cache.at < 20000) return Promise.resolve(cache.value);
  if (cache.pending) return cache.pending;
  const probe = (u) => fetch(u, { method: 'HEAD', signal: AbortSignal.timeout(5000) })
    .then(() => true, () => Promise.reject(new Error('fail')));
  cache.pending = Promise.any(PROBES.map(probe)).then(() => true, () => false).then((value) => {
    cache = { at: Date.now(), value, pending: null };
    return value;
  });
  return cache.pending;
}

module.exports = { checkUrl, isOnline, matchStatus };
