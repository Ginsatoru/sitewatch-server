// Input validation for everything the desktop apps send
const METHODS = ['GET', 'HEAD', 'POST'];
const STATUS_RE = /^\s*\d{3}(\s*-\s*\d{3})?(\s*,\s*\d{3}(\s*-\s*\d{3})?)*\s*$/;

const int = (v, min, max, label) => {
  const x = Number(v);
  if (!Number.isInteger(x) || x < min || x > max) throw new Error(`${label} must be between ${min} and ${max}`);
  return x;
};

function cleanMonitor(m = {}) {
  let url = String(m.url || '').trim();
  if (!/^https?:\/\//i.test(url)) url = `https://${url}`;
  let parsed;
  try { parsed = new URL(url); } catch { throw new Error('Enter a valid website URL'); }
  if (!['http:', 'https:'].includes(parsed.protocol) || (!parsed.hostname.includes('.') && parsed.hostname !== 'localhost')) {
    throw new Error('Enter a valid http or https URL');
  }
  const name = String(m.name || '').trim() || parsed.hostname.replace(/^www\./, '');
  if (name.length > 100) throw new Error('Name must be 100 characters or fewer');
  const method = String(m.method || 'GET').toUpperCase();
  if (!METHODS.includes(method)) throw new Error('Unsupported request method');
  const expected = String(m.expected || '200-299').replace(/\s+/g, '');
  if (!STATUS_RE.test(expected)) throw new Error('Expected status codes look like 200-299 or 200,301');
  return {
    name, url: parsed.href, method, expected,
    interval: int(m.interval, 10, 86400, 'Check interval'),
    timeout: int(m.timeout, 1, 120, 'Timeout'),
  };
}

function cleanSettings(p = {}) {
  const out = {};
  if ('interval' in p) out.interval = int(p.interval, 10, 86400, 'Default interval');
  if ('timeout' in p) out.timeout = int(p.timeout, 1, 120, 'Timeout');
  if ('threshold' in p) out.threshold = int(p.threshold, 1, 20, 'Failure threshold');
  if ('retention' in p) out.retention = int(p.retention, 1, 365, 'History retention');
  if ('remindAfter' in p) out.remindAfter = int(p.remindAfter, 1, 240, 'Reminder time');
  return out;
}

const ACK = ['open', 'investigating', 'contacted'];

function cleanName(v) {
  const name = String(v || '').trim().replace(/\s+/g, ' ');
  if (!name || name.length > 40) throw new Error('Enter your name (up to 40 characters)');
  return name;
}

function cleanAck(p = {}) {
  if (!ACK.includes(p.status)) throw new Error('Unknown status');
  const note = String(p.note || '').trim();
  if (note.length > 300) throw new Error('Note must be 300 characters or fewer');
  return { status: p.status, note: note || null };
}

module.exports = { cleanMonitor, cleanSettings, cleanName, cleanAck, int };
