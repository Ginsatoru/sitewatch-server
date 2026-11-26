// Passwords (scrypt, built into Node) and encryption of WordPress agent keys (AES-256-GCM)
const crypto = require('node:crypto');

function hashPassword(pw) {
  const salt = crypto.randomBytes(16).toString('hex');
  return `scrypt$${salt}$${crypto.scryptSync(String(pw), salt, 64).toString('hex')}`;
}

function checkPassword(pw, stored) {
  const [kind, salt, hash] = String(stored || '').split('$');
  if (kind !== 'scrypt' || !salt || !hash) return false;
  return crypto.timingSafeEqual(crypto.scryptSync(String(pw), salt, 64), Buffer.from(hash, 'hex'));
}

function validPassword(pw) {
  if (String(pw || '').length < 8) throw new Error('Password must be at least 8 characters');
  return String(pw);
}

// Key comes from SECRET in .env; losing it means re-entering the agent keys
function cipher(secret) {
  const key = crypto.createHash('sha256').update(secret).digest();
  return {
    encrypt(text) {
      const iv = crypto.randomBytes(12);
      const c = crypto.createCipheriv('aes-256-gcm', key, iv);
      const data = Buffer.concat([c.update(text, 'utf8'), c.final()]);
      return Buffer.concat([iv, c.getAuthTag(), data]).toString('base64');
    },
    decrypt(b64) {
      const buf = Buffer.from(b64, 'base64');
      const d = crypto.createDecipheriv('aes-256-gcm', key, buf.subarray(0, 12));
      d.setAuthTag(buf.subarray(12, 28));
      return Buffer.concat([d.update(buf.subarray(28)), d.final()]).toString('utf8');
    },
  };
}

module.exports = { hashPassword, checkPassword, validPassword, cipher };
