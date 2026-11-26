// Account admin from the VPS command line.
//   node --env-file=.env users.js add-admin "Name" email@example.com "password"
//   node --env-file=.env users.js reset-password email@example.com "new password"
//   node --env-file=.env users.js list
const path = require('node:path');
const fs = require('node:fs');
const db = require('./db');
const { hashPassword, validPassword } = require('./auth');

const dir = process.env.DATA_DIR || path.join(__dirname, 'data');
fs.mkdirSync(dir, { recursive: true });
db.init(path.join(dir, 'sitewatch.db'));
const [cmd, a, b, c] = process.argv.slice(2);
try {
  if (cmd === 'add-admin') {
    if (!a || !b) throw new Error('Usage: add-admin "Name" email "password"');
    db.users.add({ name: a, email: b.toLowerCase(), pass_hash: hashPassword(validPassword(c)), role: 'admin' });
    console.log(`Admin ${b} created`);
  } else if (cmd === 'reset-password') {
    const u = db.users.byEmail(String(a || '').toLowerCase());
    if (!u) throw new Error('No user with that email');
    db.users.update(u.id, { pass_hash: hashPassword(validPassword(b)) });
    db.sessions.removeUser(u.id);
    console.log(`Password reset for ${a}, signed out everywhere`);
  } else if (cmd === 'list') {
    for (const u of db.users.list()) console.log(`${u.role.padEnd(7)} ${u.email}  (${u.name})`);
  } else {
    console.log('Commands: add-admin "Name" email "password" | reset-password email "password" | list');
  }
} catch (err) {
  console.error(err.message);
  process.exitCode = 1;
}
db.close();
