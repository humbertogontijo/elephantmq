'use strict';

const fs = require('fs');
const path = require('path');

/**
 * Merge `.env.test` into `process.env` only for keys that are still undefined.
 */
function loadEnvTestFile(cwd = process.cwd()) {
  const p = path.join(cwd, '.env.test');
  if (!fs.existsSync(p)) {
    return;
  }
  const text = fs.readFileSync(p, 'utf8');
  for (const line of text.split(/\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const eq = trimmed.indexOf('=');
    if (eq <= 0) {
      continue;
    }
    const key = trimmed.slice(0, eq).trim();
    let val = trimmed.slice(eq + 1).trim();
    if (
      (val.startsWith('"') && val.endsWith('"')) ||
      (val.startsWith("'") && val.endsWith("'"))
    ) {
      val = val.slice(1, -1);
    }
    if (key && process.env[key] === undefined) {
      process.env[key] = val;
    }
  }
}

module.exports = { loadEnvTestFile };
