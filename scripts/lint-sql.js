'use strict';

const { spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const SQL_ROOT = path.join(ROOT, 'src', 'sql');
const SQUAWK_CONFIG = path.join(ROOT, '.squawk.toml');

function* listSqlFiles(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const ent of entries) {
    const full = path.join(dir, ent.name);
    if (ent.isDirectory()) {
      yield* listSqlFiles(full);
    } else if (ent.isFile() && ent.name.endsWith('.sql')) {
      yield full;
    }
  }
}

function main() {
  const squawk = path.join(ROOT, 'node_modules', '.bin', 'squawk');
  if (!fs.existsSync(squawk)) {
    console.error('elephantmq: squawk not found; run npm install');
    process.exit(1);
  }

  const files = [...listSqlFiles(SQL_ROOT)].sort();
  if (files.length === 0) {
    console.error('elephantmq: no SQL files under', SQL_ROOT);
    process.exit(1);
  }

  let failed = false;
  for (const file of files) {
    let body = fs.readFileSync(file, 'utf8');
    body = body.replace(/:EMQ_SCHEMA/g, 'public');
    const rel = path.relative(ROOT, file);
    const r = spawnSync(
      squawk,
      ['--stdin-filepath', rel, '--config', SQUAWK_CONFIG, '--reporter', 'gcc'],
      {
        cwd: ROOT,
        input: body,
        encoding: 'utf8',
        maxBuffer: 10 * 1024 * 1024,
      },
    );
    if (r.error) {
      console.error(r.error);
      process.exit(1);
    }
    if (r.status !== 0 && r.status !== null) {
      failed = true;
    }
    if (r.stdout) {
      process.stdout.write(r.stdout);
    }
    if (r.stderr) {
      process.stderr.write(r.stderr);
    }
  }

  process.exit(failed ? 1 : 0);
}

main();
