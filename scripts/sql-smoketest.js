'use strict';

/**
 * After `npm run build` + `migrate:test`, verifies every `emq_*_v1` name in
 * `src/sql/functions/manifest.txt` exists as a function in PostgreSQL (public).
 */
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { migrate } = require('../dist/cjs/classes/migrate.js');
const { ensureDatabase } = require('./ensure-database.js');

const root = path.join(__dirname, '..');
const manifestPath = path.join(root, 'src', 'sql', 'functions', 'manifest.txt');

const url =
  process.env.ELEPHANTMQ_TEST_PG_URL ||
  'postgres://postgres:postgres@127.0.0.1:55432/elephantmq_test';

(async () => {
  const raw = fs.readFileSync(manifestPath, 'utf8');
  const names = raw
    .split(/\r?\n/)
    .map(s => s.trim())
    .filter(Boolean);

  await ensureDatabase(url);
  const pool = new Pool({ connectionString: url });
  try {
    await migrate(pool, 'public');

    for (const name of names) {
      const {
        rows: [row],
      } = await pool.query(
        `select count(*)::int as c
         from pg_proc p
         join pg_namespace n on n.oid = p.pronamespace
         where n.nspname = 'public' and p.proname = $1`,
        [name],
      );
      const c = row?.c ?? 0;
      if (c < 1) {
        console.error(`sql-smoketest: missing function public.${name}`);
        process.exit(1);
      }
    }

    console.log(`elephantmq: sql-smoketest OK (${names.length} functions)`);
  } finally {
    await pool.end();
  }
})().catch(err => {
  console.error(err);
  process.exit(1);
});
