'use strict';
const { loadEnvTestFile } = require('./load-env-test.cjs');
loadEnvTestFile();

const { Pool } = require('pg');
const { migrate } = require('../dist/cjs/classes/migrate.js');
const { ensureDatabase } = require('./ensure-database.js');

const url =
  process.env.ELEPHANTMQ_TEST_PG_URL ||
  'postgres://postgres:postgres@127.0.0.1:55432/elephantmq_test';

(async () => {
  await ensureDatabase(url);
  const pool = new Pool({ connectionString: url });

  return migrate(pool, 'public')
    .then(() => {
      console.log('elephantmq: migrations applied');
      return pool.end();
    })
    .catch(err => {
      console.error(err);
      process.exit(1);
    });
})();
