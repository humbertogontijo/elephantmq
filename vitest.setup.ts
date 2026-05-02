/**
 * Per-test-file setup. Merges `.env.test`, then ensures the target database exists.
 */
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadEnvTestFile } from './tests/load-env-test';

const require = createRequire(import.meta.url);
const { ensureDatabase } = require(
  join(dirname(fileURLToPath(import.meta.url)), 'scripts', 'ensure-database.js'),
) as { ensureDatabase: (url: string | undefined) => Promise<void> };

loadEnvTestFile();
await ensureDatabase(process.env.ELEPHANTMQ_TEST_PG_URL);
