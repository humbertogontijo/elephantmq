'use strict';
/**
 * Run Vitest with stdio inherited (live output, no shell/npx pipe buffering).
 * Ctrl+C sends SIGINT to the Vitest child — you keep partial output from the run.
 *
 * Uses `node ./node_modules/vitest/vitest.mjs` (not npx) so the process tree is
 * a single Node child you can interrupt reliably.
 *
 * Loads `.env.test` before spawning. Tune parallelism with `ELEPHANTMQ_TEST_PARALLEL`
 * and `ELEPHANTMQ_TEST_MAX_FORKS` (see `vitest.config.ts`).
 *
 * Usage:
 *   node scripts/run-vitest.cjs
 *     (defaults: --testTimeout 20000 --hookTimeout 20000, parallel files)
 *   ELEPHANTMQ_TEST_PARALLEL=0 node scripts/run-vitest.cjs
 *     (serial files; useful when debugging or on tight Postgres max_connections)
 *   ELEPHANTMQ_TEST_MAX_FORKS=4 ELEPHANTMQ_TEST_PG_POOL_MAX=24 node scripts/run-vitest.cjs
 *   node scripts/run-vitest.cjs run tests/job.test.ts --testTimeout 60000
 */
const { spawn } = require('child_process');
const path = require('path');

const root = path.join(__dirname, '..');
const vitestEntry = path.join(root, 'node_modules', 'vitest', 'vitest.mjs');
const configPath = path.join(root, 'vitest.config.ts');

const { loadEnvTestFile } = require('./load-env-test.cjs');

const passthrough = process.argv.slice(2);

function forwardToChild(child, sig) {
  if (child && !child.killed && child.exitCode === null) {
    try {
      child.kill(sig);
    } catch (_) {
      /* ignore */
    }
  }
}

loadEnvTestFile(root);

if (process.env.ELEPHANTMQ_TEST_PARALLEL === undefined) {
  process.env.ELEPHANTMQ_TEST_PARALLEL = '1';
}

const testParallel = process.env.ELEPHANTMQ_TEST_PARALLEL !== '0';

if (testParallel && !process.env.ELEPHANTMQ_TEST_PG_POOL_MAX) {
  process.env.ELEPHANTMQ_TEST_PG_POOL_MAX = '24';
}

let argv =
  passthrough.length > 0
    ? [...passthrough]
    : [
        'run',
        ...(testParallel ? [] : ['--no-file-parallelism']),
        '--testTimeout',
        '20000',
        '--hookTimeout',
        '20000',
      ];

const hasConfig = argv.some(
  a => a === '--config' || a.startsWith('--config='),
);
if (!hasConfig) {
  argv = ['--config', configPath, ...argv];
}

const child = spawn(process.execPath, [vitestEntry, ...argv], {
  stdio: 'inherit',
  cwd: root,
  env: process.env,
  windowsHide: true,
});

process.on('SIGINT', () => forwardToChild(child, 'SIGINT'));
process.on('SIGTERM', () => forwardToChild(child, 'SIGTERM'));

child.on('exit', (code, signal) => {
  if (signal) {
    process.stderr.write(
      `\nVitest ended with ${signal} (partial run — output above).\n`,
    );
    process.exit(signal === 'SIGINT' ? 130 : 143);
  }
  process.exit(code === null ? 1 : code);
});
