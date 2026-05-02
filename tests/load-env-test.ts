import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const { loadEnvTestFile: loadFromScripts } = require(
  join(dirname(fileURLToPath(import.meta.url)), '..', 'scripts', 'load-env-test.cjs'),
) as { loadEnvTestFile: (cwd?: string) => void };

/**
 * Merge `.env.test` into `process.env` only for keys that are still undefined.
 */
export function loadEnvTestFile(): void {
  loadFromScripts();
}
