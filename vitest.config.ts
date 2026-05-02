import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/unit/**/*.test.ts', 'tests/integration/**/*.test.ts'],
    exclude: ['node_modules/**'],
    testTimeout: 20000,
    hookTimeout: 20000,
    setupFiles: ['./vitest.setup.ts'],
    fileParallelism: process.env.ELEPHANTMQ_TEST_PARALLEL !== '0',
    sequence: {
      concurrent: false,
    },
    reporters: ['verbose'],
    globals: false,
    pool: 'forks',
    // Override with ELEPHANTMQ_TEST_MAX_FORKS when parallel file runs exhaust Postgres connections.
    maxWorkers:
      Number.parseInt(process.env.ELEPHANTMQ_TEST_MAX_FORKS || '2', 10) || 2,
    isolate: true,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      include: ['src/**/*.ts'],
      exclude: ['src/**/*.d.ts', 'src/**/index.ts'],
      thresholds: {
        lines: 60,
        branches: 50,
        functions: 55,
        statements: 60,
      },
    },
  },
});
