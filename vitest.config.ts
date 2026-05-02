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
      exclude: [
        'src/**/*.d.ts',
        'src/**/index.ts',
        // Type-only / ambient modules — no runtime semantics to exercise.
        'src/interfaces/**',
        'src/types/**',
        'src/classes/worker/worker-listener.ts',
        // Separate CLI / worker process entry points and sandbox subprocess stack.
        'src/migrate.ts',
        'src/classes/main-base.ts',
        'src/classes/main-worker.ts',
        'src/classes/main.ts',
        'src/classes/child-pool.ts',
        'src/classes/child-processor.ts',
        'src/classes/child.ts',
        'src/classes/sandbox.ts',
        // Thin re-export shim; implementation is under `scripts/`.
        'src/classes/scripts.ts',
      ],
      thresholds: {
        lines: 70,
        /** Branch coverage is dominated by flow/getters/SQL paths; ~56% today. */
        branches: 55,
        functions: 70,
        statements: 70,
      },
    },
  },
});
