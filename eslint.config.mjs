import { defineConfig, globalIgnores } from 'eslint/config';
import tsdoc from 'eslint-plugin-tsdoc';
import promise from 'eslint-plugin-promise';
import prettier from 'eslint-plugin-prettier';
import typescriptEslintEslintPlugin from '@typescript-eslint/eslint-plugin';
import globals from 'globals';
import tsParser from '@typescript-eslint/parser';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import js from '@eslint/js';
import { FlatCompat } from '@eslint/eslintrc';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
  baseDirectory: __dirname,
  recommendedConfig: js.configs.recommended,
  allConfig: js.configs.all,
});

export default defineConfig([
  globalIgnores([
    '**/node_modules',
    'dist',
    'ref',
    'coverage',
  ]),
  {
    extends: compat.extends(
      'prettier',
      'eslint:recommended',
      'plugin:@typescript-eslint/eslint-recommended',
      'plugin:@typescript-eslint/recommended',
    ),

    plugins: {
      tsdoc,
      promise,
      prettier,
      '@typescript-eslint': typescriptEslintEslintPlugin,
    },

    languageOptions: {
      globals: {
        ...globals.node,
      },
      parser: tsParser,
    },

    rules: {
      'no-restricted-imports': [
        'error',
        {
          patterns: ['src/*'],
        },
      ],
      '@typescript-eslint/no-empty-interface': [
        'error',
        {
          allowSingleExtends: false,
        },
      ],
      '@typescript-eslint/no-require-imports': 0,
      '@typescript-eslint/no-unused-vars': 0,
      '@typescript-eslint/no-unused-expressions': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-empty-object-type': 1,
      '@typescript-eslint/no-unsafe-function-type': 1,
      '@typescript-eslint/no-var-requires': 0,
      '@typescript-eslint/no-this-alias': 0,
      'space-before-function-paren': [
        'error',
        {
          anonymous: 'ignore',
          named: 'never',
          asyncArrow: 'always',
        },
      ],
      'arrow-parens': [
        2,
        'as-needed',
        {
          requireForBlockBody: false,
        },
      ],
      curly: 'error',
      'no-async-promise-executor': 0,
      semi: 2,
      'no-bitwise': 0,
      'eol-last': 2,
      'prefer-const': 1,
      'max-len': [
        'error',
        {
          code: 120,
          ignorePattern: '^import\\s.+\\sfrom\\s.+;$',
        },
      ],
      'tsdoc/syntax': 'error',
    },
  },
  {
    files: ['src/**/*.ts'],
    rules: {
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],
    },
  },
  {
    files: ['src/interfaces/**/*.ts', 'src/types/**/*.ts'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'warn',
    },
  },
]);
