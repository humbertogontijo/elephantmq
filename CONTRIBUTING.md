# Contributing to elephantmq

Thanks for your interest in elephantmq. This document covers the things you need to know to get a patch landed quickly.

## Code of conduct

This project follows the [Contributor Covenant](./CODE_OF_CONDUCT.md). By participating you agree to abide by it.

## Reporting issues

- For **bugs**, open a GitHub issue using the *Bug report* template.
- For **feature requests**, open an issue using the *Feature request* template.
- For **suspected security vulnerabilities**, please follow [SECURITY.md](./SECURITY.md) instead of opening a public issue.

When filing a bug, include:

1. The `elephantmq` version (`npm ls elephantmq`).
2. The PostgreSQL version (`SELECT version();`).
3. The Node.js version (`node -v`).
4. A minimal reproduction. Tests in `tests/integration` are great prior art for this.

## Development setup

This repository uses **npm** (see `package-lock.json`) and CI runs `npm ci`.

```bash
git clone https://github.com/humbertogontijo/elephantmq.git
cd elephantmq
npm install
cp .env.test.example .env.test    # then edit if your Postgres is not on localhost
npm run migrate:test
npm test
```

You will need a local PostgreSQL 14+ instance reachable via `ELEPHANTMQ_TEST_PG_URL` (see `.env.test.example`). The test suite isolates each test file in its own schema, so it's safe to point at a development database.

## Making a change

1. **Open an issue first** for anything more than a typo or trivial bug fix. We want to make sure the work is something we'd accept before you spend time on it.
2. Branch from `main`. Use a short prefix: `fix/`, `feat/`, `docs/`, `chore/`.
3. Run `npm run lint` and `npm test` locally.
4. Add tests for any new behaviour. Unit tests live under `tests/unit/`; integration tests (anything that needs Postgres) live under `tests/integration/`.
5. Keep public API changes documented in `CHANGELOG.md` under `## [Unreleased]`.

## SQL changes

elephantmq's queue logic lives in PostgreSQL functions (`src/sql/functions/`).

- **Schema (DDL) changes** go in a *new* migration file under `src/sql/migrations/` (`NNNN_<short_name>.sql`). Migrations are forward-only.
- **Function changes** edit the existing file under `src/sql/functions/`. All function definitions are reapplied on every `migrate()` run, so there is no "function migration" file to add.
- Run `npm run sql-smoketest` to apply the full migration set against a clean schema and exercise a representative call path.

When you add SQL, also run the SQL linter:

```bash
npx squawk path/to/your/file.sql
```

## Pull requests

- Keep PRs focused. One logical change per PR makes review tractable.
- Reference the issue you're closing (`Closes #123`) in the description.
- We squash-merge PRs; the squash commit message is taken from the PR title and body, so make them count.
- CI must be green before review.

## Releases

Releases are cut from `main` by maintainers:

1. Bump `version` in `package.json`.
2. Move the `## [Unreleased]` section in `CHANGELOG.md` under the new version with today's date.
3. Tag `vX.Y.Z` and push the tag — the release workflow publishes to npm with provenance.

## License

By contributing you agree that your contributions will be licensed under the [MIT License](./LICENSE).
