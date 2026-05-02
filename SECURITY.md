# Security policy

## Supported versions

elephantmq is in active development. Security fixes are issued for the latest
minor release line on `main`.

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security problems.

Instead, report privately via one of the following:

- GitHub Security Advisories: <https://github.com/humbertogontijo/elephantmq/security/advisories/new>
- Email: see the maintainer profile linked from
  <https://github.com/humbertogontijo/elephantmq>.

When reporting, include:

1. A description of the vulnerability and its impact.
2. Steps to reproduce or a proof-of-concept.
3. The affected version(s) of `elephantmq` and PostgreSQL.

We aim to acknowledge new reports within **3 business days** and to ship a fix
or mitigation within **30 days** for confirmed high-severity issues. We will
credit reporters in the release notes unless you prefer to remain anonymous.

## Sandbox and forked processors

Workers can run a processor in a separate process or worker thread (`Worker`
constructor accepts a filesystem path instead of an inline function). That model
loads and executes **your code** chosen by whatever path you pass in.

Treat sandboxed processors as **arbitrary code execution** relative to that
worker process:

- Only point workers at processor files you fully trust (same hygiene as `require()`
  of attacker-controlled paths).
- Run producers, workers that load untrusted bundles, or multi-tenant setups in
  **isolated environments** — separate containers or VMs, dedicated OS users with
  minimal filesystem access, network egress policies appropriate to your threat
  model — so a malicious or compromised processor cannot pivot to Postgres
  credentials, sibling queues, or the host system.
- The parent↔child channel carries structured IPC messages (`ChildCommand` /
  `ParentCommand`). Today payloads are constrained by usage in this package;
  if you extend IPC, validate message shapes at boundaries as defense in depth.

This is comparable to BullMQ’s sandbox model: the library executes what you configure;
the deployment boundary defines how much damage untrusted job code could do.

## Scope

In scope:

- The `elephantmq` npm package (TypeScript / JavaScript code).
- The SQL functions and migrations under `src/sql/`.

Out of scope:

- Vulnerabilities in PostgreSQL itself (report those upstream).
- Issues that require an attacker who already has direct SQL access to the
  PostgreSQL database used by elephantmq.
