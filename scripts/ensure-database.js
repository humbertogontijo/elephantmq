'use strict';

const { Client } = require('pg');
const { parse } = require('pg-connection-string');

function quoteIdent(s) {
  return '"' + String(s).replace(/"/g, '""') + '"';
}

/**
 * Connect to the `postgres` maintenance DB using the same auth as `connectionString`,
 * only replacing the database segment (avoids pg's falsy empty-password merge bug).
 */
function adminConnectionString(connectionString) {
  const cs = String(connectionString).trim();
  return cs.replace(/\/[^/?#]+(?=[?#]|$)/, '/postgres');
}

// Connects to `postgres`, creates the target DB from the URL when missing.
async function ensureDatabase(connectionString) {
  if (!connectionString || !String(connectionString).trim()) {
    return;
  }
  const opts = parse(connectionString);
  const dbName = opts.database;
  if (!dbName || ['postgres', 'template0', 'template1'].includes(dbName)) {
    return;
  }

  const client = new Client({
    connectionString: adminConnectionString(connectionString),
  });
  try {
    await client.connect();
    const { rows } = await client.query(
      'select 1 from pg_database where datname = $1',
      [dbName],
    );
    if (rows.length > 0) {
      return;
    }
    await client.query(`create database ${quoteIdent(dbName)}`);
  } catch (err) {
    if (err && err.code === '42P04') {
      return;
    }
    throw err;
  } finally {
    await client.end().catch(() => {});
  }
}

module.exports = { ensureDatabase, adminConnectionString };
