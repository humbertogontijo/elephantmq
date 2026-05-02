import type { PgQueryable } from '../interfaces';
import { escapeSchema } from './queue-identity';

export { escapeSchema };

/**
 * Qualified identifier for an EMQ SQL function in a schema, e.g. "public".emq_foo_v1
 */
export function emqQualifiedFn(schema: string, fnName: string): string {
  return `${escapeSchema(schema)}.${fnName}`;
}

export async function queryOne<T extends Record<string, unknown>>(
  client: PgQueryable,
  text: string,
  values: unknown[],
): Promise<T | undefined> {
  const { rows } = await client.query<T>(text, values);
  return rows[0];
}
