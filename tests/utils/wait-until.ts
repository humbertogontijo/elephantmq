/**
 * Poll until `fn()` returns a truthy value or `timeoutMs` elapses.
 * @returns The value from `fn`, or `undefined` on timeout.
 */
export async function waitUntil<T>(
  fn: () => T | Promise<T>,
  timeoutMs = 5000,
  intervalMs = 50,
): Promise<T | undefined> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) {
      return v;
    }
    if (Date.now() >= deadline) {
      return undefined;
    }
    await new Promise(r => setTimeout(r, intervalMs));
  }
}
