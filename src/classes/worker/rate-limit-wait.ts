export interface RateLimitWaitCtx {
  getLimitUntil: () => number;
  computeRateLimitDelay: (milliseconds: number) => number;
  delay: (
    milliseconds?: number,
    abortController?: AbortController,
  ) => Promise<void>;
  resetAbortDelayForRateLimitSleep: () => AbortController;
  setDrained: (v: boolean) => void;
  clearRateLimitExpiry: () => void;
}

/**
 * Honors `limitUntil` by delaying (with abort wired to NOTIFY-driven wakeups).
 */
export async function waitForWorkerRateLimit(
  ctx: RateLimitWaitCtx,
): Promise<void> {
  const limitUntil = ctx.getLimitUntil();
  if (limitUntil > Date.now()) {
    const abortController = ctx.resetAbortDelayForRateLimitSleep();
    await ctx.delay(
      ctx.computeRateLimitDelay(limitUntil - Date.now()),
      abortController,
    );
    ctx.setDrained(false);
    ctx.clearRateLimitExpiry();
  }
}
