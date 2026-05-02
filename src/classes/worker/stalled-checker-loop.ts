export interface StallLoopCtx {
  stalledIntervalMs: number;
  shouldStop: () => boolean;
  onTick: () => Promise<void>;
  setStopper: (stopper?: () => void) => void;
}

/**
 * Repeatedly runs `onTick` (moving stalled jobs) then sleeps until interval
 * expires or stopper clears the timer (pause/close paths).
 */
export async function runStalledCheckerLoop(
  ctx: StallLoopCtx,
): Promise<void> {
  while (!ctx.shouldStop()) {
    await ctx.onTick();

    await new Promise<void>(resolve => {
      const timeout = setTimeout(resolve, ctx.stalledIntervalMs);
      ctx.setStopper(() => {
        clearTimeout(timeout);
        resolve();
      });
    });
  }
}
