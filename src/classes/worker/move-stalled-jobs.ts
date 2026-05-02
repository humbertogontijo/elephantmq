import { SpanKind, TelemetryAttributes } from '../../enums';
import type { Span, WorkerOptions } from '../../interfaces';
import type { Scripts } from '../scripts';

export interface MoveStalledJobsHost {
  readonly id: string;
  readonly name: string;
  readonly opts: WorkerOptions;

  trace<T>(
    spanKind: SpanKind,
    operation: string,
    destination: string,
    callback: (
      span?: Span,
      dstPropagationMetadata?: string,
    ) => Promise<T> | T,
    srcPropagationMetadata?: string,
  ): Promise<unknown>;

  emit(ev: string, ...args: unknown[]): boolean;
}

export async function execMoveStalledJobsToWait(
  host: MoveStalledJobsHost,
): Promise<void> {
  const scripts = (host as unknown as { scripts: Scripts }).scripts;

  await host.trace<void>(
    SpanKind.INTERNAL,
    'moveStalledJobsToWait',
    host.name,
    async span => {
      const [stalled, failed] = await scripts.moveStalledJobsToWait();
      if (process.env.EMQ_DBG_WORKER) {
        try {
          require('fs').appendFileSync(
            '/tmp/emq-dbg.log',
            `[worker ${host.id}] stalled recovered=${JSON.stringify(stalled)} failed=${JSON.stringify(failed)}\n`,
          );
        } catch {
          /* ignore */
        }
      }

      span?.setAttributes({
        [TelemetryAttributes.WorkerId]: host.id,
        [TelemetryAttributes.WorkerName]: host.opts.name,
        [TelemetryAttributes.WorkerStalledJobs]: stalled,
      });

      stalled.forEach((jobId: string) => {
        span?.addEvent('job stalled', {
          [TelemetryAttributes.JobId]: jobId,
        });
        host.emit('stalled', jobId, 'active');
      });

      void failed;
    },
  );
}
