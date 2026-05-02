import { ChildCommand, ParentCommand } from '../enums';
import {
  ChildMessage,
  DependenciesOpts,
  MoveToWaitingChildrenOpts,
} from '../interfaces';
import { JobProgress } from '../types';
import { Child } from './child';
import { ChildPool } from './child-pool';
import { Job } from './job';

const sandbox = <T, R, N extends string>(
  processFile: any,
  childPool: ChildPool,
) => {
  return async function process(
    job: Job<T, R, N>,
    token?: string,
    signal?: AbortSignal,
  ): Promise<R> {
    let child: Child | undefined;
    let dispatch: ((msg: ChildMessage) => void) | undefined;
    let exitHandler: any;
    let abortHandler: (() => void) | undefined;
    try {
      const done: Promise<R> = new Promise((resolve, reject) => {
        const initChild = async () => {
          try {
            exitHandler = (exitCode: any, signal: any) => {
              reject(
                new Error(
                  'Unexpected exit code: ' + exitCode + ' signal: ' + signal,
                ),
              );
            };

            const ch = await childPool.retain(processFile);
            child = ch;
            ch.on('exit', exitHandler);

            // Side-effect messages (Progress / Log / Update) need to fully
            // commit before `Completed` resolves, otherwise the worker emits
            // 'completed' before subscribers receive the last 'progress'
            // event. Serialize those onto a chain so terminal messages land
            // strictly after preceding side-effects. Request/response RPCs
            // (Get* / MoveToWaitingChildren) run in parallel: the child is
            // blocked awaiting their response, and serializing them with
            // Failed would deadlock when a slow handler is interrupted by
            // a child-side timeout.
            let sideEffects: Promise<unknown> = Promise.resolve();
            const enqueue = (work: () => Promise<unknown>) => {
              const next = sideEffects.then(work).catch(reject);
              sideEffects = next;
              return next;
            };

            const handleMessage = async (msg: ChildMessage) => {
              try {
                switch (msg.cmd) {
                  case ParentCommand.Completed:
                    enqueue(async () => {
                      resolve(msg.value as R);
                    });
                    break;
                  case ParentCommand.Failed:
                  case ParentCommand.Error: {
                    const err = new Error();
                    Object.assign(err, msg.value);
                    enqueue(async () => {
                      reject(err);
                    });
                    break;
                  }
                  case ParentCommand.Progress:
                    enqueue(() =>
                      job.updateProgress(msg.value as JobProgress),
                    );
                    break;
                  case ParentCommand.Log:
                    enqueue(() => job.log(msg.value as string));
                    break;
                  case ParentCommand.MoveToDelayed:
                    enqueue(() => {
                      const mv = msg.value as {
                        timestamp: number;
                        token?: string;
                      };
                      return job.moveToDelayed(mv.timestamp, mv.token);
                    });
                    break;
                  case ParentCommand.MoveToWait:
                    enqueue(() =>
                      job.moveToWait(
                        (msg.value as { token?: string })?.token,
                      ),
                    );
                    break;
                  case ParentCommand.MoveToWaitingChildren:
                    {
                      const moveOpts = msg.value as {
                        token?: string;
                        opts?: MoveToWaitingChildrenOpts;
                      };
                      const value = await job.moveToWaitingChildren(
                        moveOpts?.token ?? '',
                        moveOpts.opts ?? {},
                      );
                      ch.send({
                        requestId: msg.requestId,
                        cmd: ChildCommand.MoveToWaitingChildrenResponse,
                        value,
                      });
                    }
                    break;
                  case ParentCommand.Update:
                    enqueue(() => job.updateData(msg.value as T));
                    break;
                  case ParentCommand.GetChildrenValues:
                    {
                      const value = await job.getChildrenValues();
                      ch.send({
                        requestId: msg.requestId,
                        cmd: ChildCommand.GetChildrenValuesResponse,
                        value,
                      });
                    }
                    break;
                  case ParentCommand.GetIgnoredChildrenFailures:
                    {
                      const value = await job.getIgnoredChildrenFailures();
                      ch.send({
                        requestId: msg.requestId,
                        cmd: ChildCommand.GetIgnoredChildrenFailuresResponse,
                        value,
                      });
                    }
                    break;
                  case ParentCommand.GetDependenciesCount:
                    {
                      const value = await job.getDependenciesCount(
                        msg.value as {
                          failed?: boolean;
                          ignored?: boolean;
                          processed?: boolean;
                          unprocessed?: boolean;
                        },
                      );
                      ch.send({
                        requestId: msg.requestId,
                        cmd: ChildCommand.GetDependenciesCountResponse,
                        value,
                      });
                    }
                    break;
                  case ParentCommand.GetDependencies:
                    {
                      const value = await job.getDependencies(
                        msg.value as DependenciesOpts,
                      );
                      ch.send({
                        requestId: msg.requestId,
                        cmd: ChildCommand.GetDependenciesResponse,
                        value,
                      });
                    }
                    break;
                }
              } catch (err) {
                reject(err);
              }
            };

            dispatch = handleMessage;

            ch.on('message', dispatch);

            ch.send({
              cmd: ChildCommand.Start,
              job: job.asJSONSandbox(),
              token,
            });

            if (signal) {
              abortHandler = () => {
                try {
                  ch.send({
                    cmd: ChildCommand.Cancel,
                    value: signal.reason,
                  });
                } catch {
                  // Child process may have already exited
                }
              };

              if (signal.aborted) {
                abortHandler();
              } else {
                signal.addEventListener('abort', abortHandler, { once: true });
              }
            }
          } catch (error) {
            reject(error);
          }
        };
        initChild();
      });

      await done;
      return done;
    } finally {
      // Note: There is a potential race where the signal is aborted between
      // `await done` and this cleanup. This is safe because:
      // 1. abortHandler has a try-catch for child process already exited
      // 2. The listener is added with `once: true`, so it fires at most once
      // 3. removeEventListener here is defensive cleanup only
      if (signal && abortHandler) {
        signal.removeEventListener('abort', abortHandler);
      }
      if (child) {
        if (dispatch) {
          child.off('message', dispatch);
        }
        child.off('exit', exitHandler);
        if (child.exitCode === null && child.signalCode === null) {
          childPool.release(child);
        }
      }
    }
  };
};

export default sandbox;
