import type { WorkerOptions } from '../src/interfaces';
import type { Processor } from '../src/types/processor';
import { Queue, Worker } from '../src';
import { delay, removeAllQueueData } from '../src/utils';
import { getTestPoolAsEmqClient } from './utils';
import { pgConnectionOpts, TEST_PREFIX } from './test_context';

export { delay };

export async function removeAllQueueDataPg(
  schema: string,
  queueName: string,
  prefix = TEST_PREFIX,
): Promise<void | boolean> {
  return removeAllQueueData(
    getTestPoolAsEmqClient(),
    queueName,
    prefix,
    schema,
  );
}

export function newQueue(
  queueName: string,
  schema: string,
  extra: Record<string, unknown> = {},
): Queue {
  return new Queue(queueName, { ...pgConnectionOpts(schema), ...extra });
}

export function newWorker<DataType = any, ReturnType = any, Name extends string = string>(
  queueName: string,
  processor: Processor<DataType, ReturnType, Name>,
  schema: string,
  extra: WorkerOptions = {},
): Worker<DataType, ReturnType, Name> {
  return new Worker(queueName, processor, {
    ...pgConnectionOpts(schema),
    ...extra,
  });
}
