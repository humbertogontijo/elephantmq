/**
 * Worker Thread wrapper for sandboxing
 *
 */
import { parentPort } from 'worker_threads';
import mainBase from './main-base';

const pp = parentPort;
if (!pp) {
  throw new Error('main-worker must run inside a Worker thread');
}

mainBase(async (msg: any) => pp.postMessage(msg), pp);
