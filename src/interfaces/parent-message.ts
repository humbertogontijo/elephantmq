import { ChildCommand } from '../enums/child-command';
import type { JobJsonSandbox } from '../types/job-json-sandbox';

export interface ParentMessage {
  cmd: ChildCommand;
  value?: unknown;
  err?: Error;
  job?: JobJsonSandbox;
  token?: string;
}
