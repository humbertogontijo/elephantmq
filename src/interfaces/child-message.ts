import { ParentCommand } from '../enums/parent-command';

export interface ChildMessage {
  cmd: ParentCommand;
  requestId?: string;
  value?: unknown;
  err?: Record<string, unknown>;
}
