/**
 * Wrapper for sandboxing.
 *
 */
import { ChildProcessor } from './child-processor';
import { ParentCommand, ChildCommand } from '../enums';
import { errorToJSON, toString } from '../utils';
import { ParentMessage, ChildMessage, Receiver } from '../interfaces';

export default (
  send: (msg: ChildMessage) => Promise<void>,
  receiver: Receiver,
) => {
  const childProcessor = new ChildProcessor(send, receiver);

  receiver?.on('message', async (msg: unknown) => {
    try {
      const m = msg as ParentMessage;
      switch (m.cmd as ChildCommand) {
        case ChildCommand.Init:
          await childProcessor.init(m.value as string);
          break;
        case ChildCommand.Start:
          await childProcessor.start(m.job!, m.token);
          break;
        case ChildCommand.Stop:
          break;
        case ChildCommand.Cancel:
          childProcessor.cancel(m.value as string | undefined);
          break;
      }
    } catch {
      console.error('Error handling child message');
    }
  });

  process.on('SIGTERM', () => childProcessor.waitForCurrentJobAndExit());
  process.on('SIGINT', () => childProcessor.waitForCurrentJobAndExit());

  process.on('uncaughtException', async (err: any) => {
    if (typeof err !== 'object') {
      err = new Error(toString(err));
    }

    await send({
      cmd: ParentCommand.Failed,
      value: errorToJSON(err),
    });

    // An uncaughException leaves this process in a potentially undetermined state so
    // we must exit
    process.exit();
  });
};
