export interface Receiver {
  on: (evt: 'message', cb: (msg: unknown) => void) => void;
  off: (evt: 'message', cb: (msg: unknown) => void) => void;
}
