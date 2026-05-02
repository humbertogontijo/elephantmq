import { debuglog } from 'util';
import { errorObject, tryCatch } from '../../utils';

const logger = debuglog('elephantmq');

export function getTracesFromJson(stacktrace?: string): string[] {
  if (!stacktrace) {
    return [];
  }

  const traces = tryCatch(JSON.parse, JSON, [stacktrace]);

  if (traces === errorObject || !(traces instanceof Array)) {
    return [];
  } else {
    return traces;
  }
}

export function getReturnValueFromJson(_value: any) {
  const value = tryCatch(JSON.parse, JSON, [_value]);
  if (value !== errorObject) {
    return value;
  } else {
    logger('corrupted returnvalue: ' + _value, value);
  }
}
