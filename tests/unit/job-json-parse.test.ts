import { describe, expect, it } from 'vitest';
import {
  getTracesFromJson,
  getReturnValueFromJson,
} from '../../src/classes/job/job-json-parse';

describe('job-json-parse', () => {
  describe('getTracesFromJson', () => {
    it('returns empty array when stacktrace is missing or invalid', () => {
      expect(getTracesFromJson(undefined)).toEqual([]);
      expect(getTracesFromJson('')).toEqual([]);
      expect(getTracesFromJson('not-json')).toEqual([]);
      expect(getTracesFromJson('{}')).toEqual([]);
    });

    it('returns parsed array when valid', () => {
      expect(getTracesFromJson('["a","b"]')).toEqual(['a', 'b']);
    });
  });

  describe('getReturnValueFromJson', () => {
    it('returns parsed value when JSON is valid', () => {
      expect(getReturnValueFromJson('"x"')).toBe('x');
      expect(getReturnValueFromJson('{"a":1}')).toEqual({ a: 1 });
    });

    it('returns undefined for corrupted JSON', () => {
      expect(getReturnValueFromJson('no')).toBeUndefined();
    });
  });
});
