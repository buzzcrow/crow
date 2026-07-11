import { describe, it, expect, beforeEach } from 'vitest';
import { localStorage as ls } from './localStorage';

describe('localStorage wrapper', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it('returns the default value when the key is unset', () => {
    expect(ls.get('themeMode', 'fallback')).toBe('fallback');
  });

  it('round-trips primitive values', () => {
    ls.set('themeMode', 'dark');
    expect(ls.get('themeMode', 'system')).toBe('dark');
  });

  it('round-trips object values via JSON', () => {
    const value = { ids: ['a', 'b'], n: 3 };
    ls.set('favorites', value);
    expect(ls.get('favorites', null)).toEqual(value);
  });

  it('returns the default if the stored value is corrupt', () => {
    window.localStorage.setItem('themeMode', '{not-json');
    expect(ls.get('themeMode', 'system')).toBe('system');
  });

  it('removes a key', () => {
    ls.set('viewMode', 'Logical');
    ls.remove('viewMode');
    expect(window.localStorage.getItem('viewMode')).toBeNull();
  });
});
