import { describe, it, expect } from 'vitest';
import { fuzzySearch } from './fuzzySearch';

interface Item {
  id: string;
  name: string;
}

const items: Item[] = [
  { id: '1', name: 'rack-east-1' },
  { id: '2', name: 'rack-west-1' },
  { id: '3', name: 'store-orders' },
  { id: '4', name: 'group-alpha' },
];

describe('fuzzySearch', () => {
  it('returns all items with score 0 for empty query', () => {
    const results = fuzzySearch(items, '', (i) => [i.name]);
    expect(results).toHaveLength(items.length);
    expect(results.every((r) => r.score === 0)).toBe(true);
  });

  it('matches subsequence characters, not just substrings', () => {
    // "rwe" should match "rack-west-1" via r->rack, w->west, e->west.
    const results = fuzzySearch(items, 'rwe', (i) => [i.name]);
    expect(results.length).toBeGreaterThan(0);
    expect(results[0].item.name).toBe('rack-west-1');
  });

  it('orders results by ascending score (best first)', () => {
    const results = fuzzySearch(items, 'rack', (i) => [i.name]);
    expect(results.length).toBeGreaterThan(0);
    for (let i = 1; i < results.length; i++) {
      expect(results[i].score).toBeGreaterThanOrEqual(results[i - 1].score);
    }
  });

  it('omits items that do not contain every query char in order', () => {
    const results = fuzzySearch(items, 'zzz', (i) => [i.name]);
    expect(results).toHaveLength(0);
  });

  it('is case-insensitive', () => {
    const lower = fuzzySearch(items, 'rack', (i) => [i.name]);
    const upper = fuzzySearch(items, 'RACK', (i) => [i.name]);
    expect(upper.map((r) => r.item.id)).toEqual(lower.map((r) => r.item.id));
  });

  it('searches across all provided fields and uses the best score', () => {
    // Only items whose id or name contains "1" should match.
    const results = fuzzySearch(items, '1', (i) => [i.id, i.name]);
    const matchedIds = new Set(results.map((r) => r.item.id));
    expect(matchedIds.has('1')).toBe(true); // id "1"
    expect(matchedIds.has('2')).toBe(true); // name "rack-west-1"
    expect(matchedIds.has('3')).toBe(false);
    expect(matchedIds.has('4')).toBe(false);
  });
});
