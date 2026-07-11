import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { exportAsCSV } from './exportUtils';

describe('exportAsCSV', () => {
  // Capture the source string passed to the Blob constructor — jsdom's
  // Blob.text()/Response can't reliably read blobs back as text, so we
  // intercept at construction time.
  let captured: string | null = null;
  let originalBlob: typeof Blob;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let createSpy: any;

  beforeEach(() => {
    captured = null;
    originalBlob = globalThis.Blob;
    class CapturingBlob extends originalBlob {
      constructor(parts: BlobPart[], options?: BlobPropertyBag) {
        super(parts, options);
        captured = parts.map((p) => (typeof p === 'string' ? p : '')).join('');
      }
    }
    // @ts-expect-error - assigning a subclass over the global is fine here.
    globalThis.Blob = CapturingBlob;
    createSpy = vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:test');
    vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {});
    HTMLAnchorElement.prototype.click = vi.fn();
  });

  afterEach(() => {
    globalThis.Blob = originalBlob;
  });

  it('serializes header labels and row values', async () => {
    exportAsCSV(
      [
        { name: 'a', count: 1 },
        { name: 'b', count: 2 },
      ],
      [
        { key: 'name', label: 'Name' },
        { key: 'count', label: 'Count' },
      ],
      'out.csv',
    );

    expect(createSpy).toHaveBeenCalled();
    expect(captured).toBe('"Name","Count"\n"a","1"\n"b","2"');
  });

  it('escapes embedded double quotes by doubling them', () => {
    exportAsCSV(
      [{ msg: 'he said "hi"' }],
      [{ key: 'msg', label: 'Message' }],
      'out.csv',
    );
    expect(captured).toContain('"he said ""hi"""');
  });

  it('produces only the header row for empty data', () => {
    exportAsCSV<{ x: string }>([], [{ key: 'x', label: 'X' }], 'out.csv');
    expect(captured).toBe('"X"');
  });
});
