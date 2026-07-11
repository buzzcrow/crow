import { describe, it, expect, vi } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { ReactNode } from 'react';
import { ToastProvider } from '../contexts/ToastContext';
import { useBulkOperations } from './useBulkOperations';

const wrapper = ({ children }: { children: ReactNode }) => (
  <ToastProvider>{children}</ToastProvider>
);

describe('useBulkOperations', () => {
  it('runs perform() once per item and reports all success', async () => {
    const { result } = renderHook(() => useBulkOperations<number>(), { wrapper });
    const perform = vi.fn(async (_: number) => {});

    await act(async () => {
      await result.current.run({ items: [1, 2, 3], perform, actionLabel: 'op' });
    });

    expect(perform).toHaveBeenCalledTimes(3);
    expect(result.current.state.completed).toBe(3);
    expect(result.current.state.total).toBe(3);
    expect(result.current.state.isRunning).toBe(false);
    expect(result.current.state.results.every((r) => r.status === 'success')).toBe(true);
  });

  it('captures errors per item without aborting the rest', async () => {
    const { result } = renderHook(() => useBulkOperations<number>(), { wrapper });
    const perform = vi.fn(async (n: number) => {
      if (n === 2) throw new Error('boom');
    });

    await act(async () => {
      await result.current.run({ items: [1, 2, 3], perform, actionLabel: 'op' });
    });

    const byStatus = result.current.state.results.map((r) => r.status);
    expect(byStatus.filter((s) => s === 'success')).toHaveLength(2);
    expect(byStatus.filter((s) => s === 'failure')).toHaveLength(1);
    const failed = result.current.state.results.find((r) => r.status === 'failure');
    expect(failed?.error).toBe('boom');
  });

  it('no-ops on an empty items list', async () => {
    const { result } = renderHook(() => useBulkOperations<number>(), { wrapper });
    const perform = vi.fn();
    await act(async () => {
      await result.current.run({ items: [], perform, actionLabel: 'op' });
    });
    expect(perform).not.toHaveBeenCalled();
    expect(result.current.state.total).toBe(0);
  });

  it('respects concurrency bound (caps in-flight count)', async () => {
    const { result } = renderHook(() => useBulkOperations<number>(), { wrapper });
    let inFlight = 0;
    let maxInFlight = 0;
    const perform = vi.fn(async () => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await new Promise((r) => setTimeout(r, 5));
      inFlight--;
    });

    await act(async () => {
      await result.current.run({
        items: [1, 2, 3, 4, 5, 6, 7, 8],
        perform,
        actionLabel: 'op',
        concurrency: 2,
      });
    });

    expect(maxInFlight).toBeLessThanOrEqual(2);
  });

  it('reset() clears the state', async () => {
    const { result } = renderHook(() => useBulkOperations<number>(), { wrapper });
    await act(async () => {
      await result.current.run({ items: [1], perform: async () => {}, actionLabel: 'op' });
    });
    expect(result.current.state.total).toBe(1);
    act(() => {
      result.current.reset();
    });
    await waitFor(() => {
      expect(result.current.state.total).toBe(0);
      expect(result.current.state.results).toHaveLength(0);
    });
  });
});
