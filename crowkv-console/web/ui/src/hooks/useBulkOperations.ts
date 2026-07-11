import { useCallback, useState } from 'react';
import { useToast } from '../contexts/ToastContext';

export type BulkItemStatus = 'pending' | 'running' | 'success' | 'failure';

export interface BulkItemResult<T> {
  item: T;
  status: BulkItemStatus;
  error?: string;
}

export interface BulkOperationState<T> {
  /** Whether a bulk operation is currently running. */
  isRunning: boolean;
  /** Per-item results, updated as each item completes. */
  results: BulkItemResult<T>[];
  /** Number of completed items (success + failure). */
  completed: number;
  /** Total items in the active operation. */
  total: number;
}

export interface RunBulkOptions<T> {
  /** Items to operate on. */
  items: T[];
  /** Function performing the operation on a single item. */
  perform: (item: T) => Promise<void>;
  /** Human label used in toast notifications, e.g. "delete replica". */
  actionLabel: string;
  /** Maximum number of items to run concurrently. Default 4. */
  concurrency?: number;
}

/**
 * Runs an async operation over a batch of items with bounded concurrency,
 * tracking per-item progress and surfacing success/failure toasts.
 *
 * Intended to back the BulkActionDialog confirmation UI.
 */
export function useBulkOperations<T>() {
  const { success, error: errorToast, warning } = useToast();
  const [state, setState] = useState<BulkOperationState<T>>({
    isRunning: false,
    results: [],
    completed: 0,
    total: 0,
  });

  const reset = useCallback(() => {
    setState({ isRunning: false, results: [], completed: 0, total: 0 });
  }, []);

  const run = useCallback(
    async ({ items, perform, actionLabel, concurrency = 4 }: RunBulkOptions<T>) => {
      if (items.length === 0) return;

      const initialResults: BulkItemResult<T>[] = items.map((item) => ({
        item,
        status: 'pending' as const,
      }));
      setState({
        isRunning: true,
        results: initialResults,
        completed: 0,
        total: items.length,
      });

      let index = 0;
      let successes = 0;
      let failures = 0;

      const worker = async () => {
        while (true) {
          const current = index++;
          if (current >= items.length) return;
          // Mark running.
          setState((prev) => {
            const next = [...prev.results];
            next[current] = { ...next[current], status: 'running' };
            return { ...prev, results: next };
          });
          try {
            await perform(items[current]);
            successes++;
            setState((prev) => {
              const next = [...prev.results];
              next[current] = { ...next[current], status: 'success' };
              return { ...prev, results: next, completed: prev.completed + 1 };
            });
          } catch (err) {
            failures++;
            const message = err instanceof Error ? err.message : String(err);
            setState((prev) => {
              const next = [...prev.results];
              next[current] = { ...next[current], status: 'failure', error: message };
              return { ...prev, results: next, completed: prev.completed + 1 };
            });
          }
        }
      };

      const workers = Array.from({ length: Math.min(concurrency, items.length) }, worker);
      await Promise.all(workers);

      setState((prev) => ({ ...prev, isRunning: false }));

      if (failures === 0) {
        success(`All ${successes} ${actionLabel} succeeded`);
      } else if (successes === 0) {
        errorToast(`All ${failures} ${actionLabel} failed`);
      } else {
        warning(`${successes} ${actionLabel} succeeded, ${failures} failed`);
      }
    },
    [success, errorToast, warning],
  );

  return { state, run, reset };
}
