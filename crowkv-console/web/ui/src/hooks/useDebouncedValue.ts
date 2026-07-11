import { useEffect, useState } from 'react';

/**
 * Returns a debounced copy of `value` that updates at most once per
 * `delayMs` window of inactivity. Useful for search inputs over large
 * lists / canvases where filtering on every keystroke is wasteful.
 */
export function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);

  useEffect(() => {
    const handle = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(handle);
  }, [value, delayMs]);

  return debounced;
}
