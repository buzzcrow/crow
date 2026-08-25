// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import type { MetricsResponse } from '../types';
import { getNodeMetrics, getGroupMetrics, getStoreMetrics } from '../api';

type MetricsFetcher = (signal: AbortSignal) => Promise<MetricsResponse>;

/**
 * Poll a metrics endpoint at a fixed interval (~7s per the R11 design).
 * Returns `null` while loading, the latest `MetricsResponse` on success,
 * or `undefined` when no fetcher is applicable for the current entity.
 *
 * The poll is aborted on unmount or when the `fetcherKey` changes.
 */
export function useMetricsPoll(
  fetcher: MetricsFetcher | null,
  fetcherKey: string,
  intervalMs: number = 7000
): MetricsResponse | null {
  const [data, setData] = useState<MetricsResponse | null>(null);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  useEffect(() => {
    if (!fetcher) {
      setData(null);
      return;
    }
    let cancelled = false;
    const controller = new AbortController();

    const poll = async () => {
      try {
        const resp = await fetcherRef.current!(controller.signal);
        if (!cancelled) setData(resp);
      } catch {
        // Silently ignore — the inspector is best-effort.
      }
    };

    poll();
    const id = setInterval(poll, intervalMs);
    return () => {
      cancelled = true;
      controller.abort();
      clearInterval(id);
    };
    // Re-run only when the entity key or interval changes — not on every
    // render. The fetcher identity is unstable (new arrow function from
    // buildMetricsFetcher on each render), but fetcherRef.current always
    // points at the latest one.
  }, [fetcherKey, intervalMs]);

  return data;
}

/**
 * Build a metrics fetcher for the given entity, or `null` if metrics
 * are not applicable. The `fetcherKey` encodes the entity identity so
 * the poll effect re-initializes on selection change.
 */
export function buildMetricsFetcher(
  entityType: string,
  entityId: string,
  parentIdStore?: string,
  parentIdGroup?: string
): { fetcher: MetricsFetcher; key: string } | null {
  if (entityType === 'Node') {
    const nodeId = entityId;
    return {
      fetcher: (signal) => getNodeMetrics(Number(nodeId), undefined, { signal, skipDeduplication: true }),
      key: `node:${nodeId}`,
    };
  }
  if (entityType === 'Store') {
    const sid = entityId;
    return {
      fetcher: (signal) => getStoreMetrics(sid, undefined, { signal, skipDeduplication: true }),
      key: `store:${sid}`,
    };
  }
  if (entityType === 'Group' || entityType === 'Replica') {
    const sid = parentIdStore;
    const gid = parentIdGroup;
    if (!sid || !gid) return null;
    return {
      fetcher: (signal) => getGroupMetrics(sid, gid, undefined, { signal, skipDeduplication: true }),
      key: `group:${sid}:${gid}`,
    };
  }
  return null;
}
