// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from 'react';
import { getDiskdbUsage } from '../api';
import type { ZoneUsageDto } from '../types';

interface ZoneBitmapState {
  zone: ZoneUsageDto | null;
  loading: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
}

/**
 * On-demand zone bitmap fetch (R85 §3-B). The disk-level
 * `QueryCapacityStats` response omits `usage_bitmap` (brief per-zone
 * entries only); the full bitmap is returned only by the zone-level
 * query shape (`dg + disk + zone`). This hook fetches it when a zone
 * is selected and caches the last result. Polling (3 s) refetches the
 * focused zone via `refresh`.
 */
export function useZoneBitmap(
  dgId: number | undefined,
  diskId: string | undefined,
  zoneIndex: number | null,
): ZoneBitmapState {
  const [zone, setZone] = useState<ZoneUsageDto | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const reqIdRef = useRef(0);

  const fetchBitmap = useCallback(async () => {
    if (dgId === undefined || diskId === undefined || zoneIndex === null) {
      setZone(null);
      setError(null);
      return;
    }
    const myReq = ++reqIdRef.current;
    setLoading(true);
    try {
      const resp = await getDiskdbUsage(dgId, diskId, zoneIndex);
      // Ignore stale responses from a previous selection.
      if (myReq !== reqIdRef.current) return;
      const dg = resp.disk_groups.find((g) => g.disk_group_id === dgId);
      const disk = dg?.disks.find((d) => d.disk_id === diskId);
      const zu = disk?.zone_usages.find((z) => z.zone_index === zoneIndex);
      if (myReq !== reqIdRef.current) return;
      if (!zu) {
        setError(new Error(`zone ${zoneIndex} not found on disk ${diskId}`));
        setZone(null);
      } else {
        setZone(zu);
        setError(null);
      }
    } catch (err) {
      if (myReq !== reqIdRef.current) return;
      setError(err instanceof Error ? err : new Error('Unknown error fetching zone bitmap'));
      setZone(null);
    } finally {
      if (myReq === reqIdRef.current) setLoading(false);
    }
  }, [dgId, diskId, zoneIndex]);

  useEffect(() => {
    void fetchBitmap();
  }, [fetchBitmap]);

  return { zone, loading, error, refresh: fetchBitmap };
}
