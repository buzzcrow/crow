// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from 'react';
import { listDiskdbInstances, getDiskdbUsage, getDiskdbScanStatus } from '../api';
import type {
  DiskdbInstanceInfo,
  CapacityUsageResponse,
  ScanStatusResponse,
} from '../types';

interface UseCapacityTreeOptions {
  pollIntervalActive?: number;
  pollIntervalInactive?: number;
  enabled?: boolean;
}

interface UseCapacityTreeResult {
  instances: DiskdbInstanceInfo[];
  usage: CapacityUsageResponse | null;
  scanStatus: ScanStatusResponse | null;
  loading: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
}

export function useCapacityTree({
  pollIntervalActive = 5000,
  pollIntervalInactive = 30000,
  enabled = true,
}: UseCapacityTreeOptions = {}): UseCapacityTreeResult {
  const [instances, setInstances] = useState<DiskdbInstanceInfo[]>([]);
  const [usage, setUsage] = useState<CapacityUsageResponse | null>(null);
  const [scanStatus, setScanStatus] = useState<ScanStatusResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const isActiveRef = useRef(true);
  const pollTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const hasLoadedRef = useRef(false);

  const fetchData = useCallback(async () => {
    if (!enabled) return;

    try {
      if (!hasLoadedRef.current) {
        setLoading(true);
      }

      // Fetch all three in parallel. If instances/usage fail (no
      // diskdb deployed yet), we still want scan status.
      const [instancesResult, usageResult, scanResult] = await Promise.allSettled([
        listDiskdbInstances(),
        getDiskdbUsage(),
        getDiskdbScanStatus(),
      ]);

      if (instancesResult.status === 'fulfilled') {
        setInstances(instancesResult.value);
      } else {
        setInstances([]);
      }

      if (usageResult.status === 'fulfilled') {
        setUsage(usageResult.value);
      } else {
        setUsage(null);
      }

      if (scanResult.status === 'fulfilled') {
        setScanStatus(scanResult.value);
      } else {
        setScanStatus(null);
      }

      setError(null);
    } catch (err) {
      console.error('Failed to fetch capacity tree:', err);
      setError(err instanceof Error ? err : new Error('Unknown error fetching capacity tree'));
    } finally {
      hasLoadedRef.current = true;
      setLoading(false);
    }
  }, [enabled]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    if (!enabled) return;

    const scheduleNextPoll = () => {
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }

      const interval = isActiveRef.current ? pollIntervalActive : pollIntervalInactive;
      pollTimeoutRef.current = setTimeout(async () => {
        await fetchData();
        scheduleNextPoll();
      }, interval);
    };

    scheduleNextPoll();

    return () => {
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }
    };
  }, [enabled, pollIntervalActive, pollIntervalInactive, fetchData]);

  useEffect(() => {
    const handleVisibilityChange = () => {
      isActiveRef.current = document.visibilityState === 'visible';
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, []);

  return {
    instances,
    usage,
    scanStatus,
    loading,
    error,
    refresh: fetchData,
  };
}
