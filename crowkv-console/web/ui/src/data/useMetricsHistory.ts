import { useState, useEffect, useCallback, useRef } from 'react';

interface MetricPoint {
  timestamp: number;
  value: number;
  labels?: Record<string, string>;
}

interface UseMetricsHistoryOptions {
  /** Maximum number of points to keep per metric */
  maxPoints?: number;
}

interface UseMetricsHistoryResult {
  /** Get history for a specific metric */
  getMetricHistory: (metricName: string, labels?: Record<string, string>) => MetricPoint[];
  /** Add a new data point to a metric */
  addMetricPoint: (metricName: string, value: number, labels?: Record<string, string>) => void;
  /** Clear history for a specific metric */
  clearMetricHistory: (metricName: string) => void;
  /** Clear all metric history */
  clearAllHistory: () => void;
}

/**
 * Hook for tracking metrics history over time
 */
export function useMetricsHistory({
  maxPoints = 100,
}: UseMetricsHistoryOptions = {}): UseMetricsHistoryResult {
  // Stores metric history: key is metric name + label fingerprint, value is array of points
  const [metricsHistory, setMetricsHistory] = useState<Record<string, MetricPoint[]>>({});
  const pollTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Create a unique key for metric + labels
  const getMetricKey = useCallback((metricName: string, labels?: Record<string, string>): string => {
    if (!labels) return metricName;
    const labelString = Object.entries(labels)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${v}`)
      .join(',');
    return `${metricName}:${labelString}`;
  }, []);

  // Get history for a specific metric
  const getMetricHistory = useCallback(
    (metricName: string, labels?: Record<string, string>): MetricPoint[] => {
      const key = getMetricKey(metricName, labels);
      return metricsHistory[key] || [];
    },
    [metricsHistory, getMetricKey]
  );

  // Add a new data point to a metric
  const addMetricPoint = useCallback(
    (metricName: string, value: number, labels?: Record<string, string>): void => {
      const key = getMetricKey(metricName, labels);
      const newPoint: MetricPoint = {
        timestamp: Date.now(),
        value,
        labels,
      };

      setMetricsHistory(prev => {
        const existingHistory = prev[key] || [];
        // Add new point and keep only maxPoints
        const newHistory = [...existingHistory, newPoint].slice(-maxPoints);
        return {
          ...prev,
          [key]: newHistory,
        };
      });
    },
    [getMetricKey, maxPoints]
  );

  // Clear history for a specific metric
  const clearMetricHistory = useCallback(
    (metricName: string): void => {
      setMetricsHistory(prev => {
        const newHistory = { ...prev };
        // Delete all keys starting with metricName
        Object.keys(newHistory).forEach(key => {
          if (key.startsWith(`${metricName}:`) || key === metricName) {
            delete newHistory[key];
          }
        });
        return newHistory;
      });
    },
    []
  );

  // Clear all metric history
  const clearAllHistory = useCallback((): void => {
    setMetricsHistory({});
  }, []);

  // Clean up polling on unmount
  useEffect(() => {
    return () => {
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }
    };
  }, []);

  return {
    getMetricHistory,
    addMetricPoint,
    clearMetricHistory,
    clearAllHistory,
  };
}
