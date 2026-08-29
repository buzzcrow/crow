// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useActivity } from '../contexts/ActivityContext';
import { ActivityLogEntry } from '../types';
import { cn } from '../utils/cn';

/**
 * Client-side recent-operations list. No filter/export in v1 (deferred).
 */
export function ActivityLog() {
  const { entries, clear } = useActivity();

  return (
    <div className="tw-p-3 tw-space-y-2">
      <div className="tw-flex tw-justify-end">
        <button
          onClick={clear}
          disabled={entries.length === 0}
          className="tw-text-xs tw-text-muted hover:tw-text-failed disabled:tw-opacity-30 tw-transition-colors"
        >
          Clear log
        </button>
      </div>
      <div className="tw-border tw-border-border tw-rounded-md tw-divide-y tw-divide-border tw-max-h-[70vh] tw-overflow-y-auto">
        {entries.length === 0 ? (
          <div className="tw-px-3 tw-py-6 tw-text-center tw-text-xs tw-text-muted">No activity yet.</div>
        ) : (
          entries.map((e) => <Row key={e.id} entry={e} />)
        )}
      </div>
    </div>
  );
}

function Row({ entry }: { entry: ActivityLogEntry }) {
  const color =
    entry.status === 'Success'
      ? 'tw-text-healthy'
      : entry.status === 'Failed'
        ? 'tw-text-failed'
        : 'tw-text-degraded';
  return (
    <div className="tw-px-3 tw-py-2 tw-text-xs tw-space-y-0.5">
      <div className="tw-flex tw-items-center tw-justify-between">
        <span className="tw-font-medium tw-text-text">{entry.action}</span>
        <span className={cn('tw-text-[10px]', color)}>{entry.status}</span>
      </div>
      <div className="tw-text-[10px] tw-text-muted">
        {new Date(entry.timestamp).toLocaleString()} · {entry.target}
      </div>
      {entry.message && <div className="tw-text-[10px] tw-text-muted tw-italic">{entry.message}</div>}
    </div>
  );
}
