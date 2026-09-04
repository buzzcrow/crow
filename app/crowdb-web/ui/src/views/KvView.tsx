// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { lazy, Suspense } from 'react';
import type { EnrichedStoreView } from '../types';
import type { SelectedEntity } from '../contexts/SelectionContext';

const KvOperatorPanel = lazy(() => import('../panels/KvOperatorPanel').then((m) => ({ default: m.KvOperatorPanel })));

export interface KvViewProps {
  stores: EnrichedStoreView[];
  selectedEntity: SelectedEntity | null;
  readonly: boolean;
  backendError: boolean;
  loading: boolean;
}

export function KvView(props: KvViewProps) {
  return (
    <Suspense fallback={<ViewFallback />}>
      <KvOperatorPanel {...props} />
    </Suspense>
  );
}

function ViewFallback() {
  return <div className="tw-w-full tw-h-full tw-flex tw-items-center tw-justify-center tw-text-muted tw-text-sm">Loading…</div>;
}
