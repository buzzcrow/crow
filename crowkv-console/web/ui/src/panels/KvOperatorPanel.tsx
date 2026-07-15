// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useCallback, useEffect, useMemo } from 'react';
import { Search, Info, Database, Trash2, Loader2, Copy, AlertTriangle, FlaskConical } from 'lucide-react';
import { useToast } from '../contexts/ToastContext';
import { useActivity } from '../contexts/ActivityContext';
import { Dialog } from '../components/Dialog';
import { kvGet, kvPut, kvDelete, kvScan, type KvGetResponse, type KvScanItem } from '../api';
import type { StoreView, GroupSummary } from '../types';
import type { SelectedEntity } from '../contexts/SelectionContext';

const ALL_GROUPS = '__all__';

interface ScanRow extends KvScanItem {
  groupId: string;
  revision?: number;
  selected: boolean;
}

interface KvOperatorPanelProps {
  stores: StoreView[];
  selectedEntity: SelectedEntity | null;
  readonly?: boolean;
}

export function KvOperatorPanel({ stores, selectedEntity, readonly }: KvOperatorPanelProps) {
  const { success, error } = useToast();
  const { log } = useActivity();

  const [storeId, setStoreId] = useState('');
  const [groupId, setGroupId] = useState('');
  const [scanPrefix, setScanPrefix] = useState('');
  const [scanRows, setScanRows] = useState<ScanRow[]>([]);
  const [scanTruncated, setScanTruncated] = useState(false);
  const [scanLoading, setScanLoading] = useState(false);
  const [scanCursors, setScanCursors] = useState<Map<string, { lastKey: string; truncated: boolean }>>(new Map());
  const [loadingMore, setLoadingMore] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const [getKey, setGetKey] = useState('');
  const [getResult, setGetResult] = useState<KvGetResponse | null>(null);
  const [getLoading, setGetLoading] = useState(false);

  const [putKey, setPutKey] = useState('');
  const [putValue, setPutValue] = useState('');
  const [autoScan, setAutoScan] = useState(true);
  const [putLoading, setPutLoading] = useState(false);

  const [deleteKey, setDeleteKey] = useState('');
  const [confirmDelete, setConfirmDelete] = useState<{ count: number; keys: string[]; onConfirm: () => Promise<void> } | null>(null);
  const [deleteLoading, setDeleteLoading] = useState(false);

  const [demoCount, setDemoCount] = useState(100);
  const [demoLoading, setDemoLoading] = useState(false);

  const groupsInStore = useMemo(() => {
    if (!storeId) return [] as GroupSummary[];
    const store = stores.find((s) => String(s.store_id) === storeId);
    return store?.groups || [];
  }, [storeId, stores]);

  const groupIdsInStore = useMemo(() => groupsInStore.map((g) => String(g.group_id)), [groupsInStore]);

  useEffect(() => {
    if (stores.length > 0 && !storeId) {
      setStoreId(String(stores[0].store_id));
    }
  }, [stores, storeId]);

  useEffect(() => {
    if (groupIdsInStore.length > 0 && !groupId) {
      setGroupId(groupIdsInStore[0]);
    }
    if (groupId && !groupIdsInStore.includes(groupId) && groupId !== ALL_GROUPS) {
      setGroupId(groupIdsInStore[0] || '');
    }
  }, [groupIdsInStore, groupId]);

  useEffect(() => {
    if (selectedEntity?.type === 'Group' && selectedEntity.viewMode === 'Logical') {
      const sid = selectedEntity.parentIds?.store_id;
      const gid = selectedEntity.id;
      if (sid && stores.some((s) => String(s.store_id) === sid)) {
        setStoreId(sid);
        setGroupId(gid);
      }
    }
  }, [selectedEntity, stores]);

  const handleStoreChange = useCallback((sid: string) => {
    setStoreId(sid);
    setGroupId('');
    setScanRows([]);
    setGetResult(null);
    setErrorMsg(null);
  }, []);

  const handleGroupChange = useCallback((gid: string) => {
    setGroupId(gid);
    setScanRows([]);
    setGetResult(null);
    setErrorMsg(null);
  }, []);

  const targetLabel = groupId === ALL_GROUPS ? `${storeId}/all` : `${storeId}/${groupId}`;

  const handleScan = useCallback(async () => {
    if (!storeId || !groupId) return;
    setScanLoading(true);
    setErrorMsg(null);
    try {
      if (groupId === ALL_GROUPS) {
        const allRows: ScanRow[] = [];
        const cursors = new Map<string, { lastKey: string; truncated: boolean }>();
        let anyTruncated = false;
        for (const gid of groupIdsInStore) {
          const result = await kvScan(storeId, gid, scanPrefix);
          allRows.push(...result.items.map((item) => ({ ...item, groupId: gid, selected: false })));
          if (result.items.length > 0) {
            cursors.set(gid, { lastKey: result.items[result.items.length - 1].key_utf8, truncated: result.truncated });
          }
          if (result.truncated) anyTruncated = true;
        }
        setScanRows(allRows);
        setScanTruncated(anyTruncated);
        setScanCursors(cursors);
      } else {
        const result = await kvScan(storeId, groupId, scanPrefix);
        setScanRows(result.items.map((item) => ({ ...item, groupId, selected: false })));
        setScanTruncated(result.truncated);
        const cursors = new Map<string, { lastKey: string; truncated: boolean }>();
        if (result.items.length > 0) {
          cursors.set(groupId, { lastKey: result.items[result.items.length - 1].key_utf8, truncated: result.truncated });
        }
        setScanCursors(cursors);
      }
      log({ action: 'KV Scan', target: targetLabel, status: 'Success', message: `Found ${scanRows.length} keys` });
      success(`Scanned ${scanRows.length} keys`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Scan failed';
      setErrorMsg(msg);
      log({ action: 'KV Scan', target: targetLabel, status: 'Failed', message: msg });
      error(msg);
    } finally {
      setScanLoading(false);
    }
  }, [storeId, groupId, scanPrefix, groupIdsInStore, targetLabel, log, success, error, scanRows.length]);

  const handleLoadMore = useCallback(async () => {
    if (!storeId || !groupId || scanCursors.size === 0) return;
    setLoadingMore(true);
    setErrorMsg(null);
    try {
      const gids = groupId === ALL_GROUPS ? groupIdsInStore : [groupId];
      const newRows: ScanRow[] = [];
      const updatedCursors = new Map(scanCursors);
      let anyTruncated = false;
      for (const gid of gids) {
        const cursor = updatedCursors.get(gid);
        if (!cursor || !cursor.truncated) continue;
        const result = await kvScan(storeId, gid, scanPrefix, 100, cursor.lastKey);
        newRows.push(...result.items.map((item) => ({ ...item, groupId: gid, selected: false })));
        if (result.items.length > 0) {
          updatedCursors.set(gid, { lastKey: result.items[result.items.length - 1].key_utf8, truncated: result.truncated });
        } else {
          updatedCursors.set(gid, { lastKey: cursor.lastKey, truncated: false });
        }
        if (result.truncated) anyTruncated = true;
      }
      setScanRows((prev) => [...prev, ...newRows]);
      setScanTruncated(anyTruncated);
      setScanCursors(updatedCursors);
      log({ action: 'KV Scan', target: targetLabel, status: 'Success', message: `Loaded ${newRows.length} more keys` });
      success(`Loaded ${newRows.length} more keys`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Load more failed';
      setErrorMsg(msg);
      log({ action: 'KV Scan', target: targetLabel, status: 'Failed', message: msg });
      error(msg);
    } finally {
      setLoadingMore(false);
    }
  }, [storeId, groupId, scanCursors, groupIdsInStore, scanPrefix, targetLabel, log, success, error]);

  const handleGet = useCallback(async () => {
    if (!getKey || !storeId || !groupId || groupId === ALL_GROUPS) return;
    setGetLoading(true);
    setErrorMsg(null);
    try {
      const result = await kvGet(storeId, groupId, getKey);
      setGetResult(result);
      log({ action: 'KV Get', target: `${storeId}/${groupId}`, status: 'Success', message: `key: "${getKey}"` });
      success(result.found ? `Retrieved "${getKey}"` : `Key "${getKey}" not found`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Get failed';
      setErrorMsg(msg);
      log({ action: 'KV Get', target: `${storeId}/${groupId}`, status: 'Failed', message: msg });
      error(msg);
    } finally {
      setGetLoading(false);
    }
  }, [getKey, storeId, groupId, log, success, error]);

  const handlePut = useCallback(async () => {
    if (!putKey || !putValue || !storeId || !groupId) return;
    const targetGid = groupId === ALL_GROUPS
      ? groupIdsInStore[Math.floor(Math.random() * groupIdsInStore.length)]
      : groupId;
    if (!targetGid) return;
    setPutLoading(true);
    setErrorMsg(null);
    try {
      await kvPut(storeId, targetGid, { key: putKey, value: putValue });
      log({ action: 'KV Put', target: `${storeId}/${targetGid}`, status: 'Success', message: `key: "${putKey}"` });
      success(`Key written: "${putKey}"`);
      setPutKey('');
      setPutValue('');
      if (autoScan) {
        setGroupId(targetGid);
        setTimeout(() => handleScan(), 100);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Put failed';
      setErrorMsg(msg);
      log({ action: 'KV Put', target: `${storeId}/${targetGid}`, status: 'Failed', message: msg });
      error(msg);
    } finally {
      setPutLoading(false);
    }
  }, [putKey, putValue, storeId, groupId, groupIdsInStore, autoScan, log, success, error, handleScan]);

  const handleDeleteKey = useCallback(async () => {
    if (!deleteKey || !storeId || !groupId || groupId === ALL_GROUPS) return;
    setConfirmDelete({ count: 1, keys: [deleteKey], onConfirm: async () => {
      setDeleteLoading(true);
      try {
        await kvDelete(storeId, groupId, { key: deleteKey });
        log({ action: 'KV Delete', target: `${storeId}/${groupId}`, status: 'Success', message: `key: "${deleteKey}"` });
        success(`Key deleted: "${deleteKey}"`);
        setDeleteKey('');
        if (autoScan) setTimeout(() => handleScan(), 100);
      } catch (err) {
        const msg = err instanceof Error ? err.message : 'Delete failed';
        setErrorMsg(msg);
        log({ action: 'KV Delete', target: `${storeId}/${groupId}`, status: 'Failed', message: msg });
        error(msg);
      } finally {
        setDeleteLoading(false);
      }
    }});
  }, [deleteKey, storeId, groupId, autoScan, log, success, error, handleScan]);

  const handleDeletePrefix = useCallback(async () => {
    if (!deleteKey || !storeId || !groupId) return;
    const gids = groupId === ALL_GROUPS ? groupIdsInStore : [groupId];
    const allKeys: { key: string; gid: string }[] = [];
    for (const gid of gids) {
      const result = await kvScan(storeId, gid, deleteKey);
      allKeys.push(...result.items.map((item) => ({ key: item.key_utf8, gid })));
    }
    if (allKeys.length === 0) {
      success('No keys match prefix');
      return;
    }
    setConfirmDelete({ count: allKeys.length, keys: allKeys.map((k) => k.key), onConfirm: async () => {
      setDeleteLoading(true);
      let ok = 0, fail = 0;
      for (const { key, gid } of allKeys) {
        try {
          await kvDelete(storeId, gid, { key });
          ok++;
        } catch {
          fail++;
        }
      }
      log({ action: 'KV Delete Prefix', target: targetLabel, status: fail > 0 ? 'Failed' : 'Success', message: `${ok} deleted, ${fail} failed` });
      success(`Deleted ${ok} keys${fail > 0 ? `, ${fail} failed` : ''}`);
      setDeleteLoading(false);
      setTimeout(() => handleScan(), 100);
    }});
  }, [deleteKey, storeId, groupId, groupIdsInStore, targetLabel, log, success, handleScan]);

  const selectedRows = scanRows.filter((r) => r.selected);
  const handleDeleteSelected = useCallback(async () => {
    if (selectedRows.length === 0) return;
    setConfirmDelete({ count: selectedRows.length, keys: selectedRows.map((r) => r.key_utf8), onConfirm: async () => {
      setDeleteLoading(true);
      let ok = 0, fail = 0;
      for (const row of selectedRows) {
        try {
          await kvDelete(storeId, row.groupId, { key: row.key_utf8 });
          ok++;
        } catch {
          fail++;
        }
      }
      log({ action: 'KV Delete Selected', target: targetLabel, status: fail > 0 ? 'Failed' : 'Success', message: `${ok} deleted, ${fail} failed` });
      success(`Deleted ${ok} keys${fail > 0 ? `, ${fail} failed` : ''}`);
      setDeleteLoading(false);
      setTimeout(() => handleScan(), 100);
    }});
  }, [selectedRows, storeId, targetLabel, log, success, handleScan]);

  const handleInlineDelete = useCallback((key: string, gid: string) => {
    setConfirmDelete({ count: 1, keys: [key], onConfirm: async () => {
      setDeleteLoading(true);
      try {
        await kvDelete(storeId, gid, { key });
        log({ action: 'KV Delete', target: `${storeId}/${gid}`, status: 'Success', message: `key: "${key}"` });
        success(`Key deleted: "${key}"`);
        setTimeout(() => handleScan(), 100);
      } catch (err) {
        const msg = err instanceof Error ? err.message : 'Delete failed';
        setErrorMsg(msg);
        error(msg);
      } finally {
        setDeleteLoading(false);
      }
    }});
  }, [storeId, log, success, error, handleScan]);

  const handleDemoInject = useCallback(async () => {
    if (!storeId || demoCount <= 0) return;
    const gids = groupId === ALL_GROUPS ? groupIdsInStore : [groupId];
    if (gids.length === 0) return;
    setDemoLoading(true);
    setErrorMsg(null);
    let ok = 0, fail = 0;
    const batch = Math.random().toString(36).substring(2, 6);
    for (let i = 1; i <= demoCount; i++) {
      const key = `demo_key_${batch}_${String(i).padStart(4, '0')}`;
      const value = `demo_val_${batch}_${String(i).padStart(4, '0')}`;
      const gid = gids[Math.floor(Math.random() * gids.length)];
      try {
        await kvPut(storeId, gid, { key, value });
        ok++;
      } catch {
        fail++;
      }
    }
    log({ action: 'Demo Inject', target: targetLabel, status: fail > 0 ? 'Failed' : 'Success', message: `${ok} injected, ${fail} failed` });
    success(`Injected ${ok} demo keys${fail > 0 ? `, ${fail} failed` : ''}`);
    setDemoLoading(false);
    setTimeout(() => handleScan(), 100);
  }, [storeId, groupId, groupIdsInStore, demoCount, targetLabel, log, success, handleScan]);

  const handleDemoDelete = useCallback(async () => {
    if (!storeId || !groupId) return;
    const gids = groupId === ALL_GROUPS ? groupIdsInStore : [groupId];
    const allKeys: { key: string; gid: string }[] = [];
    for (const gid of gids) {
      const result = await kvScan(storeId, gid, 'demo_');
      allKeys.push(...result.items.map((item) => ({ key: item.key_utf8, gid })));
    }
    if (allKeys.length === 0) {
      success('No demo keys found');
      return;
    }
    setConfirmDelete({ count: allKeys.length, keys: allKeys.map((k) => k.key), onConfirm: async () => {
      setDemoLoading(true);
      let ok = 0, fail = 0;
      for (const { key, gid } of allKeys) {
        try {
          await kvDelete(storeId, gid, { key });
          ok++;
        } catch {
          fail++;
        }
      }
      log({ action: 'Demo Delete All', target: targetLabel, status: fail > 0 ? 'Failed' : 'Success', message: `${ok} deleted, ${fail} failed` });
      success(`Deleted ${ok} demo keys${fail > 0 ? `, ${fail} failed` : ''}`);
      setDemoLoading(false);
      setTimeout(() => handleScan(), 100);
    }});
  }, [storeId, groupId, groupIdsInStore, targetLabel, log, success, handleScan]);

  const toggleRow = useCallback((idx: number) => {
    setScanRows((prev) => prev.map((r, i) => (i === idx ? { ...r, selected: !r.selected } : r)));
  }, []);

  const toggleAll = useCallback(() => {
    const allSelected = scanRows.every((r) => r.selected);
    setScanRows((prev) => prev.map((r) => ({ ...r, selected: !allSelected })));
  }, [scanRows]);

  const copy = useCallback((text: string) => {
    navigator.clipboard.writeText(text).then(
      () => success('Copied to clipboard'),
      () => error('Copy failed'),
    );
  }, [success, error]);

  const showGroupColumn = groupId === ALL_GROUPS;
  const allSelected = scanRows.length > 0 && scanRows.every((r) => r.selected);

  if (stores.length === 0) {
    return (
      <div className="tw-flex tw-items-center tw-justify-center tw-h-full tw-text-muted tw-text-sm">
        No stores available. Create a store first.
      </div>
    );
  }

  return (
    <div className="tw-h-full tw-overflow-y-auto tw-bg-bg tw-text-text">
      <div className="tw-p-4 tw-space-y-3">
        {/* Selector bar */}
        <div className="tw-flex tw-items-center tw-gap-3 tw-flex-wrap">
          <div className="tw-flex tw-items-center tw-gap-1.5">
            <label className="tw-text-xs tw-text-muted">Store</label>
            <select
              value={storeId}
              onChange={(e) => handleStoreChange(e.target.value)}
              className="tw-bg-panel tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text"
            >
              {stores.map((s) => (
                <option key={s.store_id} value={String(s.store_id)}>Store {s.store_id}</option>
              ))}
            </select>
          </div>
          <div className="tw-flex tw-items-center tw-gap-1.5">
            <label className="tw-text-xs tw-text-muted">Group</label>
            <select
              value={groupId}
              onChange={(e) => handleGroupChange(e.target.value)}
              className="tw-bg-panel tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text"
            >
              <option value={ALL_GROUPS}>All Groups</option>
              {groupsInStore.map((g) => (
                <option key={g.group_id} value={String(g.group_id)}>Group {g.group_id}</option>
              ))}
            </select>
          </div>
          <div className="tw-flex tw-items-center tw-gap-1.5 tw-ml-auto">
            <input
              type="text"
              value={scanPrefix}
              onChange={(e) => setScanPrefix(e.target.value)}
              placeholder="Key prefix (empty = all)"
              className="tw-bg-panel tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted tw-w-48"
              onKeyDown={(e) => e.key === 'Enter' && handleScan()}
            />
            <button
              onClick={handleScan}
              disabled={scanLoading || !storeId || !groupId}
              className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs disabled:tw-opacity-50"
            >
              {scanLoading ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <Search className="tw-h-3 tw-w-3" />}
              Scan
            </button>
          </div>
        </div>

        {errorMsg && (
          <div className="tw-flex tw-items-start tw-gap-2 tw-p-2 tw-rounded tw-bg-failed/10 tw-border tw-border-failed/30 tw-text-failed tw-text-xs">
            <AlertTriangle className="tw-h-4 tw-w-4 tw-flex-shrink-0" />
            <span>{errorMsg}</span>
          </div>
        )}

        {/* Action bar */}
        <div className="tw-border tw-border-border tw-rounded tw-p-3 tw-space-y-2 tw-bg-panel/50">
          {/* Get row */}
          <div className="tw-flex tw-items-center tw-gap-2 tw-flex-wrap">
            <span className="tw-text-xs tw-text-muted tw-w-10">Get</span>
            <input
              type="text"
              value={getKey}
              onChange={(e) => setGetKey(e.target.value)}
              placeholder="Key"
              className="tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted tw-w-40"
              onKeyDown={(e) => e.key === 'Enter' && handleGet()}
            />
            <button
              onClick={handleGet}
              disabled={getLoading || !getKey || groupId === ALL_GROUPS}
              className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs disabled:tw-opacity-50"
            >
              {getLoading ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <Info className="tw-h-3 tw-w-3" />}
              Get
            </button>
            {getResult && (
              <span className="tw-text-xs tw-text-text tw-flex tw-items-center tw-gap-1">
                {getResult.found ? (
                  <>
                    <span className="tw-font-mono tw-text-muted">{getResult.value_utf8}</span>
                    <span className="tw-text-muted tw-text-[10px]">rev: {getResult.revision}</span>
                    <button onClick={() => copy(getResult.value_utf8 || '')} className="tw-text-muted hover:tw-text-text">
                      <Copy className="tw-h-3 tw-w-3" />
                    </button>
                  </>
                ) : (
                  <span className="tw-text-muted">not found</span>
                )}
              </span>
            )}
            {groupId === ALL_GROUPS && (
              <span className="tw-text-[10px] tw-text-muted">Select a specific group to use Get</span>
            )}
          </div>

          {/* Put row */}
          {!readonly && (
            <div className="tw-flex tw-items-center tw-gap-2 tw-flex-wrap">
              <span className="tw-text-xs tw-text-muted tw-w-10">Put</span>
              <input
                type="text"
                value={putKey}
                onChange={(e) => setPutKey(e.target.value)}
                placeholder="Key"
                className="tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted tw-w-40"
              />
              <input
                type="text"
                value={putValue}
                onChange={(e) => setPutValue(e.target.value)}
                placeholder="Value"
                className="tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted tw-w-48"
              />
              <button
                onClick={handlePut}
                disabled={putLoading || !putKey || !putValue}
                className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs disabled:tw-opacity-50"
              >
                {putLoading ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <Database className="tw-h-3 tw-w-3" />}
                Put
              </button>
              <label className="tw-flex tw-items-center tw-gap-1 tw-text-xs tw-text-muted">
                <input type="checkbox" checked={autoScan} onChange={(e) => setAutoScan(e.target.checked)} />
                auto-scan
              </label>
            </div>
          )}

          {/* Delete row */}
          {!readonly && (
            <div className="tw-flex tw-items-center tw-gap-2 tw-flex-wrap">
              <span className="tw-text-xs tw-text-muted tw-w-10">Del</span>
              <input
                type="text"
                value={deleteKey}
                onChange={(e) => setDeleteKey(e.target.value)}
                placeholder="Key"
                className="tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted tw-w-40"
              />
              <button
                onClick={handleDeleteKey}
                disabled={deleteLoading || !deleteKey || groupId === ALL_GROUPS}
                className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-bg-failed tw-text-bg tw-rounded tw-text-xs disabled:tw-opacity-50"
              >
                <Trash2 className="tw-h-3 tw-w-3" />
                Delete
              </button>
              <button
                onClick={handleDeletePrefix}
                disabled={deleteLoading || !deleteKey}
                className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-border tw-border-failed/30 tw-text-failed tw-rounded tw-text-xs hover:tw-bg-failed/10 disabled:tw-opacity-50"
                title="Delete all keys matching the prefix in the Key field"
              >
                Delete Prefix
              </button>
              <button
                onClick={handleDeleteSelected}
                disabled={deleteLoading || selectedRows.length === 0}
                className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-border tw-border-failed/30 tw-text-failed tw-rounded tw-text-xs hover:tw-bg-failed/10 disabled:tw-opacity-50"
              >
                Delete Selected ({selectedRows.length})
              </button>
            </div>
          )}

          {/* Demo row */}
          {!readonly && (
            <div className="tw-flex tw-items-center tw-gap-2 tw-flex-wrap tw-pt-1 tw-border-t tw-border-border">
              <span className="tw-text-xs tw-text-muted tw-w-10 tw-flex tw-items-center tw-gap-0.5">
                <FlaskConical className="tw-h-3 tw-w-3" /> Demo
              </span>
              <span className="tw-text-[10px] tw-text-muted">Inject</span>
              <input
                type="number"
                value={demoCount}
                onChange={(e) => setDemoCount(Math.max(1, parseInt(e.target.value) || 0))}
                className="tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text tw-w-20"
              />
              <span className="tw-text-xs tw-text-muted">demo keys</span>
              <button
                onClick={handleDemoInject}
                disabled={demoLoading}
                className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs disabled:tw-opacity-50"
              >
                {demoLoading ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <Database className="tw-h-3 tw-w-3" />}
                Inject
              </button>
              <button
                onClick={handleDemoDelete}
                disabled={demoLoading}
                className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-border tw-border-failed/30 tw-text-failed tw-rounded tw-text-xs hover:tw-bg-failed/10 disabled:tw-opacity-50"
              >
                <Trash2 className="tw-h-3 tw-w-3" />
                Delete all demo
              </button>
            </div>
          )}
        </div>

        {/* Results table */}
        {scanRows.length > 0 && (
          <div className="tw-space-y-1">
            <span className="tw-text-xs tw-text-muted">
              {scanRows.length} result(s){scanTruncated && ' (truncated)'}
            </span>
            <div className="tw-border tw-border-border tw-rounded tw-overflow-y-auto" style={{ maxHeight: 'calc(100vh - 360px)' }}>
              <table className="tw-w-full tw-text-xs">
                <thead className="tw-bg-panel tw-sticky tw-top-0">
                  <tr>
                    <th className="tw-w-8 tw-p-2 tw-border-b tw-border-border">
                      <input type="checkbox" checked={allSelected} onChange={toggleAll} />
                    </th>
                    <th className="tw-text-left tw-p-2 tw-text-muted tw-border-b tw-border-border">Key</th>
                    <th className="tw-text-left tw-p-2 tw-text-muted tw-border-b tw-border-border">Value</th>
                    {showGroupColumn && (
                      <th className="tw-text-left tw-p-2 tw-text-muted tw-border-b tw-border-border">Group</th>
                    )}
                    <th className="tw-w-8 tw-border-b tw-border-border" />
                  </tr>
                </thead>
                <tbody className="tw-divide-y tw-divide-border">
                  {scanRows.map((row, idx) => (
                    <tr
                      key={`${row.groupId}-${row.key_utf8}-${idx}`}
                      className="hover:tw-bg-panel/30 tw-cursor-pointer"
                      onClick={() => { setGetKey(row.key_utf8); }}
                    >
                      <td className="tw-p-2 tw-text-center" onClick={(e) => { e.stopPropagation(); toggleRow(idx); }}>
                        <input type="checkbox" checked={row.selected} onChange={() => toggleRow(idx)} />
                      </td>
                      <td className="tw-p-2 tw-font-mono tw-truncate tw-max-w-[200px]" title={row.key_utf8}>
                        {row.key_utf8}
                      </td>
                      <td className="tw-p-2 tw-font-mono tw-truncate tw-max-w-[200px]" title={row.value_utf8}>
                        {row.value_utf8}
                      </td>
                      {showGroupColumn && (
                        <td className="tw-p-2 tw-text-muted">{row.groupId}</td>
                      )}
                      <td className="tw-p-2" onClick={(e) => e.stopPropagation()}>
                        {!readonly && (
                          <button
                            onClick={() => handleInlineDelete(row.key_utf8, row.groupId)}
                            className="tw-text-muted hover:tw-text-failed"
                            title="Delete key"
                          >
                            <Trash2 className="tw-h-3 tw-w-3" />
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {scanTruncated && (
              <button
                onClick={handleLoadMore}
                disabled={loadingMore}
                className="tw-w-full tw-py-1.5 tw-text-xs tw-text-muted tw-border tw-border-border tw-rounded hover:tw-bg-panel tw-transition-colors disabled:tw-opacity-50"
              >
                {loadingMore ? 'Loading...' : 'Load more'}
              </button>
            )}
          </div>
        )}

        {scanRows.length === 0 && !scanLoading && (
          <div className="tw-text-center tw-text-muted tw-text-xs tw-py-8">
            No results. Click Scan to list keys.
          </div>
        )}
      </div>

      {/* Confirmation dialog */}
      <Dialog
        isOpen={confirmDelete !== null}
        onClose={() => setConfirmDelete(null)}
        title={`Delete ${confirmDelete?.count || 0} key(s)`}
        description={`Delete ${confirmDelete?.count || 0} key(s) from ${targetLabel}? This cannot be undone.`}
        confirmLabel="Delete"
        destructive
        onConfirm={async () => {
          if (confirmDelete) {
            await confirmDelete.onConfirm();
            setConfirmDelete(null);
          }
        }}
      >
        <div className="tw-text-sm tw-text-text tw-space-y-1">
          {confirmDelete?.keys.slice(0, 10).map((k) => (
            <div key={k} className="tw-font-mono tw-text-xs">{k}</div>
          ))}
          {confirmDelete && confirmDelete.keys.length > 10 && (
            <div className="tw-text-xs tw-text-muted">...and {confirmDelete.keys.length - 10} more</div>
          )}
        </div>
      </Dialog>
    </div>
  );
}
