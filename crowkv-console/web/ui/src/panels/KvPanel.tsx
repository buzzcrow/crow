import { useState, useCallback } from 'react';
import { Search, Info, Database, Trash2, Loader2, Copy, AlertTriangle } from 'lucide-react';
import { cn } from '../utils/cn';
import { useToast } from '../contexts/ToastContext';
import { useActivity } from '../contexts/ActivityContext';
import { Dialog } from '../components/Dialog';
import { kvGet, kvPut, kvDelete, kvScan, KvScanItem, type KvGetResponse } from '../api';

type Mode = 'scan' | 'get' | 'put' | 'delete';

interface KvPanelProps {
  storeId: string;
  groupId: string;
  readonly?: boolean;
}

/**
 * KV data-plane panel for a logical Group selection. Wraps scan/get/put/delete
 * against `/api/stores/:s/groups/:g/kv/*`. Destructive delete requires explicit
 * confirmation.
 */
export function KvPanel({ storeId, groupId, readonly }: KvPanelProps) {
  const { success, error } = useToast();
  const { log } = useActivity();

  const [mode, setMode] = useState<Mode>('scan');
  const [loading, setLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const [scanPrefix, setScanPrefix] = useState('');
  const [scanResults, setScanResults] = useState<KvScanItem[]>([]);
  const [scanTruncated, setScanTruncated] = useState(false);
  const [getKey, setGetKey] = useState('');
  const [getValue, setGetValue] = useState<KvGetResponse | null>(null);
  const [putKey, setPutKey] = useState('');
  const [putValue, setPutValue] = useState('');
  const [deleteKey, setDeleteKey] = useState('');
  const [confirmingDelete, setConfirmingDelete] = useState(false);

  const target = `${storeId}/${groupId}`;

  const switchMode = useCallback((m: Mode) => {
    setMode(m);
    setErrorMessage(null);
    setGetValue(null);
  }, []);

  const handleScan = useCallback(async () => {
    setLoading(true);
    setErrorMessage(null);
    try {
      const result = await kvScan(storeId, groupId, scanPrefix);
      setScanResults(result.items);
      setScanTruncated(result.truncated);
      log({ action: 'KV Scan', target, status: 'Success', message: `Found ${result.items.length} keys` });
      success(`Scanned ${result.items.length} keys`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Scan failed';
      setErrorMessage(msg);
      log({ action: 'KV Scan', target, status: 'Failed', message: msg });
      error(msg);
    } finally {
      setLoading(false);
    }
  }, [storeId, groupId, scanPrefix, target, log, success, error]);

  const handleGet = useCallback(async () => {
    if (!getKey) return;
    setLoading(true);
    setErrorMessage(null);
    try {
      const result = await kvGet(storeId, groupId, getKey);
      setGetValue(result);
      log({ action: 'KV Get', target, status: 'Success', message: `key: "${getKey}"` });
      success(result.found ? `Retrieved "${getKey}"` : `Key "${getKey}" not found`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Get failed';
      setErrorMessage(msg);
      log({ action: 'KV Get', target, status: 'Failed', message: msg });
      error(msg);
    } finally {
      setLoading(false);
    }
  }, [storeId, groupId, getKey, target, log, success, error]);

  const handlePut = useCallback(async () => {
    if (!putKey || !putValue) return;
    setLoading(true);
    setErrorMessage(null);
    try {
      await kvPut(storeId, groupId, { key: putKey, value: putValue });
      log({ action: 'KV Put', target, status: 'Success', message: `key: "${putKey}"` });
      success(`Key written: "${putKey}"`);
      setPutKey('');
      setPutValue('');
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Put failed';
      setErrorMessage(msg);
      log({ action: 'KV Put', target, status: 'Failed', message: msg });
      error(msg);
    } finally {
      setLoading(false);
    }
  }, [storeId, groupId, putKey, putValue, target, log, success, error]);

  const handleDelete = useCallback(async () => {
    if (!deleteKey) return;
    setLoading(true);
    setErrorMessage(null);
    setConfirmingDelete(false);
    try {
      await kvDelete(storeId, groupId, { key: deleteKey });
      log({ action: 'KV Delete', target, status: 'Success', message: `key: "${deleteKey}"` });
      success(`Key deleted: "${deleteKey}"`);
      setDeleteKey('');
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Delete failed';
      setErrorMessage(msg);
      log({ action: 'KV Delete', target, status: 'Failed', message: msg });
      error(msg);
    } finally {
      setLoading(false);
    }
  }, [storeId, groupId, deleteKey, target, log, success, error]);

  const copy = useCallback(
    (text: string) => {
      navigator.clipboard.writeText(text).then(
        () => success('Copied to clipboard'),
        () => error('Copy failed'),
      );
    },
    [success, error],
  );

  const modes: { id: Mode; label: string; icon: React.ReactNode }[] = [
    { id: 'scan', label: 'Scan', icon: <Search className="tw-h-3 tw-w-3" /> },
    { id: 'get', label: 'Get', icon: <Info className="tw-h-3 tw-w-3" /> },
    ...(readonly
      ? []
      : ([
          { id: 'put', label: 'Put', icon: <Database className="tw-h-3 tw-w-3" /> },
          { id: 'delete', label: 'Delete', icon: <Trash2 className="tw-h-3 tw-w-3" /> },
        ] as { id: Mode; label: string; icon: React.ReactNode }[])),
  ];

  return (
    <div className="tw-p-3 tw-space-y-3">
      <div className="tw-flex tw-gap-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-p-0.5">
        {modes.map((m) => (
          <button
            key={m.id}
            onClick={() => switchMode(m.id)}
            className={cn(
              'tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-rounded tw-text-xs tw-transition-colors',
              mode === m.id ? 'tw-bg-accent tw-text-bg' : 'tw-text-muted hover:tw-text-text',
            )}
          >
            {m.icon}
            {m.label}
          </button>
        ))}
      </div>

      {errorMessage && (
        <div className="tw-flex tw-items-start tw-gap-2 tw-p-2 tw-rounded tw-bg-failed/10 tw-border tw-border-failed/30 tw-text-failed tw-text-xs">
          <AlertTriangle className="tw-h-4 tw-w-4 tw-flex-shrink-0" />
          <span>{errorMessage}</span>
        </div>
      )}

      {mode === 'scan' && (
        <div className="tw-space-y-3">
          <div className="tw-flex tw-gap-2">
            <input
              type="text"
              value={scanPrefix}
              onChange={(e) => setScanPrefix(e.target.value)}
              placeholder="Key prefix (empty = all)"
              className="tw-flex-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted"
              onKeyDown={(e) => e.key === 'Enter' && handleScan()}
            />
            <button
              onClick={handleScan}
              disabled={loading}
              className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs disabled:tw-opacity-50"
            >
              {loading && <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" />}
              Scan
            </button>
          </div>
          {scanResults.length > 0 && (
            <div className="tw-space-y-1">
              <span className="tw-text-xs tw-text-muted">
                {scanResults.length} result(s){scanTruncated && ' (truncated)'}
              </span>
              <div className="tw-border tw-border-border tw-rounded tw-max-h-64 tw-overflow-y-auto">
                <table className="tw-w-full tw-text-xs">
                  <thead className="tw-bg-bg tw-sticky tw-top-0">
                    <tr>
                      <th className="tw-text-left tw-p-2 tw-text-muted tw-border-b tw-border-border">Key</th>
                      <th className="tw-text-left tw-p-2 tw-text-muted tw-border-b tw-border-border">Value</th>
                      <th className="tw-w-8 tw-border-b tw-border-border" />
                    </tr>
                  </thead>
                  <tbody className="tw-divide-y tw-divide-border">
                    {scanResults.map((item, idx) => (
                      <tr key={idx} className="hover:tw-bg-bg/50">
                        <td className="tw-p-2 tw-font-mono tw-truncate tw-max-w-[120px]" title={item.key_utf8}>
                          {item.key_utf8}
                        </td>
                        <td className="tw-p-2 tw-font-mono tw-truncate tw-max-w-[120px]" title={item.value_utf8}>
                          {item.value_utf8}
                        </td>
                        <td className="tw-p-2">
                          <button onClick={() => copy(item.value_utf8)} className="tw-text-muted hover:tw-text-text" title="Copy value">
                            <Copy className="tw-h-3 tw-w-3" />
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {mode === 'get' && (
        <div className="tw-space-y-3">
          <div className="tw-flex tw-gap-2">
            <input
              type="text"
              value={getKey}
              onChange={(e) => setGetKey(e.target.value)}
              placeholder="Key to get"
              className="tw-flex-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted"
              onKeyDown={(e) => e.key === 'Enter' && handleGet()}
            />
            <button
              onClick={handleGet}
              disabled={loading || !getKey}
              className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs disabled:tw-opacity-50"
            >
              {loading && <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" />}
              Get
            </button>
          </div>
          {getValue && (
            <div className="tw-border tw-border-border tw-rounded tw-p-2 tw-bg-bg tw-text-xs">
              {getValue.found ? (
                <>
                  <div className="tw-flex tw-items-center tw-justify-between tw-mb-1">
                    <span className="tw-text-muted">
                      Value ({(getValue.value_utf8 || '').length} bytes)
                    </span>
                    <button onClick={() => copy(getValue.value_utf8 || '')} className="tw-flex tw-items-center tw-gap-1 tw-text-accent hover:tw-underline">
                      <Copy className="tw-h-3 tw-w-3" /> Copy
                    </button>
                  </div>
                  <div className="tw-font-mono tw-break-all tw-text-text">{getValue.value_utf8}</div>
                  <div className="tw-text-[10px] tw-text-muted tw-mt-1">Revision: {getValue.revision}</div>
                </>
              ) : (
                <span className="tw-text-muted">Key not found</span>
              )}
            </div>
          )}
        </div>
      )}

      {mode === 'put' && !readonly && (
        <div className="tw-space-y-2">
          <input
            type="text"
            value={putKey}
            onChange={(e) => setPutKey(e.target.value)}
            placeholder="Key"
            className="tw-w-full tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted"
          />
          <textarea
            value={putValue}
            onChange={(e) => setPutValue(e.target.value)}
            placeholder="Value"
            rows={4}
            className="tw-w-full tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted tw-resize-none"
          />
          <button
            onClick={handlePut}
            disabled={loading || !putKey || !putValue}
            className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs disabled:tw-opacity-50"
          >
            {loading && <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" />}
            Put
          </button>
        </div>
      )}

      {mode === 'delete' && !readonly && (
        <div className="tw-flex tw-gap-2">
          <input
            type="text"
            value={deleteKey}
            onChange={(e) => setDeleteKey(e.target.value)}
            placeholder="Key to delete"
            className="tw-flex-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted"
          />
          <button
            onClick={() => setConfirmingDelete(true)}
            disabled={loading || !deleteKey}
            className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-bg-failed tw-text-bg tw-rounded tw-text-xs disabled:tw-opacity-50"
          >
            {loading && <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" />}
            Delete
          </button>
        </div>
      )}

      <Dialog
        isOpen={confirmingDelete}
        onClose={() => setConfirmingDelete(false)}
        title="Delete key"
        description={`Delete key "${deleteKey}" from group ${groupId}? This cannot be undone.`}
        confirmLabel="Delete"
        destructive
        onConfirm={handleDelete}
      >
        <p className="tw-text-sm tw-text-text">
          This permanently removes <span className="tw-font-mono">{deleteKey}</span>.
        </p>
      </Dialog>
    </div>
  );
}
