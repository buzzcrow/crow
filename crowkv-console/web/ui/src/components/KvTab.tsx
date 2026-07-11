import { useState } from "react";
import { kvDelete, kvGet, kvPut, kvScan, type KvScanItem, type ServerSelector } from "../api";

interface Props {
  server: string;
}

export default function KvTab({ server }: Props) {
  const sel: ServerSelector = server.trim() ? { server: server.trim() } : {};
  const [sid, setSid] = useState<number>(1);
  const [gid, setGid] = useState<number>(1);
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [prefix, setPrefix] = useState("");
  const [limit, setLimit] = useState(100);
  const [getResult, setGetResult] = useState<string>("");
  const [scanResult, setScanResult] = useState<{ items: KvScanItem[]; truncated: boolean } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const wrap = async (fn: () => Promise<void>) => {
    setBusy(true);
    setError(null);
    try {
      await fn();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onGet = () =>
    wrap(async () => {
      const r = await kvGet(sid, gid, key, sel);
      setGetResult(JSON.stringify(r, null, 2));
    });

  const onPut = () =>
    wrap(async () => {
      const r = await kvPut(sid, gid, { key, value, client_id: 1, seq: Date.now() }, sel);
      setGetResult(`PUT ok=${r.ok} revision=${r.revision}`);
    });

  const onDelete = () =>
    wrap(async () => {
      const r = await kvDelete(sid, gid, { key, client_id: 1, seq: Date.now() }, sel);
      setGetResult(`DELETE ok=${r.ok} revision=${r.revision}`);
    });

  const onScan = () =>
    wrap(async () => {
      const r = await kvScan(sid, gid, prefix, limit, sel);
      setScanResult(r);
    });

  return (
    <section className="space-y-4">
      <div className="panel space-y-3">
        <div className="flex flex-wrap gap-3 items-end">
          <Field label="store_id" value={sid} onChange={(v) => setSid(Number(v) || 0)} type="number" width="w-24" />
          <Field label="group_id" value={gid} onChange={(v) => setGid(Number(v) || 0)} type="number" width="w-24" />
          <Field label="key" value={key} onChange={setKey} width="w-72" />
          <Field label="value" value={value} onChange={setValue} width="w-72" />
          <button className="btn btn-primary" disabled={busy || !key} onClick={() => void onGet()}>
            GET
          </button>
          <button className="btn btn-primary" disabled={busy || !key} onClick={() => void onPut()}>
            PUT
          </button>
          <button className="btn" disabled={busy || !key} onClick={() => void onDelete()}>
            DELETE
          </button>
        </div>
        {error && <div className="text-red-400 text-sm">Error: {error}</div>}
        {getResult && <pre className="pre">{getResult}</pre>}
      </div>

      <div className="panel space-y-3">
        <div className="flex flex-wrap gap-3 items-end">
          <Field label="prefix" value={prefix} onChange={setPrefix} width="w-72" />
          <Field label="limit" value={limit} onChange={(v) => setLimit(Number(v) || 0)} type="number" width="w-24" />
          <button className="btn btn-primary" disabled={busy} onClick={() => void onScan()}>
            SCAN
          </button>
        </div>
        {scanResult && (
          <>
            <div className="text-xs text-text/60">
              {scanResult.items.length} item(s){scanResult.truncated ? " · truncated" : ""}
            </div>
            <table className="w-full text-sm">
              <thead className="text-text/60 text-left">
                <tr>
                  <th className="py-1">key</th>
                  <th className="py-1">value</th>
                </tr>
              </thead>
              <tbody>
                {scanResult.items.map((it) => (
                  <tr key={it.key_hex} className="border-t border-border">
                    <td className="py-1 font-mono">{it.key_utf8}</td>
                    <td className="py-1 font-mono break-all">{it.value_utf8}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}
      </div>
      <div className="text-xs text-text/50">
        Note: GET/SCAN currently read from the local replica; values may be stale within one Paxos round-trip.
      </div>
    </section>
  );
}

interface FieldProps {
  label: string;
  value: string | number;
  onChange: (v: string) => void;
  type?: string;
  width?: string;
}

function Field({ label, value, onChange, type = "text", width = "w-48" }: FieldProps) {
  return (
    <label className="flex flex-col gap-1 text-xs text-text/70">
      {label}
      <input className={`input ${width}`} type={type} value={value} onChange={(e) => onChange(e.target.value)} spellCheck={false} />
    </label>
  );
}
