import { useCallback, useEffect, useState } from "react";
import { addRack, listRacks, removeRack, type RackEntry } from "../api";

export default function RacksTab() {
  const [racks, setRacks] = useState<RackEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const refresh = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      setRacks(await listRacks());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const onAdd = async () => {
    const id = prompt("Rack id?");
    if (!id) return;
    const name = prompt("Rack name? (optional)") ?? "";
    try {
      setBusy(true);
      await addRack({ id, name });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onRemove = async (id: string) => {
    if (!confirm(`Delete rack ${id}?`)) return;
    try {
      setBusy(true);
      await removeRack(id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel space-y-3">
      <div className="flex items-center gap-2">
        <h2 className="text-accent text-sm font-bold flex-1">Racks</h2>
        <button className="btn" disabled={busy} onClick={() => void refresh()}>
          Refresh
        </button>
        <button className="btn btn-primary" disabled={busy} onClick={() => void onAdd()}>
          + Rack
        </button>
      </div>
      {error && <div className="text-red-400 text-sm">Error: {error}</div>}
      {racks == null ? (
        <div className="text-text/60 text-sm">loading…</div>
      ) : racks.length === 0 ? (
        <div className="text-text/60 text-sm">(no racks — add one to begin)</div>
      ) : (
        <table className="w-full text-sm">
          <thead className="text-text/60 text-left">
            <tr>
              <th className="py-1">id</th>
              <th className="py-1">name</th>
              <th className="py-1"></th>
            </tr>
          </thead>
          <tbody>
            {racks.map((r) => (
              <tr key={r.id} className="border-t border-border">
                <td className="py-1 text-accent2">{r.id}</td>
                <td className="py-1">{r.name ?? "—"}</td>
                <td className="py-1 text-right">
                  <button className="btn" onClick={() => void onRemove(r.id)}>
                    delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
