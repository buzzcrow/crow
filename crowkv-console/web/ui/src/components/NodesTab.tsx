import { useCallback, useEffect, useState } from "react";
import { addNode, listNodes, listRacks, pingNode, removeNode, type NodeEntry, type RackEntry } from "../api";

export default function NodesTab() {
  const [racks, setRacks] = useState<RackEntry[]>([]);
  const [nodes, setNodes] = useState<NodeEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [pingState, setPingState] = useState<Record<string, string>>({});
  const [form, setForm] = useState<NodeEntry>({ id: "", rack_id: "", host: "127.0.0.1", ssh_user: "" });

  const refresh = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      const [rs, ns] = await Promise.all([listRacks(), listNodes()]);
      setRacks(rs);
      setNodes(ns);
      if (!form.rack_id && rs.length > 0) setForm((f) => ({ ...f, rack_id: rs[0].id }));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const onAdd = async () => {
    if (!form.id || !form.rack_id || !form.host) {
      setError("id, rack_id and host are required");
      return;
    }
    try {
      setBusy(true);
      await addNode({
        id: form.id,
        rack_id: form.rack_id,
        host: form.host,
        ssh_port: form.ssh_port,
        ssh_user: form.ssh_user ?? "",
        ssh_key: form.ssh_key || undefined,
        ssh_password: form.ssh_password || undefined,
      });
      setForm({ id: "", rack_id: form.rack_id, host: "127.0.0.1", ssh_user: "" });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onRemove = async (id: string) => {
    if (!confirm(`Delete node ${id}?`)) return;
    try {
      setBusy(true);
      await removeNode(id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onPing = async (id: string) => {
    setPingState((s) => ({ ...s, [id]: "…" }));
    try {
      const r = await pingNode(id);
      setPingState((s) => ({ ...s, [id]: r.ok ? "ok" : `fail: ${r.error ?? "unknown"}` }));
    } catch (e) {
      setPingState((s) => ({ ...s, [id]: `error: ${e instanceof Error ? e.message : String(e)}` }));
    }
  };

  return (
    <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div className="panel space-y-3">
        <div className="flex items-center gap-2">
          <h2 className="text-accent text-sm font-bold flex-1">Nodes</h2>
          <button className="btn" disabled={busy} onClick={() => void refresh()}>
            Refresh
          </button>
        </div>
        {error && <div className="text-red-400 text-sm">Error: {error}</div>}
        {nodes == null ? (
          <div className="text-text/60 text-sm">loading…</div>
        ) : nodes.length === 0 ? (
          <div className="text-text/60 text-sm">(no nodes)</div>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-text/60 text-left">
              <tr>
                <th className="py-1">id</th>
                <th className="py-1">rack</th>
                <th className="py-1">host</th>
                <th className="py-1">ssh</th>
                <th className="py-1">ping</th>
                <th className="py-1"></th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((n) => (
                <tr key={n.id} className="border-t border-border">
                  <td className="py-1 text-accent2">{n.id}</td>
                  <td className="py-1">{n.rack_id}</td>
                  <td className="py-1">{n.host}</td>
                  <td className="py-1">{n.ssh_user ? `${n.ssh_user}@${n.ssh_port ?? 22}` : "(local)"}</td>
                  <td className="py-1 text-text/70">{pingState[n.id] ?? ""}</td>
                  <td className="py-1 text-right">
                    <button className="btn mr-1" onClick={() => void onPing(n.id)}>
                      ping
                    </button>
                    <button className="btn" onClick={() => void onRemove(n.id)}>
                      delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="panel space-y-3">
        <h2 className="text-accent text-sm font-bold">Add node</h2>
        {racks.length === 0 && (
          <div className="text-yellow-400 text-sm">No racks yet — add one in the Racks tab first.</div>
        )}
        <label className="block text-sm">
          <span className="text-text/70">id</span>
          <input className="input w-full mt-1" value={form.id} onChange={(e) => setForm({ ...form, id: e.target.value })} />
        </label>
        <label className="block text-sm">
          <span className="text-text/70">rack</span>
          <select
            className="input w-full mt-1"
            value={form.rack_id}
            onChange={(e) => setForm({ ...form, rack_id: e.target.value })}
            disabled={racks.length === 0}
          >
            {racks.map((r) => (
              <option key={r.id} value={r.id}>
                {r.id}
                {r.name ? ` (${r.name})` : ""}
              </option>
            ))}
          </select>
        </label>
        <label className="block text-sm">
          <span className="text-text/70">host</span>
          <input className="input w-full mt-1" value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} />
        </label>
        <details className="text-sm">
          <summary className="text-text/70 cursor-pointer">SSH (optional — leave blank for local fork)</summary>
          <div className="space-y-2 mt-2">
            <label className="block">
              <span className="text-text/70">ssh_user</span>
              <input className="input w-full mt-1" value={form.ssh_user ?? ""} onChange={(e) => setForm({ ...form, ssh_user: e.target.value })} />
            </label>
            <label className="block">
              <span className="text-text/70">ssh_port</span>
              <input
                className="input w-full mt-1"
                type="number"
                value={form.ssh_port ?? ""}
                onChange={(e) => setForm({ ...form, ssh_port: Number(e.target.value) || undefined })}
              />
            </label>
            <label className="block">
              <span className="text-text/70">ssh_key (path)</span>
              <input className="input w-full mt-1" value={form.ssh_key ?? ""} onChange={(e) => setForm({ ...form, ssh_key: e.target.value })} />
            </label>
            <label className="block">
              <span className="text-text/70">ssh_password</span>
              <input
                className="input w-full mt-1"
                type="password"
                value={form.ssh_password ?? ""}
                onChange={(e) => setForm({ ...form, ssh_password: e.target.value })}
              />
            </label>
          </div>
        </details>
        <button className="btn btn-primary" disabled={busy || racks.length === 0} onClick={() => void onAdd()}>
          Add Node
        </button>
      </div>
    </section>
  );
}
