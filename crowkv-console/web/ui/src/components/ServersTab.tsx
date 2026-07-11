import { useCallback, useEffect, useState } from "react";
import { deployServer, listNodes, listServers, registerServer, stopServer, unregisterServer, type NodeEntry, type ServerEntry } from "../api";

interface Props {
  onChange?: () => void;
}

export default function ServersTab({ onChange }: Props) {
  const [servers, setServers] = useState<ServerEntry[] | null>(null);
  const [nodes, setNodes] = useState<NodeEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [deployForm, setDeployForm] = useState({ id: "", node_id: "", mgmt_port: 9910, grpc_port: 28001 });
  const [registerForm, setRegisterForm] = useState({ id: "", url: "" });

  const refresh = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      const [ss, ns] = await Promise.all([listServers(), listNodes()]);
      setServers(ss);
      setNodes(ns);
      if (!deployForm.node_id && ns.length > 0) {
        setDeployForm((f) => ({ ...f, node_id: ns[0].id }));
      }
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

  const notify = () => onChange?.();

  const onDeploy = async () => {
    if (!deployForm.id || !deployForm.node_id) {
      setError("server id and node are required");
      return;
    }
    try {
      setBusy(true);
      await deployServer(deployForm);
      setDeployForm({ ...deployForm, id: "", mgmt_port: deployForm.mgmt_port + 1, grpc_port: deployForm.grpc_port + 1 });
      await refresh();
      notify();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onRegister = async () => {
    if (!registerForm.id || !registerForm.url) {
      setError("id and url are required");
      return;
    }
    try {
      setBusy(true);
      await registerServer(registerForm);
      setRegisterForm({ id: "", url: "" });
      await refresh();
      notify();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onStop = async (id: string) => {
    if (!confirm(`Stop server ${id} (SIGTERM)?`)) return;
    try {
      setBusy(true);
      await stopServer(id);
      await refresh();
      notify();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onUnregister = async (id: string) => {
    if (!confirm(`Unregister server ${id} (does not stop the process)?`)) return;
    try {
      setBusy(true);
      await unregisterServer(id);
      await refresh();
      notify();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div className="panel space-y-3">
        <div className="flex items-center gap-2">
          <h2 className="text-accent text-sm font-bold flex-1">Servers</h2>
          <button className="btn" disabled={busy} onClick={() => void refresh()}>
            Refresh
          </button>
        </div>
        {error && <div className="text-red-400 text-sm">Error: {error}</div>}
        {servers == null ? (
          <div className="text-text/60 text-sm">loading…</div>
        ) : servers.length === 0 ? (
          <div className="text-text/60 text-sm">(no servers — register or deploy below)</div>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-text/60 text-left">
              <tr>
                <th className="py-1">id</th>
                <th className="py-1">node</th>
                <th className="py-1">mgmt url</th>
                <th className="py-1">pid</th>
                <th className="py-1"></th>
              </tr>
            </thead>
            <tbody>
              {servers.map((s) => (
                <tr key={s.id} className="border-t border-border">
                  <td className="py-1 text-accent2">{s.id}</td>
                  <td className="py-1">{s.node_id ?? "—"}</td>
                  <td className="py-1 break-all">{s.url}</td>
                  <td className="py-1">{s.pid ?? "—"}</td>
                  <td className="py-1 text-right whitespace-nowrap">
                    {s.pid != null ? (
                      <button className="btn mr-1" onClick={() => void onStop(s.id)}>
                        stop
                      </button>
                    ) : (
                      <button className="btn" onClick={() => void onUnregister(s.id)}>
                        unregister
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="space-y-4">
        <div className="panel space-y-2">
          <h2 className="text-accent text-sm font-bold">Deploy on node</h2>
          {nodes.length === 0 && <div className="text-yellow-400 text-sm">No nodes yet — add one in the Nodes tab first.</div>}
          <label className="block text-sm">
            <span className="text-text/70">server id</span>
            <input className="input w-full mt-1" value={deployForm.id} onChange={(e) => setDeployForm({ ...deployForm, id: e.target.value })} />
          </label>
          <label className="block text-sm">
            <span className="text-text/70">node</span>
            <select
              className="input w-full mt-1"
              value={deployForm.node_id}
              onChange={(e) => setDeployForm({ ...deployForm, node_id: e.target.value })}
              disabled={nodes.length === 0}
            >
              {nodes.map((n) => (
                <option key={n.id} value={n.id}>
                  {n.id} ({n.host})
                </option>
              ))}
            </select>
          </label>
          <div className="grid grid-cols-2 gap-2">
            <label className="block text-sm">
              <span className="text-text/70">mgmt_port</span>
              <input
                className="input w-full mt-1"
                type="number"
                value={deployForm.mgmt_port}
                onChange={(e) => setDeployForm({ ...deployForm, mgmt_port: Number(e.target.value) })}
              />
            </label>
            <label className="block text-sm">
              <span className="text-text/70">grpc_port</span>
              <input
                className="input w-full mt-1"
                type="number"
                value={deployForm.grpc_port}
                onChange={(e) => setDeployForm({ ...deployForm, grpc_port: Number(e.target.value) })}
              />
            </label>
          </div>
          <button className="btn btn-primary" disabled={busy || nodes.length === 0} onClick={() => void onDeploy()}>
            Deploy
          </button>
        </div>

        <div className="panel space-y-2">
          <h2 className="text-accent text-sm font-bold">Register existing</h2>
          <label className="block text-sm">
            <span className="text-text/70">id</span>
            <input className="input w-full mt-1" value={registerForm.id} onChange={(e) => setRegisterForm({ ...registerForm, id: e.target.value })} />
          </label>
          <label className="block text-sm">
            <span className="text-text/70">mgmt url</span>
            <input
              className="input w-full mt-1"
              placeholder="http://127.0.0.1:9910"
              value={registerForm.url}
              onChange={(e) => setRegisterForm({ ...registerForm, url: e.target.value })}
            />
          </label>
          <button className="btn btn-primary" disabled={busy} onClick={() => void onRegister()}>
            Register
          </button>
        </div>
      </div>
    </section>
  );
}
