import { useCallback, useEffect, useState } from "react";
import SnapshotTab from "./components/SnapshotTab";
import StoresTab from "./components/StoresTab";
import KvTab from "./components/KvTab";
import RacksTab from "./components/RacksTab";
import NodesTab from "./components/NodesTab";
import ServersTab from "./components/ServersTab";
import { listServers, type ServerEntry } from "./api";

type TabId = "snapshot" | "racks" | "nodes" | "servers" | "stores" | "kv" | "swagger";

const TABS: { id: TabId; label: string }[] = [
  { id: "snapshot", label: "Snapshot" },
  { id: "racks", label: "Racks" },
  { id: "nodes", label: "Nodes" },
  { id: "servers", label: "Servers" },
  { id: "stores", label: "Stores" },
  { id: "kv", label: "KV" },
  { id: "swagger", label: "Swagger" },
];

const SERVER_KEY = "crowkv.console.server";
const CUSTOM = "__custom__";

export default function App() {
  const [tab, setTab] = useState<TabId>("snapshot");
  const [server, setServer] = useState<string>(() => localStorage.getItem(SERVER_KEY) ?? "");
  const [registered, setRegistered] = useState<ServerEntry[]>([]);
  const [custom, setCustom] = useState<boolean>(false);

  const reloadServers = useCallback(async () => {
    try {
      const list = await listServers();
      setRegistered(list);
      // If the saved server is not in the registry, treat it as a custom URL.
      if (server && !list.some((s) => s.url === server)) {
        setCustom(true);
      }
    } catch {
      // ignore — backend may be starting up
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    void reloadServers();
  }, [reloadServers]);

  useEffect(() => {
    localStorage.setItem(SERVER_KEY, server);
  }, [server]);

  return (
    <div className="min-h-full flex flex-col">
      <header className="border-b border-border px-6 py-3 flex items-center gap-4 bg-panel">
        <h1 className="text-accent text-base font-bold tracking-wide">CrowKV Console</h1>
        <nav className="flex gap-1 ml-4">
          {TABS.map((t) => (
            <button
              key={t.id}
              className={`tab ${tab === t.id ? "tab-active" : "text-text/70 hover:text-text"}`}
              onClick={() => {
                if (t.id === "swagger") {
                  window.open("/api/swagger/", "_blank");
                  return;
                }
                setTab(t.id);
              }}
            >
              {t.label}
            </button>
          ))}
        </nav>
        <div className="ml-auto flex items-center gap-2">
          <label htmlFor="server" className="text-sm text-text/70">
            Server:
          </label>
          {custom ? (
            <input
              id="server"
              className="input w-80"
              placeholder="http://127.0.0.1:9910"
              value={server}
              onChange={(e) => setServer(e.target.value)}
              spellCheck={false}
            />
          ) : (
            <select
              id="server"
              className="input w-80"
              value={server}
              onChange={(e) => {
                if (e.target.value === CUSTOM) {
                  setCustom(true);
                  setServer("");
                } else {
                  setServer(e.target.value);
                }
              }}
            >
              <option value="">(default — first registered)</option>
              {registered.map((s) => (
                <option key={s.id} value={s.url}>
                  {s.id} — {s.url}
                </option>
              ))}
              <option value={CUSTOM}>(custom URL…)</option>
            </select>
          )}
          {custom && (
            <button className="btn" onClick={() => setCustom(false)}>
              use registered
            </button>
          )}
          <button className="btn" title="Reload server list" onClick={() => void reloadServers()}>
            ↻
          </button>
        </div>
      </header>

      <main className="flex-1 px-6 py-4 overflow-auto">
        {tab === "snapshot" && <SnapshotTab server={server} />}
        {tab === "racks" && <RacksTab />}
        {tab === "nodes" && <NodesTab />}
        {tab === "servers" && <ServersTab onChange={() => void reloadServers()} />}
        {tab === "stores" && <StoresTab server={server} />}
        {tab === "kv" && <KvTab server={server} />}
      </main>

      <footer className="border-t border-border px-6 py-2 text-xs text-text/50">
        crowkv-web · React + Vite · {server ? `target=${server}` : "no server set (uses backend default)"}
      </footer>
    </div>
  );
}
