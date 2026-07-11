import { useCallback, useEffect, useState } from "react";
import {
  addGroup,
  addStore,
  getStore,
  listGroups,
  listStores,
  removeGroup,
  removeStore,
  type GroupSummary,
  type ServerSelector,
  type StoreDetail,
  type StoreSummary,
} from "../api";

interface Props {
  server: string;
}

export default function StoresTab({ server }: Props) {
  const sel: ServerSelector = server.trim() ? { server: server.trim() } : {};
  const [stores, setStores] = useState<StoreSummary[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [selected, setSelected] = useState<number | null>(null);
  const [detail, setDetail] = useState<StoreDetail | null>(null);
  const [groups, setGroups] = useState<GroupSummary[] | null>(null);

  const refresh = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      setStores(await listStores(sel));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [server]);

  const loadDetail = useCallback(
    async (sid: number) => {
      setError(null);
      setSelected(sid);
      try {
        setDetail(await getStore(sid, sel));
        setGroups(await listGroups(sid, sel));
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [server],
  );

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const onCreateStore = async () => {
    const name = prompt("Store name?");
    if (!name) return;
    try {
      setBusy(true);
      await addStore({ name }, sel);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onDeleteStore = async (sid: number) => {
    if (!confirm(`Delete store ${sid}?`)) return;
    try {
      setBusy(true);
      await removeStore(sid, sel);
      if (selected === sid) {
        setSelected(null);
        setDetail(null);
        setGroups(null);
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onAddGroup = async () => {
    if (selected == null) return;
    const gidStr = prompt("New group_id (integer)?");
    if (!gidStr) return;
    const gid = Number(gidStr);
    if (!Number.isFinite(gid)) return;
    try {
      await addGroup(selected, { group_id: gid }, sel);
      await loadDetail(selected);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const onRemoveGroup = async (gid: number) => {
    if (selected == null) return;
    if (!confirm(`Delete group ${gid} from store ${selected}?`)) return;
    try {
      await removeGroup(selected, gid, sel);
      await loadDetail(selected);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div className="panel space-y-3">
        <div className="flex items-center gap-2">
          <h2 className="text-accent text-sm font-bold flex-1">Stores</h2>
          <button className="btn" disabled={busy} onClick={() => void refresh()}>
            Refresh
          </button>
          <button className="btn btn-primary" disabled={busy} onClick={() => void onCreateStore()}>
            + New
          </button>
        </div>
        {error && <div className="text-red-400 text-sm">Error: {error}</div>}
        {stores == null ? (
          <div className="text-text/60 text-sm">loading…</div>
        ) : stores.length === 0 ? (
          <div className="text-text/60 text-sm">(no stores)</div>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-text/60 text-left">
              <tr>
                <th className="py-1">store_id</th>
                <th className="py-1">name</th>
                <th className="py-1">listen_addr</th>
                <th className="py-1">groups</th>
                <th className="py-1"></th>
              </tr>
            </thead>
            <tbody>
              {stores.map((s) => (
                <tr key={s.store_id} className="border-t border-border hover:bg-bg/40 cursor-pointer" onClick={() => void loadDetail(s.store_id)}>
                  <td className="py-1 text-accent2">{s.store_id}</td>
                  <td className="py-1">{s.name ?? "—"}</td>
                  <td className="py-1">{s.listen_addr ?? "—"}</td>
                  <td className="py-1">{s.group_count ?? "—"}</td>
                  <td className="py-1 text-right">
                    <button
                      className="btn"
                      onClick={(e) => {
                        e.stopPropagation();
                        void onDeleteStore(s.store_id);
                      }}
                    >
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
        <div className="flex items-center gap-2">
          <h2 className="text-accent text-sm font-bold flex-1">
            {selected == null ? "Detail" : `Store ${selected}`}
          </h2>
          {selected != null && (
            <>
              <button className="btn" onClick={() => void loadDetail(selected)}>
                Refresh
              </button>
              <button className="btn btn-primary" onClick={() => void onAddGroup()}>
                + Group
              </button>
            </>
          )}
        </div>
        {selected == null && <div className="text-text/60 text-sm">Click a store on the left to inspect.</div>}
        {selected != null && detail && (
          <>
            <pre className="pre">{JSON.stringify(detail, null, 2)}</pre>
            <h3 className="text-accent2 text-sm">Groups</h3>
            {groups == null ? (
              <div className="text-text/60 text-sm">loading…</div>
            ) : groups.length === 0 ? (
              <div className="text-text/60 text-sm">(no groups)</div>
            ) : (
              <table className="w-full text-sm">
                <thead className="text-text/60 text-left">
                  <tr>
                    <th className="py-1">group_id</th>
                    <th className="py-1">replicas</th>
                    <th className="py-1"></th>
                  </tr>
                </thead>
                <tbody>
                  {groups.map((g) => (
                    <tr key={g.group_id} className="border-t border-border">
                      <td className="py-1 text-accent2">{g.group_id}</td>
                      <td className="py-1">{g.replica_count ?? "—"}</td>
                      <td className="py-1 text-right">
                        <button className="btn" onClick={() => void onRemoveGroup(g.group_id)}>
                          delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        )}
      </div>
    </section>
  );
}
