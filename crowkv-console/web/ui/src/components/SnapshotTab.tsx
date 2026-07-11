import { useCallback, useEffect, useState } from "react";
import { fetchSnapshot } from "../api";

interface Props {
  server: string;
}

export default function SnapshotTab({ server }: Props) {
  const [snapshot, setSnapshot] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const reload = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const servers = server.trim() ? [server.trim()] : [];
      const snap = await fetchSnapshot(servers);
      setSnapshot(snap);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [server]);

  useEffect(() => {
    void reload();
  }, [reload]);

  return (
    <section className="space-y-3">
      <div className="flex items-center gap-3">
        <h2 className="text-accent text-sm font-bold">/api/cluster/snapshot</h2>
        <button className="btn btn-primary" disabled={loading} onClick={() => void reload()}>
          {loading ? "Loading…" : "Refresh"}
        </button>
      </div>
      {error && <div className="panel border-red-500/50 text-red-400 text-sm">Error: {error}</div>}
      <pre className="pre">{snapshot ? JSON.stringify(snapshot, null, 2) : loading ? "loading…" : "(empty)"}</pre>
    </section>
  );
}
