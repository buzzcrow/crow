// Thin typed wrappers over the crowkv-web HTTP API. Every
// function takes an optional `server` (the upstream crowkv-server URL);
// when omitted the backend uses its default registered server.

export interface ServerSelector {
  server?: string;
}

function qs(sel?: ServerSelector, extra?: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams();
  if (sel?.server) params.set("server", sel.server);
  if (extra) {
    for (const [k, v] of Object.entries(extra)) {
      if (v !== undefined && v !== null && v !== "") params.set(k, String(v));
    }
  }
  const s = params.toString();
  return s ? `?${s}` : "";
}

async function jsonOrThrow<T>(resp: Response): Promise<T> {
  if (!resp.ok) {
    let msg = `HTTP ${resp.status}`;
    try {
      const body = await resp.json();
      if (body?.error) msg = `${msg}: ${body.error}`;
    } catch {
      // ignore parse failure
    }
    throw new Error(msg);
  }
  // 204 No Content
  if (resp.status === 204) return undefined as unknown as T;
  return resp.json() as Promise<T>;
}

// ── Cluster snapshot ────────────────────────────────────────────────
export interface ClusterSnapshot {
  servers: unknown[];
  // The full type is large and evolves with the backend; render as JSON.
  [k: string]: unknown;
}

export async function fetchSnapshot(servers: string[]): Promise<ClusterSnapshot> {
  const params = new URLSearchParams();
  for (const s of servers) if (s) params.append("server", s);
  const url = `/api/cluster/snapshot${params.toString() ? `?${params}` : ""}`;
  return jsonOrThrow(await fetch(url));
}

// ── Stores / Groups / Remotes ──────────────────────────────────────
export interface StoreSummary {
  store_id: number;
  name?: string;
  listen_addr?: string;
  group_count?: number;
}

export interface StoreDetail extends StoreSummary {
  groups?: GroupSummary[];
}

export interface GroupSummary {
  group_id: number;
  replica_count?: number;
}

export interface RemoteReplicaInfo {
  replica_id: number;
  endpoint: string;
}

export async function listStores(sel?: ServerSelector): Promise<StoreSummary[]> {
  return jsonOrThrow(await fetch(`/api/stores${qs(sel)}`));
}

export async function addStore(req: Record<string, unknown>, sel?: ServerSelector): Promise<StoreSummary> {
  return jsonOrThrow(
    await fetch(`/api/stores${qs(sel)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    }),
  );
}

export async function getStore(sid: number, sel?: ServerSelector): Promise<StoreDetail> {
  return jsonOrThrow(await fetch(`/api/stores/${sid}${qs(sel)}`));
}

export async function removeStore(sid: number, sel?: ServerSelector): Promise<void> {
  await jsonOrThrow<void>(await fetch(`/api/stores/${sid}${qs(sel)}`, { method: "DELETE" }));
}

export async function listGroups(sid: number, sel?: ServerSelector): Promise<GroupSummary[]> {
  return jsonOrThrow(await fetch(`/api/stores/${sid}/groups${qs(sel)}`));
}

export async function addGroup(sid: number, req: Record<string, unknown>, sel?: ServerSelector): Promise<void> {
  await jsonOrThrow<void>(
    await fetch(`/api/stores/${sid}/groups${qs(sel)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    }),
  );
}

export async function removeGroup(sid: number, gid: number, sel?: ServerSelector): Promise<void> {
  await jsonOrThrow<void>(await fetch(`/api/stores/${sid}/groups/${gid}${qs(sel)}`, { method: "DELETE" }));
}

// ── KV data plane ──────────────────────────────────────────────────
export interface KvGetResponse {
  found: boolean;
  revision: number;
  value_utf8?: string;
  value_hex?: string;
}

export interface KvScanItem {
  key_utf8: string;
  key_hex: string;
  value_utf8: string;
  value_hex: string;
}

export interface KvScanResponse {
  items: KvScanItem[];
  truncated: boolean;
}

export interface KvWriteResponse {
  ok: boolean;
  revision: number;
}

export async function kvGet(sid: number, gid: number, key: string, sel?: ServerSelector): Promise<KvGetResponse> {
  return jsonOrThrow(await fetch(`/api/stores/${sid}/groups/${gid}/kv/get${qs(sel, { key })}`));
}

export async function kvPut(
  sid: number,
  gid: number,
  body: { key: string; value: string; client_id?: number; seq?: number },
  sel?: ServerSelector,
): Promise<KvWriteResponse> {
  return jsonOrThrow(
    await fetch(`/api/stores/${sid}/groups/${gid}/kv/put${qs(sel)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  );
}

export async function kvDelete(
  sid: number,
  gid: number,
  body: { key: string; client_id?: number; seq?: number },
  sel?: ServerSelector,
): Promise<KvWriteResponse> {
  return jsonOrThrow(
    await fetch(`/api/stores/${sid}/groups/${gid}/kv/delete${qs(sel)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  );
}

export async function kvScan(
  sid: number,
  gid: number,
  prefix: string,
  limit: number,
  sel?: ServerSelector,
): Promise<KvScanResponse> {
  return jsonOrThrow(
    await fetch(`/api/stores/${sid}/groups/${gid}/kv/scan${qs(sel, { prefix, limit })}`),
  );
}

// ── Hardware-lifecycle plane: racks / nodes / servers ─────────────

export interface RackEntry {
  id: string;
  name?: string;
}

export interface NodeEntry {
  id: string;
  rack_id: string;
  host: string;
  ssh_port?: number;
  ssh_user?: string;
  ssh_key?: string;
  ssh_password?: string;
}

export interface ServerEntry {
  id: string;
  url: string;
  node_id?: string;
  grpc_url?: string;
  pid?: number;
}

export interface PingResult {
  ok: boolean;
  error?: string;
}

export interface DeployServerRequest {
  id: string;
  node_id: string;
  mgmt_port: number;
  grpc_port: number;
  binary?: string;
}

export async function listRacks(): Promise<RackEntry[]> {
  return jsonOrThrow(await fetch(`/api/racks`));
}
export async function addRack(req: RackEntry): Promise<RackEntry> {
  return jsonOrThrow(
    await fetch(`/api/racks`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(req) }),
  );
}
export async function removeRack(id: string): Promise<void> {
  await jsonOrThrow<void>(await fetch(`/api/racks/${encodeURIComponent(id)}`, { method: "DELETE" }));
}

export async function listNodes(rackId?: string): Promise<NodeEntry[]> {
  const q = rackId ? `?rack_id=${encodeURIComponent(rackId)}` : "";
  return jsonOrThrow(await fetch(`/api/nodes${q}`));
}
export async function addNode(req: NodeEntry): Promise<NodeEntry> {
  return jsonOrThrow(
    await fetch(`/api/nodes`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(req) }),
  );
}
export async function removeNode(id: string): Promise<void> {
  await jsonOrThrow<void>(await fetch(`/api/nodes/${encodeURIComponent(id)}`, { method: "DELETE" }));
}
export async function pingNode(id: string): Promise<PingResult> {
  return jsonOrThrow(await fetch(`/api/nodes/${encodeURIComponent(id)}/ping`, { method: "POST" }));
}

export async function listServers(): Promise<ServerEntry[]> {
  return jsonOrThrow(await fetch(`/api/servers`));
}
export async function registerServer(req: { id: string; url: string }): Promise<ServerEntry> {
  return jsonOrThrow(
    await fetch(`/api/servers`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(req) }),
  );
}
export async function unregisterServer(id: string): Promise<void> {
  await jsonOrThrow<void>(await fetch(`/api/servers/${encodeURIComponent(id)}`, { method: "DELETE" }));
}
export async function deployServer(req: DeployServerRequest): Promise<ServerEntry> {
  return jsonOrThrow(
    await fetch(`/api/servers/deploy`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    }),
  );
}
export async function stopServer(id: string): Promise<{ sent: boolean }> {
  return jsonOrThrow(await fetch(`/api/servers/${encodeURIComponent(id)}/stop`, { method: "POST" }));
}
