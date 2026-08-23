// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Thin typed wrappers over the crow-web HTTP API.
// Separated into physical (infrastructure) and logical (cluster) endpoints as per design-console.md
import type {
  Rack,
  Node,
  ServerProcess,
  NodeStore,
  NodeGroup,
  StoreView,
  GroupView,
  ReplicaView,
  MetricsResponse,
  DiskdbInstanceInfo,
  CapacityUsageResponse,
  ScanStatusResponse,
  RecalcResultResponse,
  CompactResultResponse,
  RebuildResultResponse,
  DiskdbDeployResult,
  StopResult,
  HardwareCapacitySummary,
} from './types';

/**
 * Flat shape accepted by `POST /api/nodes` (backend `NodeEntry` in
 * `crow-console-shared::config`). The tagged `SshCreds` enum is a
 * response-only shape; on the wire the create body uses these flat
 * fields.
 */
export interface AddNodeRequest {
  id: number;
  rack_id: number;
  host: string;
  ssh_port?: number;
  ssh_user?: string;
  ssh_key?: string;
  ssh_password?: string;
}

/**
 * Configurable API base. Every wrapper builds its URL with a literal
 * `/api` prefix; `setApiBase` lets an embedding host (see `App` /
 * `CrowConsoleProps.apiPrefix`) re-root all data-plane traffic under a
 * different path (e.g. behind a reverse proxy). Default `/api` is a no-op.
 */
let apiBase = '/api';

export function setApiBase(prefix?: string): void {
  const trimmed = (prefix ?? '').trim().replace(/\/+$/, '');
  apiBase = trimmed || '/api';
}

export function getApiBase(): string {
  return apiBase;
}

/** Rewrite a literal `/api`-rooted path onto the configured `apiBase`. */
function resolveUrl(url: string): string {
  if (apiBase === '/api') return url;
  if (url === '/api') return apiBase;
  if (url.startsWith('/api/') || url.startsWith('/api?')) {
    return apiBase + url.slice('/api'.length);
  }
  return url;
}

/**
 * Helper function to build query strings
 */
function qs(params?: Record<string, string | number | boolean | undefined>): string {
  if (!params) return '';
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      searchParams.set(key, String(value));
    }
  }
  const queryString = searchParams.toString();
  return queryString ? `?${queryString}` : '';
}

/**
 * Helper function to handle JSON responses and errors
 */
async function jsonOrThrow<T>(resp: Response): Promise<T> {
  if (!resp.ok) {
    let errorMessage = `HTTP ${resp.status}`;
    try {
      const errorBody = await resp.json();
      if (errorBody?.error) {
        errorMessage = `${errorMessage}: ${errorBody.error}`;
      }
    } catch {
      // Ignore parse errors for non-JSON responses
    }
    throw new Error(errorMessage);
  }

  // Handle 204 No Content responses
  if (resp.status === 204) {
    return undefined as unknown as T;
  }

  return resp.json() as Promise<T>;
}

// Request deduplication - track in-flight requests
interface InflightRequest {
  promise: Promise<any>;
  controller: AbortController;
}

const inflightRequests = new Map<string, InflightRequest>();

/**
 * Generate a cache key for request deduplication
 */
function getRequestKey(url: string, method: string = 'GET', body?: string): string {
  return `${method}:${url}:${body || ''}`;
}

/**
 * Wait for a delay (for retries)
 */
function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check if an error is retryable (network errors, 5xx, 429)
 */
function isRetryableError(error: unknown): boolean {
  if (error instanceof Error) {
    // Network errors
    if (error.name === 'AbortError') return false; // Don't retry aborted requests
    if (error.name === 'TypeError' && error.message.includes('fetch')) return true;
  }
  return false;
}

/**
 * Check if a response status is retryable
 */
function isRetryableStatus(status: number): boolean {
  return status >= 500 || status === 429;
}

export interface RequestOptions {
  signal?: AbortSignal;
  /** Number of retries for transient errors (default: 0) */
  retries?: number;
  /** Base delay for exponential backoff in ms (default: 100) */
  retryDelay?: number;
  /** Skip request deduplication (default: false) */
  skipDeduplication?: boolean;
}

/**
 * Enhanced fetch wrapper with deduplication, retries, and AbortController support
 */
async function fetchWithOptions(
  url: string,
  init: RequestInit & RequestOptions = {}
): Promise<Response> {
  const {
    signal,
    retries = 0,
    retryDelay = 100,
    skipDeduplication = false,
    ...fetchInit
  } = init;

  const method = fetchInit.method || 'GET';
  const body = typeof fetchInit.body === 'string' ? fetchInit.body : undefined;
  const requestKey = getRequestKey(url, method, body);

  // Check for existing in-flight request for deduplication
  if (!skipDeduplication && method === 'GET') {
    const existing = inflightRequests.get(requestKey);
    if (existing) {
      // If we have a signal, chain it to the existing controller
      if (signal) {
        signal.addEventListener('abort', () => {
          existing.controller.abort();
        });
      }
      return existing.promise.then(response => response.clone());
    }
  }

  // Create a new AbortController for this request
  const controller = new AbortController();
  if (signal) {
    // Chain the user-provided signal to our controller
    if (signal.aborted) {
      controller.abort();
    } else {
      signal.addEventListener('abort', () => controller.abort());
    }
  }

  let lastError: unknown;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(resolveUrl(url), {
        ...fetchInit,
        signal: controller.signal,
      });

      // If successful, remove from in-flight and return
      if (response.ok || !isRetryableStatus(response.status) || attempt >= retries) {
        if (!skipDeduplication && method === 'GET') {
          inflightRequests.delete(requestKey);
        }
        return response;
      }

      // If retryable status, wait and retry
      lastError = new Error(`HTTP ${response.status}`);
      const waitTime = retryDelay * Math.pow(2, attempt); // Exponential backoff
      await delay(waitTime);
    } catch (error) {
      lastError = error;

      // Check if we should retry
      if (attempt >= retries || !isRetryableError(error)) {
        if (!skipDeduplication && method === 'GET') {
          inflightRequests.delete(requestKey);
        }
        throw error;
      }

      // Wait before retrying
      const waitTime = retryDelay * Math.pow(2, attempt); // Exponential backoff
      await delay(waitTime);
    }
  }

  // This line should never be reached due to the throw in the loop
  throw lastError;
}

// ─────────────────────────────────────────────────────────────────────
// Physical (Infrastructure) Endpoints: /api/racks, /api/nodes
// Used for hardware lifecycle management and debugging
// ─────────────────────────────────────────────────────────────────────

/**
 * List all racks with optional recursive depth.
 *
 * @param recursive How many levels of children to include: 0 = just racks,
 *   1 = racks + nodes, etc.
 *
 * Backend protocol quirk: at `recursive=0` (or absent) the backend returns
 * a flat `Vec<RackEntry>`; at `recursive>=1` it switches to an envelope
 * `{ items, truncated_at }` (`app/crow-web/src/lifecycle.rs`
 * `http_list_racks`). We normalize both shapes back to `Rack[]` so every
 * caller — in particular `usePhysicalTree` — sees the rack a user just
 * created and renders it in the sidebar.
 */
export async function listRacks(recursive?: number, options?: RequestOptions): Promise<Rack[]> {
  const url = `/api/racks${qs({ recursive })}`;
  const body = await jsonOrThrow<unknown>(await fetchWithOptions(url, { ...options, method: 'GET' }));
  if (Array.isArray(body)) return body as Rack[];
  if (body && typeof body === 'object' && Array.isArray((body as { items?: unknown }).items)) {
    return (body as { items: Rack[] }).items;
  }
  return [];
}

/**
 * Get a specific rack by ID
 */
export async function getRack(rackId: number, recursive?: number, options?: RequestOptions): Promise<Rack> {
  const url = `/api/racks/${encodeURIComponent(rackId)}${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Create a new rack
 */
export async function addRack(req: { id: number; name?: string }, options?: RequestOptions): Promise<Rack> {
  const body = JSON.stringify(req);
  const url = `/api/racks`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      skipDeduplication: true, // POST requests shouldn't be deduplicated
    })
  );
}

/**
 * Delete a rack
 */
export async function removeRack(rackId: number, options?: RequestOptions): Promise<void> {
  const url = `/api/racks/${encodeURIComponent(rackId)}`;
  await jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'DELETE', skipDeduplication: true }));
}

/**
 * List all nodes across all racks, or filter by rack ID
 */
export async function listNodes(rackId?: string, recursive?: number, options?: RequestOptions): Promise<Node[]> {
  const url = `/api/nodes${qs({ rack_id: rackId, recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Get a specific node by ID
 */
export async function getNode(nodeId: number, recursive?: number, options?: RequestOptions): Promise<Node> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Create a new node. Matches backend `NodeEntry` shape exactly: `ssh_user=""`
 * (default) selects the local-fork lifecycle used by integration tests.
 */
export async function addNode(req: AddNodeRequest, options?: RequestOptions): Promise<Node> {
  const body = JSON.stringify({
    ssh_port: 22,
    ssh_user: '',
    ...req,
  });
  const url = `/api/nodes`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      skipDeduplication: true,
    })
  );
}

/**
 * Delete a node
 */
export async function removeNode(nodeId: number, options?: RequestOptions): Promise<void> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}`;
  await jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'DELETE', skipDeduplication: true }));
}

/**
 * Ping a node to check reachability
 */
export async function pingNode(nodeId: number, options?: RequestOptions): Promise<{ ok: boolean; error?: string }> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/ping`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      skipDeduplication: true,
    })
  );
}

/**
 * Deploy a crow-kv-server instance to a node
 */
export async function deployServer(
  nodeId: number,
  req: { rest_port: number; rpc_port: number; binary?: string },
  options?: RequestOptions
): Promise<ServerProcess> {
  const body = JSON.stringify(req);
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/server/deploy`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      skipDeduplication: true,
    })
  );
}

/**
 * Start a previously deployed server on a node
 */
export async function startServer(nodeId: number, options?: RequestOptions): Promise<ServerProcess> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/server/start`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      skipDeduplication: true,
    })
  );
}

/**
 * Stop a running server on a node
 */
export async function stopServer(nodeId: number, options?: RequestOptions): Promise<ServerProcess> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/server/stop`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      skipDeduplication: true,
    })
  );
}

/**
 * Restart a previously deployed server on a node.
 */
export async function restartServer(nodeId: number, options?: RequestOptions): Promise<ServerProcess> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/server/restart`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      skipDeduplication: true,
    })
  );
}

/**
 * Get the server process details for a node
 */
export async function getServer(nodeId: number, options?: RequestOptions): Promise<ServerProcess> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/server`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Delete the server deployment record for a node
 */
export async function removeServer(nodeId: number, options?: RequestOptions): Promise<void> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/server`;
  await jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'DELETE', skipDeduplication: true }));
}

/**
 * Get the OpenAPI spec for a node's crow-kv-server instance
 */
export async function getNodeOpenApi(nodeId: number, options?: RequestOptions): Promise<Record<string, any>> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/openapi.json`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * List stores on a specific node (physical view)
 */
export async function listNodeStores(nodeId: number, recursive?: number, options?: RequestOptions): Promise<NodeStore[]> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/stores${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Get a specific store on a node (physical view)
 */
export async function getNodeStore(nodeId: number, storeId: string, recursive?: number, options?: RequestOptions): Promise<NodeStore> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/stores/${encodeURIComponent(storeId)}${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * List groups on a specific store on a node (physical view)
 */
export async function listNodeGroups(nodeId: number, storeId: string, recursive?: number, options?: RequestOptions): Promise<NodeGroup[]> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/stores/${encodeURIComponent(storeId)}/groups${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Get a specific group on a node (physical view), including local and remote replicas
 */
export async function getNodeGroup(
  nodeId: number,
  storeId: string,
  groupId: string,
  recursive?: number,
  options?: RequestOptions
): Promise<NodeGroup> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(
    groupId
  )}${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

// ─────────────────────────────────────────────────────────────────────
// Logical (Cluster) Endpoints: /api/stores
// Used for cluster management and KV operations
// ─────────────────────────────────────────────────────────────────────

/**
 * List all cluster-wide stores with optional recursive depth
 */
export async function listStores(recursive?: number, options?: RequestOptions): Promise<StoreView[]> {
  const url = `/api/stores${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Get a specific cluster-wide store
 */
export async function getStore(storeId: string, recursive?: number, options?: RequestOptions): Promise<StoreView> {
  const url = `/api/stores/${encodeURIComponent(storeId)}${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Body accepted by `POST /api/stores` (`crow_web::mgmt::CreateStoreBody`).
 * `store_id` is a u64 on the wire; the SPA keeps it as a string of decimal
 * digits to round-trip cleanly through URL params and React state.
 */
export interface AddStoreRequest {
  store_id: number | string;
  nodes: number[];
}

/**
 * Create a new cluster-wide store. The target nodes must already have a
 * running `crow-kv-server` (deployed via `deployServer`).
 */
export async function addStore(
  req: AddStoreRequest,
  options?: RequestOptions
): Promise<StoreView> {
  const body = JSON.stringify({
    store_id: Number(req.store_id),
    nodes: req.nodes,
  });
  const url = `/api/stores`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      skipDeduplication: true,
    })
  );
}

/**
 * Delete a cluster-wide store
 */
export async function removeStore(storeId: string, options?: RequestOptions): Promise<void> {
  const url = `/api/stores/${encodeURIComponent(storeId)}`;
  await jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'DELETE', skipDeduplication: true }));
}

/**
 * Body accepted by `POST /api/cluster/init`
 * (`crow_web::mgmt::ClusterInitBody`).
 */
export interface InitClusterRequest {
  nodes: number[];
}

/**
 * Initialize the cluster by bootstrapping the system group
 * (store 0, group 0) on the selected nodes.
 */
export async function initCluster(
  req: InitClusterRequest,
  options?: RequestOptions
): Promise<unknown> {
  const body = JSON.stringify({ nodes: req.nodes });
  const url = `/api/cluster/init`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      skipDeduplication: true,
    })
  );
}

/**
 * Reset the entire cluster: tear down all groups, stores, server
 * processes, nodes, and racks in dependency order. The system group
 * (store 0, group 0) is included. Console config is cleared.
 */
export async function resetCluster(options?: RequestOptions): Promise<unknown> {
  const url = `/api/cluster/reset`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      skipDeduplication: true,
    })
  );
}

/**
 * List all groups in a store
 */
export async function listGroups(storeId: string, recursive?: number, options?: RequestOptions): Promise<GroupView[]> {
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Get a specific group in a store
 */
export async function getGroup(storeId: string, groupId: string, recursive?: number, options?: RequestOptions): Promise<GroupView> {
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Body accepted by `POST /api/stores/:sid/groups` (`CreateGroupBody`).
 * One replica per listed node is created, starting from `replica_id`.
 */
export interface AddGroupRequest {
  group_id: number | string;
  replica_id: number | string;
  nodes: number[];
}

/**
 * Create a new group in a store.
 */
export async function addGroup(
  storeId: string,
  req: AddGroupRequest,
  options?: RequestOptions
): Promise<GroupView> {
  const body = JSON.stringify({
    group_id: Number(req.group_id),
    replica_id: Number(req.replica_id),
    nodes: req.nodes,
  });
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      skipDeduplication: true,
    })
  );
}

/**
 * Delete a group from a store
 */
export async function removeGroup(storeId: string, groupId: string, options?: RequestOptions): Promise<void> {
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}`;
  await jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'DELETE', skipDeduplication: true }));
}

/**
 * List all replicas in a group
 */
export async function listReplicas(storeId: string, groupId: string, options?: RequestOptions): Promise<ReplicaView[]> {
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}/replicas`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Body accepted by `POST /api/stores/:sid/groups/:gid/replicas`
 * (`AddReplicaBody`). `replica_id` is optional and assigned by the
 * backend (`max(existing) + 1`) when omitted.
 */
export interface AddReplicaRequest {
  node_id: number;
  replica_id?: number | string;
}

/**
 * Add a replica to a group.
 */
export async function addReplica(
  storeId: string,
  groupId: string,
  req: AddReplicaRequest,
  options?: RequestOptions
): Promise<ReplicaView> {
  const body = JSON.stringify({
    node_id: req.node_id,
    ...(req.replica_id !== undefined && req.replica_id !== ''
      ? { replica_id: Number(req.replica_id) }
      : {}),
  });
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}/replicas`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      skipDeduplication: true,
    })
  );
}

/**
 * Remove a replica from a group
 */
export async function removeReplica(storeId: string, groupId: string, replicaId: string, options?: RequestOptions): Promise<void> {
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}/replicas/${encodeURIComponent(replicaId)}`;
  await jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'DELETE', skipDeduplication: true }));
}

// ─────────────────────────────────────────────────────────────────────
// KV Data Plane Endpoints
// ─────────────────────────────────────────────────────────────────────

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

/**
 * Get a value from the KV store
 */
export async function kvGet(storeId: string, groupId: string, key: string, options?: RequestOptions): Promise<KvGetResponse> {
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}/kv/get${qs({ key })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Put a value into the KV store
 */
export async function kvPut(
  storeId: string,
  groupId: string,
  req: { key: string; value: string; client_id?: number; seq?: number },
  options?: RequestOptions
): Promise<KvWriteResponse> {
  const body = JSON.stringify(req);
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}/kv/put`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      skipDeduplication: true,
    })
  );
}

/**
 * Delete a value from the KV store
 */
export async function kvDelete(
  storeId: string,
  groupId: string,
  req: { key: string; client_id?: number; seq?: number },
  options?: RequestOptions
): Promise<KvWriteResponse> {
  const body = JSON.stringify(req);
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}/kv/delete`;
  return jsonOrThrow(
    await fetchWithOptions(url, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      skipDeduplication: true,
    })
  );
}

/**
 * Scan keys with a prefix in the KV store
 */
export async function kvScan(
  storeId: string,
  groupId: string,
  prefix: string = '',
  limit: number = 100,
  startAfter?: string,
  options?: RequestOptions
): Promise<KvScanResponse> {
  const params: Record<string, string | number> = { prefix, limit };
  if (startAfter) params.start_after = startAfter;
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}/kv/scan${qs(params)}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

// ─────────────────────────────────────────────────────────────────────
// Health & Monitoring Endpoints
// ─────────────────────────────────────────────────────────────────────

/**
 * Check if the web backend is healthy
 */
export async function healthCheck(options?: RequestOptions): Promise<{ status: 'ok'; timestamp: number }> {
  return jsonOrThrow(await fetchWithOptions(`/healthz`, { ...options, method: 'GET' }));
}

// ─────────────────────────────────────────────────────────────────────
// Metrics Endpoints (R11)
// ─────────────────────────────────────────────────────────────────────

/**
 * Fetch metrics for a specific node (proxied to the node's `/metrics`).
 * @param nodeId The node identifier
 * @param prefix Optional metric name prefix filter
 */
export async function getNodeMetrics(
  nodeId: number,
  prefix?: string,
  options?: RequestOptions
): Promise<MetricsResponse> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/metrics${qs({ prefix })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Fetch metrics for a specific group (proxied to the leader node's
 * `/metrics` with the group prefix `s.{sid}.g.{gid}.`).
 * @param storeId The store identifier
 * @param groupId The group identifier
 * @param prefix Optional metric name prefix filter (appended to group prefix)
 */
export async function getGroupMetrics(
  storeId: string,
  groupId: string,
  prefix?: string,
  options?: RequestOptions
): Promise<MetricsResponse> {
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}/metrics${qs({ prefix })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Fetch aggregated metrics for a store (fetched from each group's leader
 * and merged).
 * @param storeId The store identifier
 * @param prefix Optional metric name prefix filter (appended to store prefix)
 */
export async function getStoreMetrics(
  storeId: string,
  prefix?: string,
  options?: RequestOptions
): Promise<MetricsResponse> {
  const url = `/api/stores/${encodeURIComponent(storeId)}/metrics${qs({ prefix })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

// ── Disk / DiskDB API (R77) ───────────────────────────────────────

export interface AddDiskRequest {
  disk_id: string;
  disk_type: string;
  capacity_bytes: number;
  zone_size_bytes: number;
  unit_size_bytes: number;
  device_path?: string;
}

export interface AddDisksBatchRequest {
  disks: AddDiskRequest[];
}

export interface AddDisksBatchResult {
  added: any[];
  sysdata_errors: string[];
}

export interface DeployDiskdbRequest {
  rpc_port: number;
}

/** `GET /api/diskdb/instances` — list all diskdb instances. */
export async function listDiskdbInstances(options?: RequestOptions): Promise<DiskdbInstanceInfo[]> {
  return jsonOrThrow(await fetchWithOptions('/api/diskdb/instances', { ...options, method: 'GET' }));
}

/** `GET /api/diskdb/usage` — capacity usage drill-down. */
export async function getDiskdbUsage(
  dg?: number,
  disk?: string,
  zone?: number,
  options?: RequestOptions,
): Promise<CapacityUsageResponse> {
  const params: Record<string, string> = {};
  if (dg !== undefined) params.dg = String(dg);
  if (disk !== undefined) params.disk = disk;
  if (zone !== undefined) params.zone = String(zone);
  const url = `/api/diskdb/usage${qs(params)}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/** `GET /api/hardware/capacity` — hierarchical capacity from group-0 sysdata. */
export async function getHardwareCapacity(options?: RequestOptions): Promise<HardwareCapacitySummary> {
  return jsonOrThrow(await fetchWithOptions('/api/hardware/capacity', { ...options, method: 'GET' }));
}

/** `GET /api/diskdb/scan-status` — get scan status. */
export async function getDiskdbScanStatus(dg?: number, options?: RequestOptions): Promise<ScanStatusResponse> {
  const url = `/api/diskdb/scan-status${qs({ dg })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/** `POST /api/diskdb/scan` — trigger a scan. */
export async function triggerDiskdbScan(dg?: number, options?: RequestOptions): Promise<ScanStatusResponse> {
  return jsonOrThrow(
    await fetchWithOptions('/api/diskdb/scan', {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dg: dg ?? null }),
      skipDeduplication: true,
    }),
  );
}

/** `POST /api/diskdb/recalc` — recalculate disk usage. */
export async function recalcDiskdbUsage(dg?: number, options?: RequestOptions): Promise<RecalcResultResponse> {
  return jsonOrThrow(
    await fetchWithOptions('/api/diskdb/recalc', {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dg: dg ?? null }),
      skipDeduplication: true,
    }),
  );
}

/** `POST /api/diskdb/compact` — compact zones on a disk. */
export async function compactDiskdbZones(
  diskId: string,
  zoneIndices?: number[],
  options?: RequestOptions,
): Promise<CompactResultResponse> {
  return jsonOrThrow(
    await fetchWithOptions('/api/diskdb/compact', {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ disk_id: diskId, zone_indices: zoneIndices ?? null }),
      skipDeduplication: true,
    }),
  );
}

/** `POST /api/diskdb/rebuild` — rebuild zone bitmap(s) on a disk. */
export async function rebuildDiskdbZoneBitmap(
  diskId: string,
  zoneIndices?: number[] | null,
  options?: RequestOptions,
): Promise<RebuildResultResponse> {
  return jsonOrThrow(
    await fetchWithOptions('/api/diskdb/rebuild', {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ disk_id: diskId, zone_indices: zoneIndices ?? null }),
      skipDeduplication: true,
    }),
  );
}

/** `PUT /api/disks/:disk_id/status` — set a disk's hardware status. */
export async function setDiskStatus(diskId: string, status: string, options?: RequestOptions): Promise<void> {
  const resp = await fetchWithOptions(`/api/disks/${encodeURIComponent(diskId)}/status`, {
    ...options,
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }),
    skipDeduplication: true,
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`PUT /api/disks/${diskId}/status: HTTP ${resp.status}: ${body}`);
  }
}

/** `PUT /api/disk-groups/:rack_id/:node_id/:dg_id/status` — set a disk-group's hardware status. */
export async function setDiskGroupStatus(
  rackId: number,
  nodeId: number,
  dgId: number,
  status: string,
  options?: RequestOptions,
): Promise<void> {
  const resp = await fetchWithOptions(`/api/disk-groups/${rackId}/${nodeId}/${dgId}/status`, {
    ...options,
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }),
    skipDeduplication: true,
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`PUT /api/disk-groups/${rackId}/${nodeId}/${dgId}/status: HTTP ${resp.status}: ${body}`);
  }
}

/** `PUT /api/disk-groups/:rack_id/:node_id/:dg_id/owner` — assign a disk-group to a diskdb instance. */
export async function setDiskGroupOwner(
  rackId: number,
  nodeId: number,
  dgId: number,
  body: { instance_id: number; lease_expiry_ms: number },
  options?: RequestOptions,
): Promise<void> {
  const resp = await fetchWithOptions(`/api/disk-groups/${rackId}/${nodeId}/${dgId}/owner`, {
    ...options,
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    skipDeduplication: true,
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`PUT /api/disk-groups/${rackId}/${nodeId}/${dgId}/owner: HTTP ${resp.status}: ${text}`);
  }
}

/** `PUT /api/disk-groups/:rack_id/:node_id/:dg_id/bind` — bind a disk-group to a paxos data group. */
export async function setDiskGroupBind(
  rackId: number,
  nodeId: number,
  dgId: number,
  body: { store_id: number; group_id: number },
  options?: RequestOptions,
): Promise<void> {
  const resp = await fetchWithOptions(`/api/disk-groups/${rackId}/${nodeId}/${dgId}/bind`, {
    ...options,
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    skipDeduplication: true,
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`PUT /api/disk-groups/${rackId}/${nodeId}/${dgId}/bind: HTTP ${resp.status}: ${text}`);
  }
}

/** `POST /api/nodes/:id/diskdb/deploy` — deploy diskdb on a node. */
export async function deployDiskdb(nodeId: number, req: DeployDiskdbRequest, options?: RequestOptions): Promise<DiskdbDeployResult> {
  return jsonOrThrow(
    await fetchWithOptions(`/api/nodes/${nodeId}/diskdb/deploy`, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req),
      skipDeduplication: true,
    }),
  );
}

/** `POST /api/nodes/:id/diskdb/restart` — restart diskdb on a node. */
export async function restartDiskdb(nodeId: number, options?: RequestOptions): Promise<DiskdbDeployResult> {
  return jsonOrThrow(
    await fetchWithOptions(`/api/nodes/${nodeId}/diskdb/restart`, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
      skipDeduplication: true,
    }),
  );
}

/** `POST /api/nodes/:id/diskdb/stop` — stop diskdb on a node. */
export async function stopDiskdb(nodeId: number, options?: RequestOptions): Promise<StopResult> {
  return jsonOrThrow(
    await fetchWithOptions(`/api/nodes/${nodeId}/diskdb/stop`, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
      skipDeduplication: true,
    }),
  );
}

/** `DELETE /api/nodes/:id/diskdb` — stop and remove diskdb deployment record. */
export async function removeDiskdb(nodeId: number, options?: RequestOptions): Promise<void> {
  await jsonOrThrow(
    await fetchWithOptions(`/api/nodes/${nodeId}/diskdb`, {
      ...options,
      method: 'DELETE',
      skipDeduplication: true,
    }),
  );
}

/** `POST /api/nodes/:id/disk-groups/:dg_id/disks/batch` — batch add disks. */
export async function addDisksBatch(
  nodeId: number,
  dgId: number,
  req: AddDisksBatchRequest,
  options?: RequestOptions,
): Promise<AddDisksBatchResult> {
  return jsonOrThrow(
    await fetchWithOptions(`/api/nodes/${nodeId}/disk-groups/${dgId}/disks/batch`, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req),
      skipDeduplication: true,
    }),
  );
}

/** `GET /api/nodes/:id/disk-groups` — list disk-groups on a node. */
export async function listNodeDiskGroups(nodeId: number, options?: RequestOptions): Promise<import('./types').DiskGroupEntry[]> {
  return jsonOrThrow(await fetchWithOptions(`/api/nodes/${encodeURIComponent(nodeId)}/disk-groups`, { ...options, method: 'GET' }));
}

/** `GET /api/nodes/:id/disk-groups/:dg_id/disks` — list disks in a disk-group. */
export async function listDisksInGroup(nodeId: number, dgId: number, options?: RequestOptions): Promise<import('./types').DiskEntry[]> {
  return jsonOrThrow(await fetchWithOptions(`/api/nodes/${encodeURIComponent(nodeId)}/disk-groups/${encodeURIComponent(dgId)}/disks`, { ...options, method: 'GET' }));
}

/** `POST /api/nodes/:id/disk-groups` — add a disk-group to a node. */
export async function addDiskGroup(nodeId: number, body: { id: number; name?: string }, options?: RequestOptions): Promise<import('./types').DiskGroupEntry> {
  return jsonOrThrow(
    await fetchWithOptions(`/api/nodes/${encodeURIComponent(nodeId)}/disk-groups`, {
      ...options,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      skipDeduplication: true,
    }),
  );
}

/** `DELETE /api/nodes/:id/disk-groups/:dg_id` — remove a disk-group. */
export async function removeDiskGroup(nodeId: number, dgId: number, options?: RequestOptions): Promise<void> {
  const resp = await fetchWithOptions(`/api/nodes/${encodeURIComponent(nodeId)}/disk-groups/${encodeURIComponent(dgId)}`, {
    ...options,
    method: 'DELETE',
    skipDeduplication: true,
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`DELETE disk-group: HTTP ${resp.status}: ${body}`);
  }
}

/** `DELETE /api/nodes/:id/disk-groups/:dg_id/disks/:disk_id` — remove a disk. */
export async function removeDisk(nodeId: number, dgId: number, diskId: string, options?: RequestOptions): Promise<void> {
  const resp = await fetchWithOptions(`/api/nodes/${encodeURIComponent(nodeId)}/disk-groups/${encodeURIComponent(dgId)}/disks/${encodeURIComponent(diskId)}`, {
    ...options,
    method: 'DELETE',
    skipDeduplication: true,
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`DELETE disk: HTTP ${resp.status}: ${body}`);
  }
}

/** `POST /api/disks/:disk_id/move` — move a disk to a new disk-group. */
export async function moveDisk(
  diskId: string,
  body: { new_rack_id: number; new_node_id: number; new_disk_group_id: number },
  options?: RequestOptions,
): Promise<void> {
  const resp = await fetchWithOptions(`/api/disks/${encodeURIComponent(diskId)}/move`, {
    ...options,
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    skipDeduplication: true,
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`POST move disk: HTTP ${resp.status}: ${text}`);
  }
}

/** `GET /api/servers` — list all deployed server entries. */
export interface ServerSummary {
  node_id?: number;
  mgmt_url: string;
  grpc_url?: string;
  pid?: number;
  health: string;
  service_type: string;
}

export async function listServers(options?: RequestOptions): Promise<ServerSummary[]> {
  return jsonOrThrow(await fetchWithOptions('/api/servers', { ...options, method: 'GET' }));
}
