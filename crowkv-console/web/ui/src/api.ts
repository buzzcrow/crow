// Thin typed wrappers over the crowkv-web HTTP API.
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
  SshCreds,
} from './types';

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
      const response = await fetch(url, {
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
 * List all racks with optional recursive depth
 * @param recursive How many levels of children to include: 0 = just racks, 1 = racks + nodes, etc.
 */
export async function listRacks(recursive?: number, options?: RequestOptions): Promise<Rack[]> {
  const url = `/api/racks${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Get a specific rack by ID
 */
export async function getRack(rackId: string, recursive?: number, options?: RequestOptions): Promise<Rack> {
  const url = `/api/racks/${encodeURIComponent(rackId)}${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Create a new rack
 */
export async function addRack(req: { id: string; name?: string }, options?: RequestOptions): Promise<Rack> {
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
export async function removeRack(rackId: string, options?: RequestOptions): Promise<void> {
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
export async function getNode(nodeId: string, recursive?: number, options?: RequestOptions): Promise<Node> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Create a new node
 */
export async function addNode(req: { id: string; rack_id: string; host: string; ssh: SshCreds }, options?: RequestOptions): Promise<Node> {
  const body = JSON.stringify(req);
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
export async function removeNode(nodeId: string, options?: RequestOptions): Promise<void> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}`;
  await jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'DELETE', skipDeduplication: true }));
}

/**
 * Ping a node to check reachability
 */
export async function pingNode(nodeId: string, options?: RequestOptions): Promise<{ ok: boolean; error?: string }> {
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
 * Deploy a crowkv-server instance to a node
 */
export async function deployServer(
  nodeId: string,
  req: { mgmt_port: number; grpc_port: number; binary?: string },
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
export async function startServer(nodeId: string, options?: RequestOptions): Promise<ServerProcess> {
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
export async function stopServer(nodeId: string, options?: RequestOptions): Promise<ServerProcess> {
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
 * Get the server process details for a node
 */
export async function getServer(nodeId: string, options?: RequestOptions): Promise<ServerProcess> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/server`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Delete the server deployment record for a node
 */
export async function removeServer(nodeId: string, options?: RequestOptions): Promise<void> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/server`;
  await jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'DELETE', skipDeduplication: true }));
}

/**
 * Get the OpenAPI spec for a node's crowkv-server instance
 */
export async function getNodeOpenApi(nodeId: string, options?: RequestOptions): Promise<Record<string, any>> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/openapi.json`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * List stores on a specific node (physical view)
 */
export async function listNodeStores(nodeId: string, recursive?: number, options?: RequestOptions): Promise<NodeStore[]> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/stores${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Get a specific store on a node (physical view)
 */
export async function getNodeStore(nodeId: string, storeId: string, recursive?: number, options?: RequestOptions): Promise<NodeStore> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/stores/${encodeURIComponent(storeId)}${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * List groups on a specific store on a node (physical view)
 */
export async function listNodeGroups(nodeId: string, storeId: string, recursive?: number, options?: RequestOptions): Promise<NodeGroup[]> {
  const url = `/api/nodes/${encodeURIComponent(nodeId)}/stores/${encodeURIComponent(storeId)}/groups${qs({ recursive })}`;
  return jsonOrThrow(await fetchWithOptions(url, { ...options, method: 'GET' }));
}

/**
 * Get a specific group on a node (physical view), including local and remote replicas
 */
export async function getNodeGroup(
  nodeId: string,
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
 * Create a new cluster-wide store
 */
export async function addStore(
  req: { id: string; name?: string; node_ids: string[] },
  options?: RequestOptions
): Promise<StoreView> {
  const body = JSON.stringify(req);
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
 * Create a new group in a store
 */
export async function addGroup(
  storeId: string,
  req: { id: string; replica_count: number; node_ids?: string[] },
  options?: RequestOptions
): Promise<GroupView> {
  const body = JSON.stringify(req);
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
 * Add a replica to a group
 */
export async function addReplica(
  storeId: string,
  groupId: string,
  req: { node_id: string; replica_id?: string },
  options?: RequestOptions
): Promise<ReplicaView> {
  const body = JSON.stringify(req);
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
  options?: RequestOptions
): Promise<KvScanResponse> {
  const url = `/api/stores/${encodeURIComponent(storeId)}/groups/${encodeURIComponent(groupId)}/kv/scan${qs({ prefix, limit })}`;
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
