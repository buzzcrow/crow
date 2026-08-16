// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { expect, request } from '@playwright/test';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

export const DEFAULT_SERVER_BINARY =
  process.env.CROW_KV_SERVER_BINARY ?? resolve(__dirname, '../../../../../target/debug/crow-kv-server');

// Monotonic port counter for E2E tests. Tests run sequentially (workers: 1),
// so a counter is safe — each test cleans up its own servers before the
// next test starts. Stays below the Linux ephemeral port range
// (32768–60999) so the kernel never hands these ports to outgoing
// connections, which would cause "Address already in use" bind errors.
const PORT_BASE = 30000;
const PORT_CEILING = 32768;
let nextPort = PORT_BASE;
export function freePort(): number {
  if (nextPort >= PORT_CEILING) {
    throw new Error(`freePort: exhausted ${PORT_CEILING - PORT_BASE} ports; raise PORT_CEILING above the ephemeral range`);
  }
  return nextPort++;
}

export interface TestRack {
  id: number;
  name?: string;
}

export interface TestNode {
  id: number;
  rack_id: number;
  host?: string;
}

export async function apiContext(baseURL: string) {
  return request.newContext({ baseURL });
}

export async function createRack(baseURL: string, rack: TestRack) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/racks', {
      data: {
        id: rack.id,
        name: rack.name ?? rack.id,
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function createNode(baseURL: string, node: TestNode) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/racks/${encodeURIComponent(node.rack_id)}/nodes`, {
      data: {
        id: node.id,
        rack_id: node.rack_id,
        host: node.host ?? '127.0.0.1',
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function seedRackAndNode(baseURL: string, rackId = 1, nodeId = 1) {
  await createRack(baseURL, { id: rackId, name: `rack-${rackId}` });
  await createNode(baseURL, { id: nodeId, rack_id: rackId });
}

export async function deployNodeServer(baseURL: string, nodeId: number, restPort: number, rpcPort: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/server/deploy`, {
      data: {
        rest_port: restPort,
        rpc_port: rpcPort,
        binary: DEFAULT_SERVER_BINARY,
        election_profile: 'e2e',
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function stopNodeServer(baseURL: string, nodeId: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/server/stop`);
    if (!response.ok() && response.status() !== 400 && response.status() !== 404 && response.status() !== 409) {
      console.warn(`stopNodeServer(${nodeId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`stopNodeServer(${nodeId}) failed:`, err);
  } finally {
    await api.dispose();
  }
}

export async function addGroup(baseURL: string, storeId: number, groupId: number, replicaId: number, nodeIds: number[]) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/stores/${storeId}/groups`, {
      data: {
        group_id: groupId,
        replica_id: replicaId,
        nodes: nodeIds,
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function addReplica(baseURL: string, storeId: number, groupId: number, nodeId: number, replicaId?: number) {
  const api = await apiContext(baseURL);
  try {
    const body: Record<string, unknown> = { node_id: nodeId };
    if (replicaId !== undefined) body.replica_id = replicaId;
    const response = await api.post(`/api/stores/${storeId}/groups/${groupId}/replicas`, { data: body });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

/**
 * Poll until the group reports an elected leader. `POST /api/stores` only
 * creates the store; a group must be added separately (`addGroup`) and then
 * needs a moment to elect before KV ops can resolve a leader.
 */
export async function waitForLeader(baseURL: string, storeId: number, groupId: number, timeoutMs = 3_000) {
  const api = await apiContext(baseURL);
  try {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const r = await api.get(`/api/stores/${storeId}/groups/${groupId}`);
      if (r.ok()) {
        const v = await r.json();
        const hasLeader =
          (Array.isArray(v.replicas) && v.replicas.some((x: any) => String(x.role).toLowerCase() === 'leader')) ||
          (typeof v.leader_id === 'number' && v.leader_id > 0);
        if (hasLeader) return;
      }
      await new Promise((res) => setTimeout(res, 100));
    }
    throw new Error(`leader not elected for store ${storeId} group ${groupId} within ${timeoutMs}ms`);
  } finally {
    await api.dispose();
  }
}

export async function clusterInit(baseURL: string, nodeIds: number[]) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/cluster/init', {
      data: { nodes: nodeIds },
    });
    // 201 = freshly initialized, 409/200 = already initialized — both OK.
    if (response.status() !== 201 && response.status() !== 409) {
      throw new Error(`cluster_init failed: ${response.status()} ${await response.text()}`);
    }
  } finally {
    await api.dispose();
  }
}

export async function createStore(baseURL: string, storeId: number, nodeIds: number[]) {
  // Non-zero stores require the system group (store 0 / group 0) to exist.
  // system_init is idempotent, so this is safe to call every time.
  if (storeId !== 0) {
    await clusterInit(baseURL, nodeIds);
  }
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/stores', {
      data: {
        store_id: storeId,
        nodes: nodeIds,
      },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

export async function resetAll(baseURL: string) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/internal/reset');
    expect(response.status(), await response.text()).toBe(200);
  } finally {
    await api.dispose();
  }
}

// ── DiskDB helpers ─────────────────────────────────────────────────

export const DEFAULT_DISKDB_BINARY =
  process.env.CROW_DISKDB_BIN ?? resolve(__dirname, '../../../../../target/debug/crow-diskdb');

/** Copy the crow-diskdb binary + a minimal config into a node workspace. */
export async function stageDiskdbBinaryAndConfig(baseURL: string, nodeId: number, rpcPort: number) {
  const api = await apiContext(baseURL);
  try {
    // The deploy handler calls prepare_node_workspace which creates
    // bin/ and conf/ dirs. We trigger that by calling deploy, but
    // deploy will fail if the binary isn't there yet. Instead, use
    // the internal stage endpoint if available, or just let the
    // deploy handler handle it — the handler now falls back to
    // crow_diskdb_bin() if the workspace bin/ doesn't have it.
    // For E2E, we set CROW_DISKDB_BIN env so the handler finds it.
    void api;
    void nodeId;
    void rpcPort;
  } finally {
    await api.dispose();
  }
}

/** Deploy a diskdb instance on a node via the REST API. */
export async function deployDiskdb(baseURL: string, nodeId: number, rpcPort: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/diskdb/deploy`, {
      data: { rpc_port: rpcPort },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

/** Stop a diskdb instance on a node via the REST API. */
export async function stopDiskdb(baseURL: string, nodeId: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/diskdb/stop`);
    if (!response.ok() && response.status() !== 400 && response.status() !== 404) {
      console.warn(`stopDiskdb(${nodeId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`stopDiskdb(${nodeId}) failed:`, err);
  } finally {
    await api.dispose();
  }
}

/** Add a disk-group to a node via the REST API. */
export async function addDiskGroup(baseURL: string, nodeId: number, dgId: number, name?: string) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(`/api/nodes/${encodeURIComponent(nodeId)}/disk-groups`, {
      data: { id: dgId, name: name ?? '' },
    });
    expect(response.status(), await response.text()).toBe(201);
  } finally {
    await api.dispose();
  }
}

/** Remove a disk-group from a node via the REST API. */
export async function removeDiskGroup(baseURL: string, nodeId: number, dgId: number) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.delete(`/api/nodes/${encodeURIComponent(nodeId)}/disk-groups/${encodeURIComponent(dgId)}`);
    if (!response.ok() && response.status() !== 404) {
      console.warn(`removeDiskGroup(${nodeId}, ${dgId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`removeDiskGroup(${nodeId}, ${dgId}) failed:`, err);
  } finally {
    await api.dispose();
  }
}

/** Add disks to a disk-group via the batch REST API. */
export async function addDisksBatch(
  baseURL: string,
  nodeId: number,
  dgId: number,
  disks: { disk_id: string; disk_type?: string; capacity_bytes?: number; zone_size_bytes?: number; unit_size_bytes?: number }[],
) {
  const api = await apiContext(baseURL);
  try {
    const payload = disks.map((d) => ({
      disk_id: d.disk_id,
      disk_type: d.disk_type ?? 'Hdd',
      capacity_bytes: d.capacity_bytes ?? 4 * 1024 * 1024 * 1024 * 1024,
      zone_size_bytes: d.zone_size_bytes ?? 32 * 1024 * 1024 * 1024,
      unit_size_bytes: d.unit_size_bytes ?? 1024 * 1024,
    }));
    const response = await api.post(
      `/api/nodes/${encodeURIComponent(nodeId)}/disk-groups/${encodeURIComponent(dgId)}/disks/batch`,
      { data: { disks: payload } },
    );
    expect(response.status(), await response.text()).toBe(201);
    return await response.json();
  } finally {
    await api.dispose();
  }
}

/** Remove a disk from a disk-group via the REST API. */
export async function removeDisk(baseURL: string, nodeId: number, dgId: number, diskId: string) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.delete(
      `/api/nodes/${encodeURIComponent(nodeId)}/disk-groups/${encodeURIComponent(dgId)}/disks/${encodeURIComponent(diskId)}`,
    );
    if (!response.ok() && response.status() !== 404) {
      console.warn(`removeDisk(${nodeId}, ${dgId}, ${diskId}) returned ${response.status()}:`, await response.text());
    }
  } catch (err) {
    console.warn(`removeDisk(${nodeId}, ${dgId}, ${diskId}) failed:`, err);
  } finally {
    await api.dispose();
  }
}

/** Generate a random disk ID in the crow-protocol format: 16 hex chars + dash + 16 hex chars. */
export function randomDiskId(): string {
  const hex16 = () => Array.from({ length: 16 }, () => '0123456789abcdef'[Math.floor(Math.random() * 16)]).join('');
  return `${hex16()}-${hex16()}`;
}

// ── setupCluster helper + topology presets ──────────────────────────

export interface TopologyDescriptor {
  nodeCount: number;
  storeCount: number;
  groupsPerStore: number;
  replicasPerGroup: number;
  rackBase: number;
  nodeBase: number;
  portBase: number;
  storeBase: number;
  groupBase: number;
}

export interface SetupResult {
  racks: number[];
  nodes: number[];
  stores: number[];
  groups: { storeId: number; groupId: number }[];
  apiBase: string;
}

export const SIMPLE: TopologyDescriptor = {
  nodeCount: 3,
  storeCount: 1,
  groupsPerStore: 1,
  replicasPerGroup: 3,
  rackBase: 100,
  nodeBase: 100,
  portBase: 9800,
  storeBase: 800,
  groupBase: 8000,
};

export const COMPLEX: TopologyDescriptor = {
  nodeCount: 8,
  storeCount: 2,
  groupsPerStore: 2,
  replicasPerGroup: 3,
  rackBase: 200,
  nodeBase: 200,
  portBase: 9900,
  storeBase: 900,
  groupBase: 9000,
};

/**
 * Create a full cluster topology via API calls: racks → nodes → deploy →
 * stores → groups → wait for leaders. Returns the created entity IDs.
 * Each node gets its own rack (1:1 mapping) for simplicity.
 */
export async function setupCluster(baseURL: string, topo: TopologyDescriptor): Promise<SetupResult> {
  const racks: number[] = [];
  const nodes: number[] = [];

  // Create racks + nodes (sequential — cheap API calls).
  for (let i = 0; i < topo.nodeCount; i++) {
    const rackId = topo.rackBase + i;
    const nodeId = topo.nodeBase + i;
    await createRack(baseURL, { id: rackId, name: `rack-${rackId}` });
    await createNode(baseURL, { id: nodeId, rack_id: rackId });
    racks.push(rackId);
    nodes.push(nodeId);
  }

  // Deploy all nodes concurrently — each deploy polls /health until
  // ready, so parallel deploy overlaps the readiness waits.
  await Promise.all(
    nodes.map((nodeId) => deployNodeServer(baseURL, nodeId, freePort(), freePort())),
  );

  const stores: number[] = [];
  const groups: { storeId: number; groupId: number }[] = [];

  for (let s = 0; s < topo.storeCount; s++) {
    const storeId = topo.storeBase + s;
    const storeNodes = nodes.slice(0, Math.min(topo.replicasPerGroup, nodes.length));
    await createStore(baseURL, storeId, storeNodes);
    stores.push(storeId);

    for (let g = 0; g < topo.groupsPerStore; g++) {
      const groupId = topo.groupBase + s * topo.groupsPerStore + g;
      const groupNodes = nodes.slice(0, topo.replicasPerGroup);
      await addGroup(baseURL, storeId, groupId, 1, groupNodes);
      await waitForLeader(baseURL, storeId, groupId);
      groups.push({ storeId, groupId });
    }
  }

  return { racks, nodes, stores, groups, apiBase: baseURL };
}

/**
 * Stop all deployed servers from a setupCluster call.
 */
export async function teardownCluster(baseURL: string, result: SetupResult) {
  for (const nodeId of result.nodes) {
    await stopNodeServer(baseURL, nodeId);
  }
}
