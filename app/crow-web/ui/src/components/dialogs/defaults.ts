// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Per-service port bases. Ports are generated dynamically from these
// bases (offset by the node-id suffix, then incremented past collisions
// against ports already assigned in the console config). The bases are
// dev defaults; production should override via a deployment config story.
export const KV_SERVER_REST_PORT_BASE = 19910;
export const KV_SERVER_RPC_PORT_BASE = 19920;
export const DISKDB_RPC_PORT_BASE = 29920;

const DIGIT_SUFFIX = /(\d+)$/;

interface ServerPortSource {
  id?: string | number;
  rest_port?: number | null;
  rpc_port?: number | null;
  process?: {
    mgmt_url: string;
    grpc_url: string;
  };
  server?: {
    mgmt_url: string;
    grpc_url: string;
  };
}

export function nextIdFromSuffix(existingIds: (string | number)[], min = 1): string {
  let max = min - 1;

  for (const id of existingIds) {
    const raw = String(id).trim();
    if (!raw) continue;

    const match = raw.match(DIGIT_SUFFIX);
    if (!match) continue;

    const n = Number(match[1]);
    if (Number.isFinite(n)) max = Math.max(max, n);
  }

  return String(max + 1);
}

export function nextPrefixedId(existingIds: string[], prefix: string): string {
  let max = 0;
  const normalizedPrefix = prefix.toLowerCase();

  for (const id of existingIds) {
    const raw = id.trim();
    if (!raw) continue;

    const lower = raw.toLowerCase();
    if (!lower.startsWith(normalizedPrefix)) continue;

    const suffix = lower.slice(normalizedPrefix.length);
    if (!/^\d+$/.test(suffix)) continue;

    const n = Number(suffix);
    if (Number.isFinite(n)) max = Math.max(max, n);
  }

  return `${prefix}${max + 1}`;
}

export function nextNumericId(existingIds: (string | number)[], min = 1): string {
  let max = min - 1;

  for (const id of existingIds) {
    const raw = String(id).trim();
    if (!/^\d+$/.test(raw)) continue;
    const n = Number(raw);
    if (Number.isFinite(n)) max = Math.max(max, n);
  }

  return String(max + 1);
}

/// Find the minimal unused numeric id >= `min`. Unlike `nextNumericId`
/// (which returns max+1), this fills gaps: if ids 1 and 3 exist, it
/// returns 2 instead of 4.
export function minUnusedId(existingIds: (string | number)[], min = 1): string {
  const used = new Set<number>();
  for (const id of existingIds) {
    const raw = String(id).trim();
    if (!/^\d+$/.test(raw)) continue;
    const n = Number(raw);
    if (Number.isFinite(n)) used.add(n);
  }
  let candidate = min;
  while (used.has(candidate)) candidate += 1;
  return String(candidate);
}

export function extractPort(urlOrAddr?: string | null): number | null {
  const value = (urlOrAddr || '').trim();
  if (!value) return null;

  try {
    const parsed = new URL(value);
    if (!parsed.port) return null;
    const port = Number(parsed.port);
    return Number.isFinite(port) ? port : null;
  } catch {
    const match = value.match(DIGIT_SUFFIX);
    if (!match) return null;
    const port = Number(match[1]);
    return Number.isFinite(port) ? port : null;
  }
}

export function nextAvailablePort(usedPorts: number[], start: number): string {
  const used = new Set(usedPorts.filter((p) => Number.isFinite(p) && p > 0));
  let candidate = start;
  while (used.has(candidate)) candidate += 1;
  return String(candidate);
}

function preferredPortStart(base: number, nodeId: number): number {
  const raw = String(nodeId).trim();
  if (!raw) return base;

  const match = raw.match(DIGIT_SUFFIX);
  if (!match) return base;

  const suffix = Number(match[1]);
  if (!Number.isFinite(suffix) || suffix < 0) return base;
  return base + suffix;
}

export function deployPortDefaultsForNode(
  servers: ServerPortSource[],
  nodeId: number,
  restStart = KV_SERVER_REST_PORT_BASE,
  rpcStart = KV_SERVER_RPC_PORT_BASE,
  extraUsedRestPorts: number[] = [],
  extraUsedRpcPorts: number[] = [],
): { defaultRestPort: string; defaultRpcPort: string } {
  const usedRestPorts: number[] = [...extraUsedRestPorts];
  const usedRpcPorts: number[] = [...extraUsedRpcPorts];

  for (const server of servers) {
    const mgmt =
      server.rest_port ??
      (server.process?.mgmt_url ? extractPort(server.process.mgmt_url) : null) ??
      (server.server?.mgmt_url ? extractPort(server.server.mgmt_url) : null);
    const grpc =
      server.rpc_port ??
      (server.process?.grpc_url ? extractPort(server.process.grpc_url) : null) ??
      (server.server?.grpc_url ? extractPort(server.server.grpc_url) : null);
    if (mgmt) usedRestPorts.push(mgmt);
    if (grpc) usedRpcPorts.push(grpc);
  }

  const defaultRestPort = nextAvailablePort(usedRestPorts, preferredPortStart(restStart, nodeId));
  const defaultRpcPort = nextAvailablePort(
    [...usedRpcPorts, Number(defaultRestPort)],
    preferredPortStart(rpcStart, nodeId),
  );

  return { defaultRestPort, defaultRpcPort };
}

interface DiskdbPortSource {
  rpc_endpoint?: string | null;
}

/**
 * Pick a dynamic diskdb gRPC port for a node: offset the base by the
 * node-id suffix, then increment past ports already assigned to other
 * diskdb instances (extracted from their `rpc_endpoint`) and any
 * extra remembered ports. Mirrors `deployPortDefaultsForNode` so diskdb
 * deploy gets the same collision-avoidance as kv-server deploy.
 */
export function diskdbPortDefaultsForNode(
  instances: DiskdbPortSource[],
  nodeId: number,
  rpcStart = DISKDB_RPC_PORT_BASE,
  extraUsedRpcPorts: number[] = [],
): string {
  const usedRpcPorts: number[] = [...extraUsedRpcPorts];
  for (const inst of instances) {
    const port = extractPort(inst.rpc_endpoint);
    if (port) usedRpcPorts.push(port);
  }
  return nextAvailablePort(usedRpcPorts, preferredPortStart(rpcStart, nodeId));
}
