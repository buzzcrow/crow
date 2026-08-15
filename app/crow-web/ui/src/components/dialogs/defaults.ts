// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

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
  restStart = 19910,
  rpcStart = 19920,
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
