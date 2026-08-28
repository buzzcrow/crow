// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { CrowKVServerView, Node, Rack, ServerProcess } from '../types';
import { isAvailableProcess } from '../utils/entityDisplay';
import { serverLabel } from '../utils/entityDisplay';

const DIGIT_SUFFIX = /(\d+)$/;

export function isCrowKVServerAvailable(server: Pick<CrowKVServerView, 'process'> | null | undefined): boolean {
  return isAvailableProcess(server?.process);
}

function extractPort(urlOrAddr?: string | null): number | null {
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

function toServerView(node: { id: number; rack_id: number; host: string; server?: ServerProcess } | null | undefined): CrowKVServerView | null {
  if (!node?.server) return null;
  return {
    id: serverLabel(String(node.id)),
    node_id: node.id,
    rack_id: node.rack_id,
    host: node.host,
    process: node.server,
    rest_port: extractPort(node.server.mgmt_url),
    rpc_port: extractPort(node.server.rpc_url),
  };
}

export function buildCrowKVServers(nodes: Node[], racks: Rack[]): CrowKVServerView[] {
  const nodeMap = new Map<number, { id: number; rack_id: number; host: string; server?: ServerProcess }>();

  for (const node of nodes) {
    nodeMap.set(node.id, node);
  }

  for (const rack of racks) {
    for (const entry of ((rack as any).nodes as any[]) || []) {
      if (typeof entry !== 'object' || !entry?.id) continue;
      const existing = nodeMap.get(entry.id);
      nodeMap.set(entry.id, {
        id: entry.id,
        rack_id: entry.rack_id || existing?.rack_id || rack.id,
        host: entry.host || existing?.host || '',
        server: entry.server || existing?.server,
      });
    }
  }

  return [...nodeMap.values()]
    .map((node) => toServerView(node))
    .filter((server): server is CrowKVServerView => server !== null);
}

export function crowKvServerNodeIds(servers: CrowKVServerView[]): Set<number> {
  return new Set(servers.map((server) => server.node_id));
}

export function crowKvServerByNodeId(servers: CrowKVServerView[]): Map<number, CrowKVServerView> {
  return new Map(servers.map((server) => [server.node_id, server]));
}

export function crowKvServerById(servers: CrowKVServerView[]): Map<string, CrowKVServerView> {
  return new Map(servers.map((server) => [server.id, server]));
}
