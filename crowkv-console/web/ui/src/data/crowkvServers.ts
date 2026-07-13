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

function toServerView(node: { id: string; rack_id: string; host: string; server?: ServerProcess } | null | undefined): CrowKVServerView | null {
  if (!node?.server) return null;
  return {
    id: serverLabel(node.id),
    node_id: node.id,
    rack_id: node.rack_id,
    host: node.host,
    process: node.server,
    mgmt_port: extractPort(node.server.mgmt_url),
    grpc_port: extractPort(node.server.grpc_url),
  };
}

export function buildCrowKVServers(nodes: Node[], racks: Rack[]): CrowKVServerView[] {
  const nodeMap = new Map<string, { id: string; rack_id: string; host: string; server?: ServerProcess }>();

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

export function crowkvServerNodeIds(servers: CrowKVServerView[]): Set<string> {
  return new Set(servers.map((server) => server.node_id));
}

export function crowkvServerByNodeId(servers: CrowKVServerView[]): Map<string, CrowKVServerView> {
  return new Map(servers.map((server) => [server.node_id, server]));
}

export function crowkvServerById(servers: CrowKVServerView[]): Map<string, CrowKVServerView> {
  return new Map(servers.map((server) => [server.id, server]));
}
