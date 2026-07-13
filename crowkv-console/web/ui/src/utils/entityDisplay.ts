// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

export type UiHealth = 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';
export type UiRole = 'Leader' | 'Follower' | 'Remote';

function normalize(value?: string | null): string {
  return String(value || '').trim().toLowerCase();
}

function stripKnownPrefix(value: string, prefix: string): string {
  const raw = String(value || '').trim();
  if (!raw) return raw;
  const exact = `${prefix}-`;
  if (raw.toLowerCase().startsWith(exact.toLowerCase())) return raw.slice(exact.length);
  return raw;
}

function titleCaseToken(token: string): string {
  return token ? `${token[0].toUpperCase()}${token.slice(1)}` : '';
}

export function toDisplayState(value?: string | null): string {
  const raw = normalize(value);
  if (!raw) return 'Unknown';
  return raw.split('_').map(titleCaseToken).join(' ');
}

export function toUiHealth(value?: string | null): UiHealth {
  const raw = normalize(value);
  if (!raw) return 'Unknown';
  if (raw === 'up' || raw === 'healthy') return 'Healthy';
  if (raw === 'degraded') return 'Degraded';
  if (raw === 'down' || raw === 'failed' || raw === 'unhealthy' || raw === 'unavailable') return 'Failed';
  if (raw === 'running') return 'Healthy';
  if (raw === 'initializing' || raw === 'starting' || raw === 'unknown') return 'Unknown';
  if (raw === 'draining' || raw === 'stopped') return 'Failed';
  return 'Unknown';
}

export function toUiRole(value?: string | null): UiRole {
  const raw = normalize(value);
  if (raw === 'leader') return 'Leader';
  if (raw === 'follower') return 'Follower';
  return 'Remote';
}

export function toUiReplicaRole(value?: string | null, state?: string | null): UiRole | undefined {
  const rawState = normalize(state);
  if (!rawState || rawState === 'unknown' || rawState === 'initializing') return undefined;
  const rawRole = normalize(value);
  if (rawRole === 'leader') return 'Leader';
  if (rawRole === 'follower') return 'Follower';
  return undefined;
}

export function isAvailableProcess(process?: { state?: string | null; health?: string | null } | null): boolean {
  return normalize(process?.state) === 'running' && normalize(process?.health) === 'up';
}

export function prefixedId(prefix: string, id: string | number): string {
  return `${prefix}-${stripKnownPrefix(String(id), prefix)}`;
}

export function rackLabel(id: string): string {
  return prefixedId('R', id);
}

export function nodeLabel(id: string): string {
  return prefixedId('N', id);
}

export function serverLabel(nodeId: string): string {
  return prefixedId('KV', nodeId);
}

export function storeLabel(id: string | number): string {
  return prefixedId('S', id);
}

export function groupLabel(id: string | number): string {
  return prefixedId('G', id);
}

export function localReplicaLabel(id: string | number): string {
  return prefixedId('LR', id);
}

export function remoteReplicaLabel(id: string | number): string {
  return prefixedId('RR', id);
}
