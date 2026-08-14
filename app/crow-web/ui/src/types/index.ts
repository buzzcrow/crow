// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Identifiers
export type RackId = number;
export type NodeId = number;
export type StoreId = string;
export type GroupId = string;
export type ReplicaId = string;

// SSH Credentials
export type SshCreds =
  | { type: 'KeyDefault'; user: string }
  | { type: 'KeyPath'; user: string; key_path: string }
  | { type: 'Password'; user: string; pass: string };

// Server Process State
export enum ProcState {
  Unknown = 'unknown',
  Stopped = 'stopped',
  Starting = 'starting',
  Running = 'running',
  Failed = 'failed'
}

export enum NodeHealth {
  Up = 'up',
  Down = 'down',
  Unknown = 'unknown'
}

export interface ServerProcess {
  mgmt_url: string;
  grpc_url: string;
  pid?: number;
  state: ProcState;
  health: NodeHealth;
  last_seen_ms: number;
}

// Physical View Types
export interface Rack {
  id: RackId;
  name?: string;
  nodes: Node[];
}

export interface Node {
  id: NodeId;
  rack_id: RackId;
  host: string;
  ssh: SshCreds;
  server?: ServerProcess;
}

export interface CrowKVServerView {
  id: string;
  node_id: NodeId;
  rack_id: RackId;
  host: string;
  process: ServerProcess;
  mgmt_port: number | null;
  grpc_port: number | null;
}

export interface NodeStore {
  node_id: NodeId;
  store_id: StoreId;
  groups: NodeGroup[];
}

// Batched crow-tree engine diagnostics (doc/todo-sm.md Step 6); mirrors
// crow-kv's `CrowTreeStatsView`. Present only when the group's engine is
// `CrowTreeEngine` (absent/undefined for `InMemKV`).
export interface CrowTreeStats {
  last_applied_slot: number;
  contiguous_slot: number;
  gc_watermark: number;
  snapshot_pages_written: number;
  snapshot_segments_written: number;
  buffer_pool_hits: number;
  buffer_pool_misses: number;
  buffer_pool_evictions: number;
  buffer_pool_writebacks: number;
  buffer_pool_resident: number;
  buffer_pool_dirty: number;
  buffer_pool_used: number;
  buffer_pool_num_frames: number;
}

export interface LocalReplicaInfo {
  replica_id: ReplicaId;
  role: ReplicaRole;
  state: ReplicaState;
  engine_healthy: boolean;
  crowtree_stats?: CrowTreeStats;
  election?: ElectionState;
}

export interface RemoteReplicaInfo {
  replica_id: ReplicaId;
  node_id: NodeId;
  reachable: boolean;
}

export interface NodeGroup {
  node_id: NodeId;
  store_id: StoreId;
  group_id: GroupId;
  local: LocalReplicaInfo;
  remotes: RemoteReplicaInfo[];
  leader_hint?: ReplicaId;
  read_state?: ReadState;
}

// Logical View Types
export interface StoreView {
  store_id: StoreId;
  name?: string;
  nodes: NodeId[];
  groups: GroupSummary[];
}

export interface GroupSummary {
  group_id: GroupId;
  leader?: ReplicaId;
  health: GroupHealth;
  replica_count: number;
}

export interface GroupView {
  store_id: StoreId;
  group_id: GroupId;
  leader?: ReplicaId;
  replicas: ReplicaView[];
  state: GroupHealth;
  read_state?: ReadState;
}

export interface ReplicaView {
  replica_id: ReplicaId;
  node_id: NodeId;
  store_id: string; // Added for logical tree
  group_id: string; // Added for logical tree
  role: ReplicaRole;
  state: ReplicaState;
  engine_healthy: boolean;
  crowtree_stats?: CrowTreeStats;
  election?: ElectionState;
}

// Common Enums
export enum ReplicaRole {
  Leader = 'leader',
  Follower = 'follower'
}

export enum ReplicaState {
  Unknown = 'unknown',
  Initializing = 'initializing',
  Running = 'running',
  Draining = 'draining',
  Failed = 'failed'
}

export enum GroupHealth {
  Healthy = 'healthy',
  Degraded = 'degraded',
  Unavailable = 'unavailable',
  Unknown = 'unknown'
}

export enum ViewMode {
  Physical = 'Physical',
  Logical = 'Logical'
}

export enum ThemeMode {
  Light = 'Light',
  Dark = 'Dark',
  System = 'System'
}

// Activity Log Types
export interface ActivityLogEntry {
  id: string;
  timestamp: number;
  action: string;
  target: string;
  status: 'Success' | 'Failed' | 'Pending';
  message?: string;
}

// API Error Types
export enum ErrorType {
  NodeUnreachable = 'NodeUnreachable',
  UpstreamRpc = 'UpstreamRpc',
  Validation = 'Validation',
  NotFound = 'NotFound',
  Conflict = 'Conflict'
}

export interface ApiError {
  type: ErrorType;
  message: string;
  details?: any;
}

// Election/lease state (mirrors crow-kv's ElectionStateView)
export interface ElectionState {
  election_count: number;
  current_term: number;
  last_heartbeat_age_ms?: number;
  lease_remaining_ms?: number;
  bulk_phase1_in_flight_slots: number;
  step_downs_higher_term: number;
  step_downs_lease_unrenewable: number;
  step_downs_admin: number;
}

// Read-path state gauges (mirrors crow-kv's ReadStateView)
export interface ReadState {
  lease_valid: number;
  contiguous_applied: number;
  safe_slot: number;
}

// Metrics snapshot types (mirrors crow-console-shared's MetricsResponse)
export interface MetricField {
  key: string;
  value: number;
}

export interface MetricPoint {
  name: string;
  kind: string;
  fields: MetricField[];
}

export interface MetricsResponse {
  window_secs: number;
  timestamp: string;
  metrics: MetricPoint[];
}
