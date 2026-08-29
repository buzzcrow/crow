// Copyright 2026-present Gian <crow.db@outlook.com>
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
  rpc_url: string;
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
  // Present on the recursive `GET /api/racks?recursive=N` and flat
  // `GET /api/nodes` responses (mirrors `crowdb_web::physical_view::NodeView`
  // / the manually-built node JSON in `lifecycle`). Absent on the bare
  // rack-membership shape.
  has_server?: boolean;
}

export interface CrowdbKVServerView {
  id: string;
  node_id: NodeId;
  rack_id: RackId;
  host: string;
  process: ServerProcess;
  rest_port: number | null;
  rpc_port: number | null;
}

export interface NodeStore {
  node_id: NodeId;
  store_id: StoreId;
  groups: NodeGroup[];
}

// Batched crowdb-tree engine diagnostics (doc/todo-sm.md Step 6); mirrors
// crowdb-kv's `CrowdbTreeStatsView`. Present only when the group's engine is
// `CrowdbTreeEngine` (absent/undefined for `InMemKV`).
export interface CrowdbTreeStats {
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
  crowtree_stats?: CrowdbTreeStats;
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

// Lightweight summary used by `StoreView::groups`. The full view is
// `GroupView`, returned from `GET /api/stores/:s/groups/:g`. Mirrors
// `crowdb_console_shared::cluster::GroupSummary` (group_id, replica_count,
// leader only — no health field).
export interface GroupSummary {
  group_id: GroupId;
  replica_count: number;
  leader?: ReplicaId;
}

// `StoreView` with each summary group expanded to its full `GroupView`.
// This is the shape `useLogicalTree` exposes: it fetches every group via
// `getGroup` and replaces `groups` with the detailed views, so consumers
// (Sidebar, buildFlow, Inspector) can read `replicas` / `state` /
// `read_state` without `as any` casts. The raw `listStores` / `getStore`
// APIs still return `StoreView` (summary groups).
export interface EnrichedStoreView extends Omit<StoreView, 'groups'> {
  groups: GroupView[];
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
  crowtree_stats?: CrowdbTreeStats;
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
  Logical = 'Logical',
  Capacity = 'Capacity'
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

// Election/lease state (mirrors crowdb-kv's ElectionStateView)
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

// Read-path state gauges (mirrors crowdb-kv's ReadStateView)
export interface ReadState {
  lease_valid: number;
  contiguous_applied: number;
  safe_slot: number;
}

// Metrics snapshot types (mirrors crowdb-console-shared's MetricsResponse)
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

// ── Capacity view types (R77) ─────────────────────────────────────
// Mirror the DTOs from crowdb-web/src/diskdb.rs.

// Console-config disk-group entry (mirrors crowdb-console-shared DiskGroupEntry).
export interface DiskGroupEntry {
  id: number;
  rack_id: number;
  node_id: number;
  name?: string;
}

// Console-config disk entry (mirrors crowdb-console-shared DiskEntry).
export interface DiskEntry {
  disk_id: string;
  disk_group_id: number;
  rack_id: number;
  node_id: number;
  disk_type: string;
  capacity_bytes: number;
  zone_size_bytes: number;
  unit_size_bytes: number;
}

export interface DiskdbInstanceInfo {
  instance_id: number;
  rpc_endpoint: string;
  last_heartbeat_ms: number;
  owned_dg_ids: number[];
  group_usages: DiskGroupUsageSummary[];
}

export interface DiskGroupUsageSummary {
  disk_group_id: number;
  capacity_bytes: number;
  busy_bytes: number;
  free_bytes: number;
  disk_count: number;
}

export interface CapacityUsageResponse {
  disk_groups: DiskGroupInfoDto[];
}

export interface DiskGroupInfoDto {
  rack_id: number;
  node_id: number;
  disk_group_id: number;
  status: number;
  disk_ids: string[];
  disks: DiskInfoDto[];
  capacity_bytes: number;
  busy_bytes: number;
  free_bytes: number;
  allocatable_disk_count: number;
}

export interface DiskInfoDto {
  rack_id: number;
  node_id: number;
  disk_group_id: number;
  disk_id: string;
  disk_type: number;
  capacity_units: number;
  zone_size_units: number;
  unit_size_bytes: number;
  zone_count: number;
  status: number;
  busy_units: number;
  free_units: number;
  capacity_bytes: number;
  busy_bytes: number;
  free_bytes: number;
  active_zone_count: number;
  zone_usages: ZoneUsageDto[];
}

export interface ZoneUsageDto {
  zone_index: number;
  capacity_bytes: number;
  busy_bytes: number;
  free_bytes: number;
  busy_block_count: number;
  free_block_count: number;
  alloc_state: number;
  usage_bitmap?: string;
}

export interface ScanStatusResponse {
  summary?: ScanSummaryDto;
  has_run: boolean;
  scan_in_progress: boolean;
}

export interface ScanSummaryDto {
  started_at_ms: number;
  duration_ms: number;
  zones_scanned: number;
  zones_skipped_active: number;
  zones_skipped_compacting: number;
  ghost_busy: number;
  ghost_free: number;
  uncompacted_lag: number;
  corrupt_snapshots: number;
  corrupt_records: number;
  owner_mismatches: number;
  leak_status: string;
}

export interface RecalcResultResponse {
  results: DiskGroupRecalcResultDto[];
}

export interface DiskGroupRecalcResultDto {
  disk_group_id: number;
  drift_detected: boolean;
  zones: ZoneRecalcResultDto[];
}

export interface ZoneRecalcResultDto {
  disk_id: string;
  zone_index: number;
  matches: boolean;
  drift_detected: boolean;
  live_busy_blocks: number;
  replayed_busy_blocks: number;
  live_snapshot_slot: number;
  replayed_snapshot_slot: number;
  fallback_reason?: string;
}

export interface CompactResultResponse {
  compacted_zone_count: number;
  total_free_records_deleted: number;
  zones: ZoneCompactionResultDto[];
}

export interface ZoneCompactionResultDto {
  zone_index: number;
  success: boolean;
  free_records_deleted: number;
  error?: string;
}

export interface RebuildResultResponse {
  rebuilt_zone_count: number;
  total_busy_units: number;
  total_free_units: number;
}

export interface DiskdbDeployResult {
  node_id: number;
  mgmt_url: string;
  rpc_url: string;
  pid: number;
}

export interface StopResult {
  sent: boolean;
}

// ── Hardware capacity summary (from group-0 sysdata) ─────────────

export interface HardwareCapacitySummary {
  datacenter_capacity_bytes: number;
  racks: RackCapacityEntry[];
  nodes: NodeCapacityEntry[];
  disk_groups: DiskGroupCapacityEntry[];
}

export interface RackCapacityEntry {
  rack_id: number;
  status: number;
  node_count: number;
  capacity_bytes: number;
}

export interface NodeCapacityEntry {
  node_id: number;
  rack_id: number;
  status: number;
  disk_group_count: number;
  capacity_bytes: number;
}

export interface DiskGroupCapacityEntry {
  disk_group_id: number;
  rack_id: number;
  node_id: number;
  status: number;
  disk_count: number;
  capacity_bytes: number;
  disks: DiskCapacityEntry[];
}

export interface DiskCapacityEntry {
  disk_id: string;
  disk_type: number;
  status: number;
  capacity_bytes: number;
  zone_count: number;
  unit_size_bytes: number;
}
