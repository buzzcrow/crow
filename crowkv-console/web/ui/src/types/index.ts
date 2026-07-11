// Identifiers
export type RackId = string;
export type NodeId = string;
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
  Stopped = 'Stopped',
  Starting = 'Starting',
  Running = 'Running',
  Failed = 'Failed'
}

export enum NodeHealth {
  Up = 'Up',
  Down = 'Down',
  Unknown = 'Unknown'
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
  nodes: NodeId[];
}

export interface Node {
  id: NodeId;
  rack_id: RackId;
  host: string;
  ssh: SshCreds;
  server?: ServerProcess;
}

export interface NodeStore {
  node_id: NodeId;
  store_id: StoreId;
  groups: NodeGroup[];
}

export interface LocalReplicaInfo {
  replica_id: ReplicaId;
  role: ReplicaRole;
  state: ReplicaState;
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
}

export interface ReplicaView {
  replica_id: ReplicaId;
  node_id: NodeId;
  store_id: string; // Added for logical tree
  group_id: string; // Added for logical tree
  role: ReplicaRole;
  state: ReplicaState;
}

// Common Enums
export enum ReplicaRole {
  Leader = 'Leader',
  Follower = 'Follower'
}

export enum ReplicaState {
  Up = 'Up',
  Down = 'Down',
  Unknown = 'Unknown'
}

export enum GroupHealth {
  Healthy = 'Healthy',
  Unhealthy = 'Unhealthy',
  Unknown = 'Unknown'
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

// Custom Action/Panel Types for Embedding
export interface CustomAction {
  id: string;
  label: string;
  icon?: React.ReactNode;
  appliesTo: ('Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica')[];
  viewModes?: ViewMode[];
  placement?: ('contextMenu' | 'inspector' | 'both')[];
  isDisabled?: (entity: any) => boolean;
}

export interface CustomPanel {
  id: string;
  label: string;
  appliesTo: ('Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica')[];
  component: React.ComponentType<{
    entity: any;
    viewMode: ViewMode;
    apiPrefix: string;
    pollingData: any;
  }>;
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
