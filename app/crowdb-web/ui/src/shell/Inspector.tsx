// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useMemo, lazy, Suspense, type MutableRefObject } from 'react';
import { X, Info, ListChecks, ExternalLink } from 'lucide-react';
import { useSelection, SelectedEntity } from '../contexts/SelectionContext';
import { useDomain } from '../contexts/DomainContext';
import { cn } from '../utils/cn';
import { Domain, Node, Rack, EnrichedStoreView, CrowdbKVServerView, CapacityUsageResponse, DiskdbInstanceInfo, HardwareCapacitySummary, ElectionState, ReadState, ReplicaRole } from '../types';
import { DEFAULT_DC_NAME } from '../data/defaultDatacenter';
import { ActivityLog } from '../panels/ActivityLog';
import { groupLabel, localReplicaLabel, nodeLabel, rackLabel, serverLabel, storeLabel } from '../utils/entityDisplay';
import { useMetricsPoll, buildMetricsFetcher } from '../utils/useMetricsPoll';
import { MetricsRegion, ElectionStateRegion, ReadStateRegion } from '../components/MetricsRegion';

const KvPanel = lazy(() => import('../panels/KvPanel').then((m) => ({ default: m.KvPanel })));

type TabId = 'details' | 'activity';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function displayEntityId(entity: SelectedEntity): string {
  switch (entity.type) {
    case 'Datacenter':
      return DEFAULT_DC_NAME;
    case 'Rack':
      return rackLabel(entity.id);
    case 'Node':
      return nodeLabel(entity.id);
    case 'Server':
      return entity.parentIds?.node_id ? serverLabel(String(entity.parentIds.node_id)) : entity.id;
    case 'Store':
      return storeLabel(entity.id);
    case 'Group':
      return groupLabel(entity.id);
    case 'Replica':
      return localReplicaLabel(entity.id);
    default:
      return entity.id;
  }
}

interface InspectorProps {
  readonly?: boolean;
  modules?: Record<string, boolean>;
  nodes?: Node[];
  racks?: Rack[];
  servers?: CrowdbKVServerView[];
  stores?: EnrichedStoreView[];
  capacityUsage?: CapacityUsageResponse | null;
  hardwareCapacity?: HardwareCapacitySummary | null;
  diskdbInstances?: DiskdbInstanceInfo[];
  width?: number;
  pendingSelectionRef?: MutableRefObject<SelectedEntity | null>;
}

/**
 * Right-side inspector. Reacts to SelectionContext: Details + Activity for any
 * selection.
 */
export function Inspector({ readonly, modules: _modules, nodes = [], racks = [], servers = [], stores = [], capacityUsage = null, hardwareCapacity = null, diskdbInstances = [], width = 320, pendingSelectionRef }: InspectorProps) {
  const { selectedEntity, clearSelection, selectEntity } = useSelection();
  const { setDomain } = useDomain();
  const [activeTab, setActiveTab] = useState<TabId>('details');

  if (!selectedEntity) return null;

  const displayType = selectedEntity.type === 'Server'
    ? (selectedEntity.serviceType === 'diskdb' ? 'DiskDB' : 'KV')
    : selectedEntity.type;
  const displayName = selectedEntity.name || displayEntityId(selectedEntity);

  return (
    <aside
      className="tw-fixed tw-right-0 tw-top-14 tw-bottom-0 tw-bg-panel tw-border-l tw-border-border tw-flex tw-flex-col tw-z-30 tw-shadow-2xl"
      style={{ width }}
      aria-label="Entity inspector"
    >
      <div className="tw-flex tw-items-start tw-justify-between tw-gap-2 tw-px-4 tw-py-3 tw-border-b tw-border-border">
        <div className="tw-flex-1 tw-min-w-0">
          <div className="tw-text-[10px] tw-uppercase tw-tracking-wider tw-text-muted">{displayType}</div>
          <div className="tw-text-sm tw-font-semibold tw-text-text tw-truncate">
            {displayName}
          </div>
        </div>
        <button onClick={clearSelection} className="tw-text-muted hover:tw-text-text" aria-label="Close inspector">
          <X className="tw-h-4 tw-w-4" />
        </button>
      </div>

      <div className="tw-flex tw-items-center tw-border-b tw-border-border tw-px-2 tw-text-xs">
        <Tab id="details" current={activeTab} set={setActiveTab} icon={<Info className="tw-h-3 tw-w-3" />} label="Details" />
        <Tab id="activity" current={activeTab} set={setActiveTab} icon={<ListChecks className="tw-h-3 tw-w-3" />} label="Activity" />
      </div>

      <div className="tw-flex-1 tw-overflow-y-auto">
        {activeTab === 'details' && (
          <DetailsTab entity={selectedEntity} nodes={nodes} racks={racks} servers={servers} stores={stores} capacityUsage={capacityUsage} hardwareCapacity={hardwareCapacity} diskdbInstances={diskdbInstances} selectEntity={selectEntity} setDomain={setDomain} readonly={readonly} pendingSelectionRef={pendingSelectionRef} />
        )}
        {activeTab === 'activity' && <ActivityLog />}
      </div>
    </aside>
  );
}

function Tab({
  id,
  current,
  set,
  icon,
  label,
}: {
  id: TabId;
  current: TabId;
  set: (id: TabId) => void;
  icon: React.ReactNode;
  label: string;
}) {
  const active = id === current;
  return (
    <button
      onClick={() => set(id)}
      className={cn(
        'tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-2 tw-border-b-2 tw-transition-colors',
        active ? 'tw-border-accent tw-text-accent' : 'tw-border-transparent tw-text-muted hover:tw-text-text',
      )}
      role="tab"
      aria-selected={active}
    >
      {icon}
      <span>{label}</span>
    </button>
  );
}

interface DetailsTabProps {
  entity: SelectedEntity;
  nodes: Node[];
  racks: Rack[];
  servers: CrowdbKVServerView[];
  stores: EnrichedStoreView[];
  capacityUsage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  diskdbInstances: DiskdbInstanceInfo[];
  selectEntity: (e: SelectedEntity | null) => void;
  setDomain: (m: Domain) => void;
  readonly?: boolean;
  pendingSelectionRef?: MutableRefObject<SelectedEntity | null>;
}

function DetailsTab({ entity, nodes, racks, servers, stores, capacityUsage, hardwareCapacity, diskdbInstances, selectEntity, setDomain, readonly, pendingSelectionRef }: DetailsTabProps) {
  const displayType = entity.type === 'Server'
    ? (entity.serviceType === 'diskdb' ? 'DiskDB' : 'KV')
    : entity.type;
  const displayId = displayEntityId(entity);
  const serverNodeId = entity.type === 'Node' ? entity.id : entity.parentIds?.node_id;
  const server =
    entity.type === 'Server'
      ? servers.find((item) => item.id === entity.id) || servers.find((item) => item.node_id === serverNodeId)
      : entity.type === 'Node'
        ? servers.find((item) => item.node_id === Number(entity.id))
        : undefined;
  const restPort = server?.rest_port ?? null;
  const rpcPort = server?.rpc_port ?? null;

  // Logical Replica: dig the full ReplicaView (role/state/engine_healthy/
  // crowtree_stats) out of `stores`. `stores` is `EnrichedStoreView[]`,
  // so `groups[].replicas` is typed `ReplicaView[]` — no cast needed.
  const replica =
    entity.type === 'Replica' && entity.domain === Domain.KV
      ? stores
          .find((s) => String(s.store_id) === entity.parentIds?.store_id)
          ?.groups.find((g) => String(g.group_id) === entity.parentIds?.group_id)
          ?.replicas.find((r) => String(r.replica_id) === entity.id)
      : undefined;

  // Logical Group: dig the full GroupView (read_state) out of `stores`.
  const groupView =
    entity.type === 'Group' && entity.domain === Domain.KV
      ? stores
          .find((s) => String(s.store_id) === entity.parentIds?.store_id)
          ?.groups.find((g) => String(g.group_id) === entity.id)
      : entity.type === 'Replica' && entity.domain === Domain.KV
        ? stores
            .find((s) => String(s.store_id) === entity.parentIds?.store_id)
            ?.groups.find((g) => String(g.group_id) === entity.parentIds?.group_id)
        : undefined;

  const electionState: ElectionState | undefined = replica?.election ?? groupView?.replicas.find((r) => r.role === ReplicaRole.Leader)?.election;
  const readState: ReadState | undefined = groupView?.read_state;

  // Capacity totals for the selected entity.
  // Uses hardwareCapacity (from group-0 sysdata) which is available
  // without diskdb ownership/binding. Falls back to capacityUsage
  // (from diskdb) for busy/free when available.
  // Works in both Capacity and Physical views for DiskGroup/Disk.
  const capacityTotals = useMemo(() => {
    const hwDgs = hardwareCapacity?.disk_groups || [];
    if (hwDgs.length === 0) return null;

    // In Physical view, only show capacity for DiskGroup and Disk
    // (rack/node/datacenter capacity is a Capacity-view concept).
    if (entity.domain === Domain.Cluster && entity.type !== 'DiskGroup' && entity.type !== 'Disk') return null;

    let dgs = hwDgs;
    if (entity.type === 'Datacenter') {
      dgs = hwDgs;
    } else if (entity.type === 'Rack') {
      const rackId = Number(entity.id);
      dgs = hwDgs.filter((g) => g.rack_id === rackId);
    } else if (entity.type === 'Node') {
      const nodeId = Number(entity.id);
      dgs = hwDgs.filter((g) => g.node_id === nodeId);
    } else if (entity.type === 'DiskGroup') {
      const dgId = Number(entity.parentIds?.disk_group_id ?? entity.id);
      dgs = hwDgs.filter((g) => g.disk_group_id === dgId);
    } else if (entity.type === 'Disk') {
      const dgId = Number(entity.parentIds?.disk_group_id);
      const diskId = String(entity.parentIds?.disk_id ?? entity.id);
      const dg = hwDgs.find((g) => g.disk_group_id === dgId);
      const disk = dg?.disks.find((d) => d.disk_id === diskId);
      if (disk) {
        const usageDg = capacityUsage?.disk_groups.find((g) => g.disk_group_id === dgId);
        const usageDisk = usageDg?.disks.find((d) => d.disk_id === diskId);
        return {
          capacity: disk.capacity_bytes,
          busy: usageDisk?.busy_bytes ?? 0,
          free: usageDisk?.free_bytes ?? disk.capacity_bytes,
        };
      }
      return null;
    } else {
      return null;
    }
    const capacity = dgs.reduce((sum, g) => sum + g.capacity_bytes, 0);
    const usageDgs = capacityUsage?.disk_groups || [];
    const busy = dgs.reduce((sum, g) => {
      const u = usageDgs.find((ud) => ud.disk_group_id === g.disk_group_id);
      return sum + (u?.busy_bytes ?? 0);
    }, 0);
    return { capacity, busy, free: capacity - busy };
  }, [entity.type, entity.domain, entity.id, entity.parentIds, hardwareCapacity, capacityUsage]);

  // Disk list for the selected DiskGroup (from hardwareCapacity sysdata).
  // Works in both Physical and Capacity views.
  const dgDisks = useMemo(() => {
    if (entity.type !== 'DiskGroup') return null;
    const dgId = Number(entity.parentIds?.disk_group_id ?? entity.id);
    const dg = hardwareCapacity?.disk_groups.find((g) => g.disk_group_id === dgId);
    return dg?.disks || [];
  }, [entity.type, entity.parentIds, entity.id, hardwareCapacity]);

  // Ownership info for DiskGroup: which diskdb instance owns this DG.
  const dgOwnerInstanceId = useMemo(() => {
    if (entity.type !== 'DiskGroup') return undefined;
    const dgId = Number(entity.parentIds?.disk_group_id ?? entity.id);
    const owner = diskdbInstances.find((inst) => inst.owned_dg_ids.includes(dgId));
    return owner?.instance_id;
  }, [entity.type, entity.parentIds, entity.id, diskdbInstances]);

  // Metrics poll: build a fetcher for the current entity type.
  const parentStoreId = entity.parentIds?.store_id != null ? String(entity.parentIds.store_id) : undefined;
  const parentGroupId = entity.parentIds?.group_id != null ? String(entity.parentIds.group_id) : undefined;
  const metricsFetcherInfo = buildMetricsFetcher(
    entity.type,
    entity.id,
    parentStoreId,
    parentGroupId,
  );
  const metricsData = useMetricsPoll(
    metricsFetcherInfo?.fetcher ?? null,
    metricsFetcherInfo?.key ?? 'none',
  );

  const fields: { label: string; value: string }[] = [
    { label: 'Type', value: displayType },
    { label: 'ID', value: displayId },
    ...(entity.name && entity.type !== 'Server' && entity.type !== 'Disk' ? [{ label: 'Name', value: entity.name }] : []),
    ...(entity.type === 'Datacenter' ? [{ label: 'Rack Count', value: String(racks.length) }] : []),
    ...(capacityTotals
      ? [
          { label: 'Total Capacity', value: formatBytes(capacityTotals.capacity) },
          { label: 'Used', value: formatBytes(capacityTotals.busy) },
          { label: 'Free', value: formatBytes(capacityTotals.free) },
        ]
      : []),
    ...(dgOwnerInstanceId !== undefined
      ? [{ label: 'Owner Instance', value: `diskdb-${dgOwnerInstanceId}` }]
      : entity.type === 'DiskGroup'
        ? [{ label: 'Owner Instance', value: 'unassigned' }]
        : []),
    ...(restPort ? [{ label: 'REST Port', value: String(restPort) }] : []),
    ...(rpcPort ? [{ label: 'RPC Port', value: String(rpcPort) }] : []),
    ...Object.entries(entity.parentIds || {})
      .filter(([k, v]) => v && k !== 'disk_id')
      .map(([k, v]) => ({ label: `Parent: ${k}`, value: String(v) })),
    ...(replica && typeof replica.engine_healthy === 'boolean'
      ? [{ label: 'Engine Healthy', value: replica.engine_healthy ? 'Yes' : 'No' }]
      : []),
    ...(replica?.crowtree_stats
      ? [
          { label: 'Last Applied Slot', value: String(replica.crowtree_stats.last_applied_slot) },
          { label: 'Contiguous Slot', value: String(replica.crowtree_stats.contiguous_slot) },
          { label: 'GC Watermark', value: String(replica.crowtree_stats.gc_watermark) },
          { label: 'Snapshot Pages Written', value: String(replica.crowtree_stats.snapshot_pages_written) },
          { label: 'Snapshot Segments Written', value: String(replica.crowtree_stats.snapshot_segments_written) },
          {
            label: 'Buffer Pool Hit Rate',
            value: bufferPoolHitRate(replica.crowtree_stats.buffer_pool_hits, replica.crowtree_stats.buffer_pool_misses),
          },
          {
            label: 'Buffer Pool Resident/Used/Frames',
            value: `${replica.crowtree_stats.buffer_pool_resident}/${replica.crowtree_stats.buffer_pool_used}/${replica.crowtree_stats.buffer_pool_num_frames}`,
          },
        ]
      : []),
  ];

  // Single cross-jump per design §3.1.
  const crossJump = buildCrossJump(entity, nodes, stores, selectEntity, setDomain, pendingSelectionRef);

  return (
    <div className="tw-p-3 tw-space-y-3">
      <dl className="tw-divide-y tw-divide-border tw-border tw-border-border tw-rounded-md tw-overflow-hidden">
        {fields.map((f) => (
          <div key={f.label} className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-text-xs tw-gap-2">
            <dt className="tw-text-muted tw-flex-shrink-0">{f.label}</dt>
            <dd className="tw-font-mono tw-text-text tw-text-right tw-select-text tw-break-all tw-whitespace-pre-wrap">
              {f.value}
            </dd>
          </div>
        ))}
      </dl>

      {dgDisks && dgDisks.length > 0 && (
        <div className="tw-space-y-1">
          <div className="tw-text-[10px] tw-uppercase tw-tracking-wider tw-text-muted">Disks ({dgDisks.length})</div>
          <div className="tw-border tw-border-border tw-rounded-md tw-overflow-hidden">
            <table className="tw-w-full tw-text-xs">
              <thead>
                <tr className="tw-bg-bg tw-text-muted">
                  <th className="tw-text-left tw-px-2 tw-py-1">Disk ID</th>
                  <th className="tw-text-right tw-px-2 tw-py-1">Capacity</th>
                  <th className="tw-text-right tw-px-2 tw-py-1">Zones</th>
                </tr>
              </thead>
              <tbody>
                {dgDisks.map((d) => (
                  <tr key={d.disk_id} className="tw-border-t tw-border-border">
                    <td className="tw-px-2 tw-py-1 tw-font-mono tw-text-text">{d.disk_id.slice(0, 12)}…</td>
                    <td className="tw-px-2 tw-py-1 tw-text-right tw-font-mono tw-text-text">{formatBytes(d.capacity_bytes)}</td>
                    <td className="tw-px-2 tw-py-1 tw-text-right tw-font-mono tw-text-text">{d.zone_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {crossJump && (
        <button
          onClick={crossJump.go}
          className="tw-w-full tw-flex tw-items-center tw-justify-center tw-gap-1.5 tw-px-3 tw-py-2 tw-rounded-md tw-border tw-border-border tw-text-xs tw-text-accent hover:tw-bg-bg tw-transition-colors"
        >
          <ExternalLink className="tw-h-3.5 tw-w-3.5" />
          {crossJump.label}
        </button>
      )}

      {electionState && <ElectionStateRegion state={electionState} />}
      {readState && <ReadStateRegion state={readState} />}
      <MetricsRegion data={metricsData} />

      {entity.type === 'Group' && parentStoreId && (
        <div className="tw-space-y-1">
          <div className="tw-text-[10px] tw-uppercase tw-tracking-wider tw-text-muted">KV</div>
          <Suspense fallback={null}>
            <KvPanel storeId={parentStoreId} groupId={entity.id} readonly={readonly} />
          </Suspense>
        </div>
      )}
    </div>
  );
}

/** `hits / (hits + misses)` as a percentage string; `"n/a"` with no accesses yet. */
function bufferPoolHitRate(hits: number, misses: number): string {
  const total = hits + misses;
  if (total === 0) return 'n/a';
  return `${((hits / total) * 100).toFixed(1)}%`;
}

/** Build the single most useful cross-jump for the current selection. */
function buildCrossJump(
  entity: SelectedEntity,
  nodes: Node[],
  stores: EnrichedStoreView[],
  selectEntity: (e: SelectedEntity | null) => void,
  setDomain: (m: Domain) => void,
  pendingSelectionRef?: MutableRefObject<SelectedEntity | null>,
): { label: string; go: () => void } | null {
  // Logical Replica -> physical Node ("show on node").
  if (entity.domain === Domain.KV && entity.type === 'Replica') {
    const nodeId = entity.parentIds?.node_id;
    if (nodeId) {
      const node = nodes.find((n) => n.id === Number(nodeId));
      return {
        label: `Show on node ${nodeId}`,
        go: () => {
          const target: SelectedEntity = {
            type: 'Node',
            id: String(nodeId),
            domain: Domain.Cluster,
            parentIds: node?.rack_id ? { rack_id: node.rack_id } : {},
            name: node?.host,
          };
          if (pendingSelectionRef) pendingSelectionRef.current = target;
          setDomain(Domain.Cluster);
          selectEntity(target);
        },
      };
    }
  }
  // Physical Node -> logical Store ("show in cluster").
  if (entity.domain === Domain.Cluster && entity.type === 'Node') {
    const store = stores.find((s) => String(s.store_id) !== '0' && s.nodes?.includes(Number(entity.id)));
    if (store) {
      return {
        label: `Show store ${store.store_id} in cluster`,
        go: () => {
          const target: SelectedEntity = { type: 'Store', id: String(store.store_id), domain: Domain.KV, name: store.name };
          if (pendingSelectionRef) pendingSelectionRef.current = target;
          setDomain(Domain.KV);
          selectEntity(target);
        },
      };
    }
  }
  return null;
}
