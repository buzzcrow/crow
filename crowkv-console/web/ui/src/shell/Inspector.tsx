import { useState } from 'react';
import { X, Info, ListChecks, Database, ExternalLink } from 'lucide-react';
import { useSelection, SelectedEntity } from '../contexts/SelectionContext';
import { useViewMode } from '../contexts/ViewModeContext';
import { cn } from '../utils/cn';
import { ViewMode, Node, StoreView, CrowKVServerView } from '../types';
import { KvPanel } from '../panels/KvPanel';
import { ActivityLog } from '../panels/ActivityLog';
import { groupLabel, localReplicaLabel, nodeLabel, rackLabel, serverLabel, storeLabel } from '../utils/entityDisplay';

type TabId = 'details' | 'activity' | 'kv';

function displayEntityId(entity: SelectedEntity): string {
  switch (entity.type) {
    case 'Rack':
      return rackLabel(entity.id);
    case 'Node':
      return nodeLabel(entity.id);
    case 'Server':
      return entity.parentIds?.node_id ? serverLabel(entity.parentIds.node_id) : entity.id;
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
  servers?: CrowKVServerView[];
  stores?: StoreView[];
  width?: number;
}

/**
 * Right-side inspector. Reacts to SelectionContext: Details + Activity for any
 * selection, plus a KV tab when a logical Group is selected.
 */
export function Inspector({ readonly, modules, nodes = [], servers = [], stores = [], width = 320 }: InspectorProps) {
  const { selectedEntity, clearSelection, selectEntity } = useSelection();
  const { setViewMode } = useViewMode();
  const [activeTab, setActiveTab] = useState<TabId>('details');

  if (!selectedEntity) return null;

  const kvEnabled =
    modules?.kv !== false &&
    selectedEntity.type === 'Group' &&
    selectedEntity.viewMode === ViewMode.Logical;
  const displayType = selectedEntity.type === 'Server' ? 'CrowKV' : selectedEntity.type;
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
        {kvEnabled && (
          <Tab id="kv" current={activeTab} set={setActiveTab} icon={<Database className="tw-h-3 tw-w-3" />} label="KV" />
        )}
      </div>

      <div className="tw-flex-1 tw-overflow-y-auto">
        {activeTab === 'details' && (
          <DetailsTab entity={selectedEntity} nodes={nodes} servers={servers} stores={stores} selectEntity={selectEntity} setViewMode={setViewMode} />
        )}
        {activeTab === 'activity' && <ActivityLog />}
        {activeTab === 'kv' && kvEnabled && (
          <KvPanel storeId={selectedEntity.parentIds?.store_id || ''} groupId={selectedEntity.id} readonly={readonly} />
        )}
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
  servers: CrowKVServerView[];
  stores: StoreView[];
  selectEntity: (e: SelectedEntity | null) => void;
  setViewMode: (m: ViewMode) => void;
}

function DetailsTab({ entity, nodes, servers, stores, selectEntity, setViewMode }: DetailsTabProps) {
  const displayType = entity.type === 'Server' ? 'CrowKV' : entity.type;
  const displayId = displayEntityId(entity);
  const serverNodeId = entity.type === 'Node' ? entity.id : entity.parentIds?.node_id;
  const server =
    entity.type === 'Server'
      ? servers.find((item) => item.id === entity.id) || servers.find((item) => item.node_id === serverNodeId)
      : entity.type === 'Node'
        ? servers.find((item) => item.node_id === entity.id)
        : undefined;
  const mgmtPort = server?.mgmt_port ?? null;
  const grpcPort = server?.grpc_port ?? null;

  const fields: { label: string; value: string }[] = [
    { label: 'Type', value: displayType },
    { label: 'ID', value: displayId },
    ...(entity.name && entity.type !== 'Server' ? [{ label: 'Name', value: entity.name }] : []),
    ...(mgmtPort ? [{ label: 'Management Port', value: String(mgmtPort) }] : []),
    ...(grpcPort ? [{ label: 'gRPC Port', value: String(grpcPort) }] : []),
    ...Object.entries(entity.parentIds || {})
      .filter(([, v]) => v)
      .map(([k, v]) => ({ label: `Parent: ${k}`, value: v })),
  ];

  // Single cross-jump per design §3.1.
  const crossJump = buildCrossJump(entity, nodes, stores, selectEntity, setViewMode);

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

      {crossJump && (
        <button
          onClick={crossJump.go}
          className="tw-w-full tw-flex tw-items-center tw-justify-center tw-gap-1.5 tw-px-3 tw-py-2 tw-rounded-md tw-border tw-border-border tw-text-xs tw-text-accent hover:tw-bg-bg tw-transition-colors"
        >
          <ExternalLink className="tw-h-3.5 tw-w-3.5" />
          {crossJump.label}
        </button>
      )}
    </div>
  );
}

/** Build the single most useful cross-jump for the current selection. */
function buildCrossJump(
  entity: SelectedEntity,
  nodes: Node[],
  stores: StoreView[],
  selectEntity: (e: SelectedEntity | null) => void,
  setViewMode: (m: ViewMode) => void,
): { label: string; go: () => void } | null {
  // Logical Replica -> physical Node ("show on node").
  if (entity.viewMode === ViewMode.Logical && entity.type === 'Replica') {
    const nodeId = entity.parentIds?.node_id;
    if (nodeId) {
      const node = nodes.find((n) => n.id === nodeId);
      return {
        label: `Show on node ${nodeId}`,
        go: () => {
          setViewMode(ViewMode.Physical);
          selectEntity({
            type: 'Node',
            id: nodeId,
            viewMode: ViewMode.Physical,
            parentIds: node?.rack_id ? { rack_id: node.rack_id } : {},
            name: node?.host,
          });
        },
      };
    }
  }
  // Physical Node -> logical Store ("show in cluster").
  if (entity.viewMode === ViewMode.Physical && entity.type === 'Node') {
    const store = stores.find((s) => s.nodes?.includes(entity.id));
    if (store) {
      return {
        label: `Show store ${store.store_id} in cluster`,
        go: () => {
          setViewMode(ViewMode.Logical);
          selectEntity({ type: 'Store', id: String(store.store_id), viewMode: ViewMode.Logical, name: store.name });
        },
      };
    }
  }
  return null;
}
