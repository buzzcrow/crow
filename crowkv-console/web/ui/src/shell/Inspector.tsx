import { useMemo, useState, useCallback } from 'react';
import { X, Star, StarOff, Info, BarChart3, ListChecks, ExternalLink, Database, Loader2, Trash2, Search, Copy, AlertTriangle } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { useSelection, SelectedEntity } from '../contexts/SelectionContext';
import { useViewMode } from '../contexts/ViewModeContext';
import { useActivity } from '../contexts/ActivityContext';
import { useToast } from '../contexts/ToastContext';
import { cn } from '../utils/cn';
import { exportAsJSON, exportAsCSV } from '../utils/exportUtils';
import { ExportDropdown } from '../components/ExportDropdown';
import { FilterControls, SortState } from '../components/FilterControls';
import { ActivityLogEntry, CustomAction, CustomPanel, ViewMode, Node, StoreView } from '../types';
import { useMetricsHistory } from '../data/useMetricsHistory';
import { kvGet, kvPut, kvDelete, kvScan, KvScanItem, type KvGetResponse } from '../api';

export type InspectorTabId = 'details' | 'metrics' | 'activity' | string;

interface InspectorProps {
  /** Optional host-injected panels rendered after the built-in tabs. */
  customPanels?: CustomPanel[];
  /** Optional host-injected actions rendered in the inspector header. */
  customActions?: CustomAction[];
  /** API prefix forwarded to custom panels. */
  apiPrefix?: string;
  /** Structured event callback (e.g. when a custom action fires). */
  onEvent?: (event: { type: string; payload?: unknown }) => void;
  /** Physical tree nodes for cross-jump functionality */
  nodes?: Node[];
  /** Logical tree stores for cross-jump functionality */
  stores?: StoreView[];
}

/**
 * Right-side inspector. Reacts to SelectionContext: when an entity is
 * selected the panel slides in with tabs for Details, Metrics, and
 * Activity (plus any custom panels supplied by the embedding host).
 */
export function Inspector({ customPanels, customActions, apiPrefix = '/api', onEvent, nodes = [], stores = [] }: InspectorProps) {
  const { selectedEntity, clearSelection, isFavorite, addToFavorites, removeFromFavorites, selectEntity } =
    useSelection();
  const { setViewMode } = useViewMode();
  const [activeTab, setActiveTab] = useState<InspectorTabId>('details');

  if (!selectedEntity) return null;

  // Show KV tab only for Group entities in Logical view
  const isGroupInLogicalView = selectedEntity.type === 'Group' && selectedEntity.viewMode === ViewMode.Logical;

  // Restrict custom panels to ones that apply to the current entity type.
  const applicableCustomPanels = (customPanels || []).filter((p) =>
    p.appliesTo.includes(selectedEntity.type as 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica'),
  );

  const favorited = isFavorite(selectedEntity.id);

  const toggleFavorite = () => {
    if (favorited) removeFromFavorites(selectedEntity.id);
    else addToFavorites(selectedEntity);
  };

  return (
    <aside
      className="tw-fixed tw-right-0 tw-top-14 tw-bottom-0 tw-w-80 tw-bg-panel tw-border-l tw-border-border tw-flex tw-flex-col tw-z-30 tw-animate-slide-in-right tw-shadow-2xl"
      aria-label="Entity inspector"
    >
      {/* Header */}
      <div className="tw-flex tw-items-start tw-justify-between tw-gap-2 tw-px-4 tw-py-3 tw-border-b tw-border-border">
        <div className="tw-flex-1 tw-min-w-0">
          <div className="tw-text-[10px] tw-uppercase tw-tracking-wider tw-text-muted">
            {selectedEntity.type}
          </div>
          <div className="tw-text-sm tw-font-semibold tw-text-text tw-truncate">
            {selectedEntity.name || selectedEntity.id}
          </div>
        </div>
        <button
          onClick={toggleFavorite}
          className="tw-text-muted hover:tw-text-yellow-400 tw-transition-colors"
          aria-label={favorited ? 'Remove from favorites' : 'Add to favorites'}
          title={favorited ? 'Remove from favorites' : 'Add to favorites'}
        >
          {favorited ? <Star className="tw-h-4 tw-w-4 tw-fill-yellow-400 tw-text-yellow-400" /> : <StarOff className="tw-h-4 tw-w-4" />}
        </button>
        <button
          onClick={clearSelection}
          className="tw-text-muted hover:tw-text-text tw-transition-colors"
          aria-label="Close inspector"
        >
          <X className="tw-h-4 tw-w-4" />
        </button>
      </div>

      {/* Tabs */}
      <div className="tw-flex tw-items-center tw-border-b tw-border-border tw-px-2 tw-text-xs tw-overflow-x-auto">
        <TabButton id="details" current={activeTab} setCurrent={setActiveTab} icon={<Info className="tw-h-3 tw-w-3" />} label="Details" />
        <TabButton id="metrics" current={activeTab} setCurrent={setActiveTab} icon={<BarChart3 className="tw-h-3 tw-w-3" />} label="Metrics" />
        <TabButton id="activity" current={activeTab} setCurrent={setActiveTab} icon={<ListChecks className="tw-h-3 tw-w-3" />} label="Activity" />
        {isGroupInLogicalView && (
          <TabButton id="kv" current={activeTab} setCurrent={setActiveTab} icon={<Database className="tw-h-3 tw-w-3" />} label="KV" />
        )}
        {applicableCustomPanels.map((p) => (
          <TabButton key={p.id} id={p.id} current={activeTab} setCurrent={setActiveTab} label={p.label} />
        ))}
      </div>

      {/* Body */}
      <div className="tw-flex-1 tw-overflow-y-auto">
        {activeTab === 'details' && (
          <DetailsTab
            entity={selectedEntity}
            customActions={customActions}
            onEvent={onEvent}
            selectEntity={selectEntity}
            setViewMode={setViewMode}
            nodes={nodes}
            stores={stores}
          />
        )}
        {activeTab === 'metrics' && <MetricsTab entity={selectedEntity} />}
        {activeTab === 'activity' && <ActivityTab entity={selectedEntity} />}
        {activeTab === 'kv' && isGroupInLogicalView && <KvTab entity={selectedEntity} />}
        {applicableCustomPanels.map((panel) =>
          activeTab === panel.id ? (
            <div key={panel.id} className="tw-p-3">
              <panel.component
                entity={selectedEntity}
                viewMode={selectedEntity.viewMode}
                apiPrefix={apiPrefix}
                pollingData={null}
              />
            </div>
          ) : null,
        )}
      </div>
    </aside>
  );
}

interface TabButtonProps {
  id: InspectorTabId;
  current: InspectorTabId;
  setCurrent: (id: InspectorTabId) => void;
  icon?: React.ReactNode;
  label: string;
}

function TabButton({ id, current, setCurrent, icon, label }: TabButtonProps) {
  const isActive = id === current;
  return (
    <button
      onClick={() => setCurrent(id)}
      className={cn(
        'tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-2 tw-border-b-2 tw-transition-colors tw-flex-shrink-0',
        isActive
          ? 'tw-border-accent tw-text-accent'
          : 'tw-border-transparent tw-text-muted hover:tw-text-text',
      )}
      role="tab"
      aria-selected={isActive}
    >
      {icon}
      <span>{label}</span>
    </button>
  );
}

// -------------------- Details --------------------

function DetailsTab({
  entity,
  customActions,
  onEvent,
  selectEntity,
  setViewMode,
  nodes,
  stores,
}: {
  entity: SelectedEntity;
  customActions?: CustomAction[];
  onEvent?: (event: { type: string; payload?: unknown }) => void;
  selectEntity: (entity: SelectedEntity | null) => void;
  setViewMode: (mode: ViewMode) => void;
  nodes: Node[];
  stores: StoreView[];
}) {
  // Build base fields
  const baseFields: { label: string; value: string; clickable?: () => void }[] = [
    { label: 'Type', value: entity.type },
    { label: 'ID', value: entity.id },
    ...(entity.name ? [{ label: 'Name', value: entity.name }] : []),
    { label: 'View', value: entity.viewMode },
    ...Object.entries(entity.parentIds || {}).map(([k, v]) => {
      const node = k === 'node_id' ? nodes.find(n => n.id === v) : undefined;
      const clickable =
        entity.type === 'Replica' && entity.viewMode === ViewMode.Logical && k === 'node_id'
          ? () => {
              setViewMode(ViewMode.Physical);
              selectEntity({
                type: 'Node',
                id: v,
                viewMode: ViewMode.Physical,
                parentIds: node?.rack_id ? { rack_id: node.rack_id } : {},
                name: node?.host,
              });
            }
          : undefined;
      return {
        label: `Parent: ${k}`,
        value: v,
        clickable,
      };
    }),
  ];

  // Build cross-jump fields based on entity type and view mode
  const crossJumpFields: { label: string; value: string; clickable?: () => void }[] = [];

  // Logical view cross-jumps to physical view
  if (entity.viewMode === ViewMode.Logical) {
    if (entity.type === 'Store') {
      // Store -> Jump to corresponding Nodes in physical view
      const store = stores.find(s => s.store_id === entity.id);
      if (store?.nodes) {
        store.nodes.forEach(nodeId => {
          const node = nodes.find(n => n.id === nodeId);
          crossJumpFields.push({
            label: 'Running on Node',
            value: node?.host || nodeId,
            clickable: () => {
              setViewMode(ViewMode.Physical);
              selectEntity({
                type: 'Node',
                id: nodeId,
                viewMode: ViewMode.Physical,
                parentIds: node?.rack_id ? { rack_id: node.rack_id } : {},
                name: node?.host,
              });
            },
          });
        });
      }
    } else if (entity.type === 'Replica') {
      // Replica -> Jump to corresponding Node in physical view
      const directNodeId = entity.parentIds?.node_id;
      if (directNodeId) {
        const node = nodes.find(n => n.id === directNodeId);
        crossJumpFields.push({
          label: 'Running on Node',
          value: node?.host || directNodeId,
          clickable: () => {
            setViewMode(ViewMode.Physical);
            selectEntity({
              type: 'Node',
              id: directNodeId,
              viewMode: ViewMode.Physical,
              parentIds: node?.rack_id ? { rack_id: node.rack_id } : {},
              name: node?.host,
            });
          },
        });
      }
      // Find the store to get the replica info
      for (const store of stores) {
        if (store.groups) {
        for (const group of store.groups) {
          // @ts-ignore - group may have replicas
          if (group.replicas) {
            // @ts-ignore
            const replica = group.replicas.find((r: any) => String(r.replica_id) === entity.id);
            if (replica?.node_id) {
              const node = nodes.find(n => n.id === replica.node_id);
              crossJumpFields.push({
                label: 'Running on Node',
                value: node?.host || replica.node_id,
                clickable: () => {
                  setViewMode(ViewMode.Physical);
                  selectEntity({
                    type: 'Node',
                    id: replica.node_id,
                    viewMode: ViewMode.Physical,
                    parentIds: node?.rack_id ? { rack_id: node.rack_id } : {},
                    name: node?.host,
                  });
                },
              });
              break;
            }
          }
        }
        }
      }
    }
  } else {
    // Physical view cross-jumps to logical view
    if (entity.type === 'Node') {
      // Node -> Jump to corresponding Stores in logical view
      stores.forEach(store => {
        if (store.nodes.includes(entity.id)) {
          crossJumpFields.push({
            label: 'Hosts Store',
            value: store.name || store.store_id,
            clickable: () => {
              setViewMode(ViewMode.Logical);
              selectEntity({
                type: 'Store',
                id: store.store_id,
                viewMode: ViewMode.Logical,
                name: store.name,
              });
            },
          });
        }
      });
    }
  }

  const fields = [...baseFields, ...crossJumpFields];

  const exportOptions = [
    {
      id: 'json',
      label: 'Export as JSON',
      onSelect: () => exportAsJSON(entity, `${entity.type}-${entity.id}.json`),
    },
  ];

  // Filter host-supplied actions down to those applicable to this entity +
  // placed in the inspector.
  const applicableActions = (customActions || []).filter(
    (a) =>
      a.appliesTo.includes(entity.type) &&
      (!a.placement || a.placement.includes('inspector') || a.placement.includes('both')) &&
      (!a.viewModes || a.viewModes.includes(entity.viewMode)),
  );

  return (
    <div className="tw-p-3 tw-space-y-3">
      <div className="tw-flex tw-justify-end">
        <ExportDropdown options={exportOptions} />
      </div>
      {applicableActions.length > 0 && (
        <div className="tw-flex tw-flex-wrap tw-gap-1.5">
          {applicableActions.map((action) => {
            const disabled = action.isDisabled?.(entity) ?? false;
            return (
              <button
                key={action.id}
                onClick={() =>
                  onEvent?.({
                    type: 'customAction',
                    payload: { actionId: action.id, entity },
                  })
                }
                disabled={disabled}
                className={cn(
                  'tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-rounded tw-text-xs tw-border tw-transition-colors',
                  disabled
                    ? 'tw-bg-bg tw-text-muted tw-border-border tw-opacity-50 tw-cursor-not-allowed'
                    : 'tw-bg-bg tw-text-text tw-border-border hover:tw-bg-panel',
                )}
              >
                {action.icon}
                {action.label}
              </button>
            );
          })}
        </div>
      )}
      <dl className="tw-divide-y tw-divide-border tw-border tw-border-border tw-rounded-md tw-overflow-hidden">
        {fields.map((f) => (
          <div key={f.label} className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-text-xs">
            <dt className="tw-text-muted">{f.label}</dt>
            <dd className={cn(
              'tw-font-mono tw-truncate tw-ml-2',
              f.clickable ? 'tw-text-accent hover:tw-underline tw-cursor-pointer' : 'tw-text-text'
            )} onClick={f.clickable}>
              <span className="tw-flex tw-items-center tw-gap-1">
                {f.value}
                {f.clickable && <ExternalLink className="tw-h-3 tw-w-3" />}
              </span>
            </dd>
          </div>
        ))}
      </dl>
      <p className="tw-text-[10px] tw-text-muted">
        Additional entity-specific fields will appear here once the backend exposes them via
        per-resource endpoints.
      </p>
    </div>
  );
}

// -------------------- Metrics --------------------

type TimeRange = '15m' | '1h' | '6h' | '1d';
const RANGE_MS: Record<TimeRange, number> = {
  '15m': 15 * 60 * 1000,
  '1h': 60 * 60 * 1000,
  '6h': 6 * 60 * 60 * 1000,
  '1d': 24 * 60 * 60 * 1000,
};

function MetricsTab({ entity }: { entity: SelectedEntity }) {
  const [range, setRange] = useState<TimeRange>('1h');
  const { getMetricHistory } = useMetricsHistory({ maxPoints: 2880 });

  // Pull a couple of representative series so we have a non-empty UI shape.
  const seriesNames = ['cpu', 'rps'] as const;
  const cutoff = Date.now() - RANGE_MS[range];

  const series = useMemo(() => {
    return seriesNames.map((name) => {
      const points = getMetricHistory(name, { entityId: entity.id }).filter(
        (p) => p.timestamp >= cutoff,
      );
      return { name, points };
    });
  }, [getMetricHistory, entity.id, cutoff]);

  const allPoints = useMemo(() => {
    const rows: { name: string; timestamp: string; value: number }[] = [];
    for (const s of series) {
      for (const p of s.points) {
        rows.push({ name: s.name, timestamp: new Date(p.timestamp).toISOString(), value: p.value });
      }
    }
    return rows;
  }, [series]);

  const exportOptions = [
    {
      id: 'csv',
      label: 'Export as CSV',
      hint: `${allPoints.length} samples`,
      onSelect: () =>
        exportAsCSV(
          allPoints,
          [
            { key: 'timestamp', label: 'Timestamp' },
            { key: 'name', label: 'Metric' },
            { key: 'value', label: 'Value' },
          ],
          `metrics-${entity.id}-${range}.csv`,
        ),
    },
  ];

  // Format data for Recharts
  const chartData = useMemo(() => {
    // Find the maximum number of points across all series
    const maxPoints = Math.max(...series.map(s => s.points.length));
    if (maxPoints === 0) return [];

    // Create a map of timestamps to values for each series
    const timestampMap = new Map<number, Record<string, number>>();

    series.forEach((s) => {
      s.points.forEach((p) => {
        if (!timestampMap.has(p.timestamp)) {
          timestampMap.set(p.timestamp, {});
        }
        timestampMap.get(p.timestamp)![s.name] = p.value;
      });
    });

    // Convert to sorted array
    return Array.from(timestampMap.entries())
      .sort(([a], [b]) => a - b)
      .map(([timestamp, values]) => ({
        timestamp,
        time: new Date(timestamp).toLocaleTimeString(),
        ...values,
      }));
  }, [series]);

  return (
    <div className="tw-p-3 tw-space-y-3">
      <div className="tw-flex tw-items-center tw-justify-between">
        <select
          value={range}
          onChange={(e) => setRange(e.target.value as TimeRange)}
          className="tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text"
          aria-label="Time range"
        >
          <option value="15m">Last 15 min</option>
          <option value="1h">Last 1 hour</option>
          <option value="6h">Last 6 hours</option>
          <option value="1d">Last 24 hours</option>
        </select>
        <ExportDropdown options={exportOptions} />
      </div>
      {chartData.length === 0 ? (
        <div className="tw-h-64 tw-flex tw-items-center tw-justify-center tw-text-[10px] tw-text-muted tw-border tw-border-border tw-rounded-md">
          No metric data available yet
        </div>
      ) : (
        <div className="tw-h-64 tw-border tw-border-border tw-rounded-md tw-p-2">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
              <XAxis
                dataKey="time"
                stroke="var(--color-muted)"
                tick={{ fontSize: 10 }}
                tickFormatter={(value) => value.split(':').slice(0, 2).join(':')}
              />
              <YAxis
                stroke="var(--color-muted)"
                tick={{ fontSize: 10 }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'var(--color-bg)',
                  border: '1px solid var(--color-border)',
                  borderRadius: '4px',
                  fontSize: '10px',
                }}
                labelStyle={{ color: 'var(--color-text)' }}
              />
              {series.map((s, index) => (
                <Line
                  key={s.name}
                  type="monotone"
                  dataKey={s.name}
                  stroke={index === 0 ? '#88c0d0' : '#bf616a'}
                  strokeWidth={1.5}
                  dot={false}
                  activeDot={{ r: 4 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
      <p className="tw-text-[10px] tw-text-muted">
        Live metric polling will populate these series once the backend metrics endpoint is wired
        in.
      </p>
    </div>
  );
}

// -------------------- Activity --------------------

const ACTIVITY_FILTERS = [
  {
    id: 'status',
    label: 'Status',
    options: [
      { value: 'Success', label: 'Success' },
      { value: 'Failed', label: 'Failed' },
      { value: 'Pending', label: 'Pending' },
    ],
  },
];

const ACTIVITY_SORTS = [
  { id: 'timestamp', label: 'Timestamp' },
  { id: 'action', label: 'Action' },
];

function ActivityTab({ entity }: { entity: SelectedEntity }) {
  const { entries, clear } = useActivity();
  const [filters, setFilters] = useState<Record<string, string[]>>({});
  const [sort, setSort] = useState<SortState>({ id: 'timestamp', direction: 'desc' });
  const [scope, setScope] = useState<'all' | 'entity'>('entity');

  const filtered = useMemo(() => {
    let rows = entries.slice();
    if (scope === 'entity') {
      rows = rows.filter((r) => r.target.includes(entity.id));
    }
    const statusFilters = filters.status || [];
    if (statusFilters.length > 0) {
      rows = rows.filter((r) => statusFilters.includes(r.status));
    }
    rows.sort((a, b) => {
      const dir = sort.direction === 'asc' ? 1 : -1;
      if (sort.id === 'action') return a.action.localeCompare(b.action) * dir;
      return (a.timestamp - b.timestamp) * dir;
    });
    return rows;
  }, [entries, scope, filters, sort, entity.id]);

  const exportOptions = [
    {
      id: 'csv',
      label: 'Export as CSV',
      hint: `${filtered.length} entries`,
      onSelect: () =>
        exportAsCSV<ActivityLogEntry>(
          filtered,
          [
            { key: 'timestamp', label: 'Timestamp' },
            { key: 'action', label: 'Action' },
            { key: 'target', label: 'Target' },
            { key: 'status', label: 'Status' },
            { key: 'message', label: 'Message' },
          ],
          `activity-${entity.id}.csv`,
        ),
    },
  ];

  return (
    <div className="tw-p-3 tw-space-y-3">
      <div className="tw-flex tw-items-center tw-justify-between">
        <div className="tw-flex tw-items-center tw-gap-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-p-0.5">
          <ScopeButton
            current={scope}
            setCurrent={setScope}
            value="entity"
            label="This entity"
          />
          <ScopeButton current={scope} setCurrent={setScope} value="all" label="All" />
        </div>
        <ExportDropdown options={exportOptions} />
      </div>
      <FilterControls
        presetNamespace={`activity-${entity.viewMode}`}
        filterDimensions={ACTIVITY_FILTERS}
        sortOptions={ACTIVITY_SORTS}
        selectedFilters={filters}
        selectedSort={sort}
        onFiltersChange={setFilters}
        onSortChange={setSort}
      />
      <div className="tw-flex tw-justify-end">
        <button
          onClick={clear}
          disabled={entries.length === 0}
          className="tw-text-xs tw-text-muted hover:tw-text-failed disabled:tw-opacity-30 tw-transition-colors"
        >
          Clear log
        </button>
      </div>
      <div className="tw-border tw-border-border tw-rounded-md tw-divide-y tw-divide-border tw-max-h-[60vh] tw-overflow-y-auto">
        {filtered.length === 0 ? (
          <div className="tw-px-3 tw-py-6 tw-text-center tw-text-xs tw-text-muted">
            No activity entries.
          </div>
        ) : (
          filtered.map((entry) => <ActivityRow key={entry.id} entry={entry} />)
        )}
      </div>
    </div>
  );
}

interface ScopeButtonProps {
  current: 'all' | 'entity';
  setCurrent: (v: 'all' | 'entity') => void;
  value: 'all' | 'entity';
  label: string;
}

function ScopeButton({ current, setCurrent, value, label }: ScopeButtonProps) {
  const isActive = current === value;
  return (
    <button
      onClick={() => setCurrent(value)}
      className={cn(
        'tw-px-2 tw-py-0.5 tw-rounded tw-text-xs tw-transition-colors',
        isActive ? 'tw-bg-accent tw-text-bg' : 'tw-text-muted hover:tw-text-text',
      )}
      aria-pressed={isActive}
    >
      {label}
    </button>
  );
}

function ActivityRow({ entry }: { entry: ActivityLogEntry }) {
  const statusColor =
    entry.status === 'Success'
      ? 'tw-text-healthy'
      : entry.status === 'Failed'
        ? 'tw-text-failed'
        : 'tw-text-degraded';
  return (
    <div className="tw-px-3 tw-py-2 tw-text-xs tw-space-y-0.5">
      <div className="tw-flex tw-items-center tw-justify-between">
        <span className="tw-font-medium tw-text-text">{entry.action}</span>
        <span className={cn('tw-text-[10px]', statusColor)}>{entry.status}</span>
      </div>
      <div className="tw-text-[10px] tw-text-muted">
        {new Date(entry.timestamp).toLocaleString()} · {entry.target}
      </div>
      {entry.message && <div className="tw-text-[10px] tw-text-muted tw-italic">{entry.message}</div>}
    </div>
  );
}

// -------------------- KV Tab --------------------

interface KvTabProps {
  entity: SelectedEntity;
}

type KvOperationMode = 'scan' | 'get' | 'put' | 'delete';

function KvTab({ entity }: KvTabProps) {
  const { success, error } = useToast();
  const { log } = useActivity();

  // Get storeId and groupId from entity
  const storeId = entity.parentIds?.store_id as string || '';
  const groupId = entity.id;

  // State
  const [mode, setMode] = useState<KvOperationMode>('scan');
  const [loading, setLoading] = useState(false);
  const [scanPrefix, setScanPrefix] = useState('');
  const [scanResults, setScanResults] = useState<KvScanItem[]>([]);
  const [scanTruncated, setScanTruncated] = useState(false);
  const [getKey, setGetKey] = useState('');
  const [getValue, setGetValue] = useState<KvGetResponse | null>(null);
  const [putKey, setPutKey] = useState('');
  const [putValue, setPutValue] = useState('');
  const [deleteKey, setDeleteKey] = useState('');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  // Clear error when switching modes
  const switchMode = useCallback((newMode: KvOperationMode) => {
    setMode(newMode);
    setErrorMessage(null);
    setGetValue(null);
  }, []);

  // Handlers
  const handleScan = useCallback(async () => {
    setLoading(true);
    setErrorMessage(null);
    log({ action: 'KV Scan', target: `${storeId}/${groupId}`, status: 'Pending', message: `prefix: "${scanPrefix}"` });

    try {
      const result = await kvScan(storeId, groupId, scanPrefix);
      setScanResults(result.items);
      setScanTruncated(result.truncated);
      log({ action: 'KV Scan', target: `${storeId}/${groupId}`, status: 'Success', message: `Found ${result.items.length} keys`, timestamp: Date.now() });
      success(`Scanned ${result.items.length} keys`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Scan failed';
      setErrorMessage(msg);
      log({ action: 'KV Scan', target: `${storeId}/${groupId}`, status: 'Failed', message: msg, timestamp: Date.now() });
      error(msg);
    } finally {
      setLoading(false);
    }
  }, [storeId, groupId, scanPrefix, log, success, error]);

  const handleGet = useCallback(async () => {
    if (!getKey) return;
    setLoading(true);
    setErrorMessage(null);
    log({ action: 'KV Get', target: `${storeId}/${groupId}`, status: 'Pending', message: `key: "${getKey}"` });

    try {
      const result = await kvGet(storeId, groupId, getKey);
      setGetValue(result);
      log({ action: 'KV Get', target: `${storeId}/${groupId}`, status: 'Success', message: `key: "${getKey}"`, timestamp: Date.now() });
      success(result.found ? `Retrieved value for "${getKey}"` : `Key "${getKey}" not found`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Get failed';
      setErrorMessage(msg);
      log({ action: 'KV Get', target: `${storeId}/${groupId}`, status: 'Failed', message: msg, timestamp: Date.now() });
      error(msg);
    } finally {
      setLoading(false);
    }
  }, [storeId, groupId, getKey, log, success, error]);

  const handlePut = useCallback(async () => {
    if (!putKey || !putValue) return;
    setLoading(true);
    setErrorMessage(null);
    log({ action: 'KV Put', target: `${storeId}/${groupId}`, status: 'Pending', message: `key: "${putKey}"` });

    try {
      await kvPut(storeId, groupId, { key: putKey, value: putValue });
      log({ action: 'KV Put', target: `${storeId}/${groupId}`, status: 'Success', message: `key: "${putKey}"`, timestamp: Date.now() });
      success(`Set value for "${putKey}"`);
      setPutKey('');
      setPutValue('');
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Put failed';
      setErrorMessage(msg);
      log({ action: 'KV Put', target: `${storeId}/${groupId}`, status: 'Failed', message: msg, timestamp: Date.now() });
      error(msg);
    } finally {
      setLoading(false);
    }
  }, [storeId, groupId, putKey, putValue, log, success, error]);

  const handleDelete = useCallback(async () => {
    if (!deleteKey) return;
    setLoading(true);
    setErrorMessage(null);
    log({ action: 'KV Delete', target: `${storeId}/${groupId}`, status: 'Pending', message: `key: "${deleteKey}"` });

    try {
      await kvDelete(storeId, groupId, { key: deleteKey });
      log({ action: 'KV Delete', target: `${storeId}/${groupId}`, status: 'Success', message: `key: "${deleteKey}"`, timestamp: Date.now() });
      success(`Deleted "${deleteKey}"`);
      setDeleteKey('');
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Delete failed';
      setErrorMessage(msg);
      log({ action: 'KV Delete', target: `${storeId}/${groupId}`, status: 'Failed', message: msg, timestamp: Date.now() });
      error(msg);
    } finally {
      setLoading(false);
    }
  }, [storeId, groupId, deleteKey, log, success, error]);

  const copyToClipboard = useCallback((text: string, label: string) => {
    navigator.clipboard.writeText(text).then(() => {
      success(`Copied ${label} to clipboard`);
    }).catch(() => {
      error('Failed to copy to clipboard');
    });
  }, [success, error]);

  const exportOptions = [
    {
      id: 'json',
      label: 'Export as JSON',
      onSelect: () => exportAsJSON(scanResults, `kv-${storeId}-${groupId}.json`),
    },
    {
      id: 'csv',
      label: 'Export as CSV',
      onSelect: () => exportAsCSV(scanResults, [{ key: 'key_utf8', label: 'Key' }, { key: 'value_utf8', label: 'Value' }], `kv-${storeId}-${groupId}.csv`),
    },
  ];

  return (
    <div className="tw-p-3 tw-space-y-3">
      {/* Mode selector */}
      <div className="tw-flex tw-gap-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-p-0.5">
        {[
          { id: 'scan' as const, label: 'Scan', icon: <Search className="tw-h-3 tw-w-3" /> },
          { id: 'get' as const, label: 'Get', icon: <Info className="tw-h-3 tw-w-3" /> },
          { id: 'put' as const, label: 'Put', icon: <Database className="tw-h-3 tw-w-3" /> },
          { id: 'delete' as const, label: 'Delete', icon: <Trash2 className="tw-h-3 tw-w-3" /> },
        ].map((m) => (
          <button
            key={m.id}
            onClick={() => switchMode(m.id)}
            className={cn(
              'tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-rounded tw-text-xs tw-transition-colors',
              mode === m.id ? 'tw-bg-accent tw-text-bg' : 'tw-text-muted hover:tw-text-text'
            )}
          >
            {m.icon}
            {m.label}
          </button>
        ))}
      </div>

      {/* Error display */}
      {errorMessage && (
        <div className="tw-flex tw-items-start tw-gap-2 tw-p-2 tw-rounded tw-bg-failed/10 tw-border tw-border-failed/30 tw-text-failed tw-text-xs">
          <AlertTriangle className="tw-h-4 tw-w-4 tw-flex-shrink-0" />
          <span>{errorMessage}</span>
        </div>
      )}

      {/* Scan mode */}
      {mode === 'scan' && (
        <div className="tw-space-y-3">
          <div className="tw-flex tw-gap-2">
            <input
              type="text"
              value={scanPrefix}
              onChange={(e) => setScanPrefix(e.target.value)}
              placeholder="Key prefix (leave empty for all keys)"
              className="tw-flex-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted"
              onKeyDown={(e) => e.key === 'Enter' && handleScan()}
            />
            <button
              onClick={handleScan}
              disabled={loading}
              className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs tw-transition-opacity disabled:tw-opacity-50 disabled:tw-cursor-not-allowed"
            >
              {loading && <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" />}
              Scan
            </button>
          </div>

          {scanResults.length > 0 && (
            <div className="tw-space-y-2">
              <div className="tw-flex tw-items-center tw-justify-between">
                <span className="tw-text-xs tw-text-muted">
                  {scanResults.length} keys{scanTruncated && ' (truncated)'}
                </span>
                <ExportDropdown options={exportOptions} />
              </div>
              <div className="tw-border tw-border-border tw-rounded tw-overflow-hidden">
                <div className="tw-max-h-64 tw-overflow-y-auto">
                  <table className="tw-w-full tw-text-xs">
                    <thead className="tw-bg-bg tw-sticky tw-top-0">
                      <tr>
                        <th className="tw-text-left tw-p-2 tw-font-medium tw-text-muted tw-border-b tw-border-border">Key</th>
                        <th className="tw-text-left tw-p-2 tw-font-medium tw-text-muted tw-border-b tw-border-border">Value</th>
                        <th className="tw-w-8 tw-border-b tw-border-border"></th>
                      </tr>
                    </thead>
                    <tbody className="tw-divide-y tw-divide-border">
                      {scanResults.map((item, idx) => (
                        <tr key={idx} className="hover:tw-bg-bg/50">
                          <td className="tw-p-2 tw-font-mono tw-truncate tw-max-w-[150px]" title={item.key_utf8}>
                            {item.key_utf8}
                          </td>
                          <td className="tw-p-2 tw-font-mono tw-truncate tw-max-w-[150px]" title={item.value_utf8}>
                            {item.value_utf8}
                          </td>
                          <td className="tw-p-2">
                            <button
                              onClick={() => copyToClipboard(item.value_utf8, 'value')}
                              className="tw-text-muted hover:tw-text-text tw-transition-colors"
                              title="Copy value"
                            >
                              <Copy className="tw-h-3 tw-w-3" />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Get mode */}
      {mode === 'get' && (
        <div className="tw-space-y-3">
          <div className="tw-flex tw-gap-2">
            <input
              type="text"
              value={getKey}
              onChange={(e) => setGetKey(e.target.value)}
              placeholder="Key to get"
              className="tw-flex-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted"
              onKeyDown={(e) => e.key === 'Enter' && handleGet()}
            />
            <button
              onClick={handleGet}
              disabled={loading || !getKey}
              className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs tw-transition-opacity disabled:tw-opacity-50 disabled:tw-cursor-not-allowed"
            >
              {loading && <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" />}
              Get
            </button>
          </div>

          {getValue && (
            <div className="tw-space-y-2">
              <div className="tw-flex tw-items-center tw-justify-between">
                <span className="tw-text-xs tw-text-muted">
                  {getValue.found ? 'Key found' : 'Key not found'}
                </span>
                {getValue.found && (
                  <button
                    onClick={() => copyToClipboard(getValue.value_utf8 || '', 'value')}
                    className="tw-flex tw-items-center tw-gap-1 tw-text-xs tw-text-accent hover:tw-underline"
                  >
                    <Copy className="tw-h-3 tw-w-3" />
                    Copy
                  </button>
                )}
              </div>
              {getValue.found && (
                <div className="tw-border tw-border-border tw-rounded tw-p-2 tw-bg-bg">
                  <div className="tw-text-xs tw-text-muted tw-mb-1">Value:</div>
                  <div className="tw-font-mono tw-text-xs tw-text-text tw-break-all">
                    {getValue.value_utf8}
                  </div>
                  {getValue.revision !== undefined && (
                    <div className="tw-text-[10px] tw-text-muted tw-mt-1">
                      Revision: {getValue.revision}
                    </div>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Put mode */}
      {mode === 'put' && (
        <div className="tw-space-y-3">
          <div className="tw-space-y-2">
            <input
              type="text"
              value={putKey}
              onChange={(e) => setPutKey(e.target.value)}
              placeholder="Key"
              className="tw-w-full tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted"
            />
            <textarea
              value={putValue}
              onChange={(e) => setPutValue(e.target.value)}
              placeholder="Value"
              rows={4}
              className="tw-w-full tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted tw-resize-none"
            />
          </div>
          <button
            onClick={handlePut}
            disabled={loading || !putKey || !putValue}
            className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-bg-accent tw-text-bg tw-rounded tw-text-xs tw-transition-opacity disabled:tw-opacity-50 disabled:tw-cursor-not-allowed"
          >
            {loading && <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" />}
            Put
          </button>
        </div>
      )}

      {/* Delete mode */}
      {mode === 'delete' && (
        <div className="tw-space-y-3">
          <div className="tw-flex tw-gap-2">
            <input
              type="text"
              value={deleteKey}
              onChange={(e) => setDeleteKey(e.target.value)}
              placeholder="Key to delete"
              className="tw-flex-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-px-2 tw-py-1 tw-text-xs tw-text-text placeholder:tw-text-muted"
              onKeyDown={(e) => e.key === 'Enter' && handleDelete()}
            />
            <button
              onClick={handleDelete}
              disabled={loading || !deleteKey}
              className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-bg-failed tw-text-bg tw-rounded tw-text-xs tw-transition-opacity disabled:tw-opacity-50 disabled:tw-cursor-not-allowed"
            >
              {loading && <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" />}
              Delete
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// Re-export so callers can pass typed view modes if needed.
export type { ViewMode };
