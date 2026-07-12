import { useEffect, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input, Select } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addGroup } from '../../api';
import { Node, StoreView, CrowKVServerView } from '../../types';
import { isCrowKVServerAvailable } from '../../data/crowkvServers';

export interface AddGroupDialogProps {
  isOpen: boolean;
  onClose: () => void;
  storeId?: string;
  stores: StoreView[];
  nodes: Node[];
  servers?: CrowKVServerView[];
  defaultGroupId?: string;
  defaultReplicaId?: string;
  defaultNodeIds?: string[];
  onSuccess?: () => void | Promise<void>;
}

/**
 * Create a new replication group inside an existing store. The backend
 * (`POST /api/stores/:sid/groups`) creates one replica per selected
 * node, starting from `replica_id`.
 */
export function AddGroupDialog({
  isOpen,
  onClose,
  storeId = '',
  stores,
  nodes,
  servers = [],
  defaultGroupId = '',
  defaultReplicaId = '',
  defaultNodeIds = [],
  onSuccess,
}: AddGroupDialogProps) {
  const [selectedStoreId, setSelectedStoreId] = useState(storeId || (stores[0] ? String(stores[0].store_id) : ''));
  const availableNodes = nodes.filter(
    (node) =>
      servers.some((server) => server.node_id === node.id && isCrowKVServerAvailable(server)),
  );
  const availableNodeIds = availableNodes.map((node) => node.id);
  const resolveDefaultNodeIds = (targetStoreId: string, preferExplicit: boolean): string[] => {
    const explicit = defaultNodeIds.filter((id) => availableNodeIds.includes(id));
    if (preferExplicit && explicit.length > 0) {
      return explicit;
    }
    const selectedStore = stores.find((store) => String(store.store_id) === targetStoreId);
    const storeNodeIds = (selectedStore?.nodes || []).map(String).filter((id) => availableNodeIds.includes(id));
    if (storeNodeIds.length > 0) {
      return storeNodeIds;
    }
    return availableNodeIds.slice(0, 3);
  };
  const defaultSelectedNodeIds = resolveDefaultNodeIds(selectedStoreId, true);
  const [groupId, setGroupId] = useState(defaultGroupId);
  const [replicaId, setReplicaId] = useState(defaultReplicaId);
  const [selectedNodeIds, setSelectedNodeIds] = useState<string[]>(defaultSelectedNodeIds);
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  const isNumeric = (v: string) => /^\d+$/.test(v.trim());
  const valid = isNumeric(groupId) && isNumeric(replicaId) && selectedNodeIds.length > 0;

  const reset = () => {
    const nextStoreId = storeId || (stores[0] ? String(stores[0].store_id) : '');
    setSelectedStoreId(nextStoreId);
    setGroupId(defaultGroupId);
    setReplicaId(defaultReplicaId);
    setSelectedNodeIds(resolveDefaultNodeIds(nextStoreId, true));
  };

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) reset();
    wasOpenRef.current = isOpen;
  }, [defaultGroupId, defaultReplicaId, defaultSelectedNodeIds, isOpen, storeId, stores]);

  useEffect(() => {
    if (!isOpen) return;
    setSelectedNodeIds(resolveDefaultNodeIds(selectedStoreId, false));
  }, [isOpen, selectedStoreId]);

  const handleSubmit = async () => {
    if (!valid || !selectedStoreId) return;
    setIsLoading(true);
    try {
      await addGroup(selectedStoreId, {
        group_id: groupId.trim(),
        replica_id: replicaId.trim(),
        nodes: selectedNodeIds,
      });
      success(`Group ${groupId} created successfully`);
      reset();
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create group';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    reset();
    onClose();
  };

  const toggleNode = (nodeId: string) => {
    setSelectedNodeIds((prev) =>
      prev.includes(nodeId) ? prev.filter((id) => id !== nodeId) : [...prev, nodeId],
    );
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleClose}
      title="Add Group"
      description="Create a new replication group in a selected KV store. One local replica will be created on each selected node, then the backend wires the remote replicas between them."
      confirmLabel="Create Group"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || !selectedStoreId || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        {stores.length > 0 ? (
          <Select label="KV Store" value={selectedStoreId} onChange={(e) => setSelectedStoreId(e.target.value)} autoFocus>
            <option value="" disabled>Select a KV store</option>
            {stores.map((store) => {
              const sid = String(store.store_id);
              return (
                <option key={sid} value={sid}>
                  {store.name ? `${store.name} (${sid})` : sid}
                </option>
              );
            })}
          </Select>
        ) : (
          <div className="tw-text-sm tw-text-muted">No KV stores available. Create a KV store first.</div>
        )}
        <Input
          label="Group ID (numeric)"
          placeholder="80"
          inputMode="numeric"
          value={groupId}
          onChange={(e) => setGroupId(e.target.value)}
        />
        <Input
          label="Starting Replica ID (numeric)"
          placeholder="800"
          inputMode="numeric"
          value={replicaId}
          onChange={(e) => setReplicaId(e.target.value)}
        />
        <div className="tw-space-y-2">
          <label className="tw-text-xs tw-font-medium tw-text-text">
            Available CrowKV nodes (select at least one — one local replica is created per node)
          </label>
          {availableNodes.length === 0 ? (
            <div className="tw-text-sm tw-text-muted">No reachable CrowKV nodes are available.</div>
          ) : (
            <div className="tw-max-h-40 tw-overflow-y-auto tw-border tw-border-border tw-rounded-md tw-p-2 tw-space-y-1">
              {availableNodes.map((node) => (
                <label
                  key={node.id}
                  className="tw-flex tw-items-center tw-gap-2 tw-p-2 tw-rounded tw-cursor-pointer hover:tw-bg-bg"
                >
                  <input
                    type="checkbox"
                    checked={selectedNodeIds.includes(node.id)}
                    onChange={() => toggleNode(node.id)}
                    className="tw-h-4 tw-w-4 tw-rounded tw-border-border tw-text-accent focus:tw-ring-accent"
                  />
                  <span className="tw-text-sm tw-text-text">
                    {node.id}
                    <span className="tw-text-xs tw-text-muted tw-ml-1">({node.host})</span>
                  </span>
                </label>
              ))}
            </div>
          )}
        </div>
      </div>
    </Dialog>
  );
}
