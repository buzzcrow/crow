import { useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addStore } from '../../api';
import { Node } from '../../types';

export interface AddStoreDialogProps {
  isOpen: boolean;
  onClose: () => void;
  nodes: Node[];
  onSuccess?: () => void | Promise<void>;
}

/**
 * Create a new cluster-wide store. The backend (`POST /api/stores`)
 * orchestrates creation of the initial group + first replica on the
 * picked nodes, so the dialog collects `store_id`, the initial
 * `group_id`, the first `replica_id` and the set of target nodes.
 *
 * Backend contract: `crowkv-console/web/src/mgmt.rs::CreateStoreBody`.
 */
export function AddStoreDialog({ isOpen, onClose, nodes, onSuccess }: AddStoreDialogProps) {
  const [storeId, setStoreId] = useState('');
  const [groupId, setGroupId] = useState('');
  const [replicaId, setReplicaId] = useState('');
  const [selectedNodeIds, setSelectedNodeIds] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();

  const isNumeric = (v: string) => /^\d+$/.test(v.trim());
  const valid =
    isNumeric(storeId) &&
    isNumeric(groupId) &&
    isNumeric(replicaId) &&
    selectedNodeIds.length > 0;

  const reset = () => {
    setStoreId('');
    setGroupId('');
    setReplicaId('');
    setSelectedNodeIds([]);
  };

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      await addStore({
        store_id: storeId.trim(),
        group_id: groupId.trim(),
        replica_id: replicaId.trim(),
        nodes: selectedNodeIds,
      });
      success(`Store ${storeId} created successfully`);
      reset();
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create store';
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
      title="Add Store"
      description="Create a new distributed store; the initial group and first replica are deployed on the selected nodes."
      confirmLabel="Create Store"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        <Input
          label="Store ID (numeric)"
          placeholder="7"
          inputMode="numeric"
          value={storeId}
          onChange={(e) => setStoreId(e.target.value)}
          autoFocus
        />
        <Input
          label="Initial Group ID (numeric)"
          placeholder="70"
          inputMode="numeric"
          value={groupId}
          onChange={(e) => setGroupId(e.target.value)}
        />
        <Input
          label="First Replica ID (numeric)"
          placeholder="700"
          inputMode="numeric"
          value={replicaId}
          onChange={(e) => setReplicaId(e.target.value)}
        />
        <div className="tw-space-y-2">
          <label className="tw-text-xs tw-font-medium tw-text-text">
            Nodes (select at least one — each must already have a deployed crowkv-server)
          </label>
          {nodes.length === 0 ? (
            <div className="tw-text-sm tw-text-muted">
              No nodes available. Create a rack + node and deploy a server first.
            </div>
          ) : (
            <div className="tw-max-h-40 tw-overflow-y-auto tw-border tw-border-border tw-rounded-md tw-p-2 tw-space-y-1">
              {nodes.map((node) => (
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
