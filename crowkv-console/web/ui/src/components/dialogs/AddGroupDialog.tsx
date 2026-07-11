import { useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addGroup } from '../../api';
import { Node } from '../../types';

export interface AddGroupDialogProps {
  isOpen: boolean;
  onClose: () => void;
  storeId: string;
  nodes: Node[];
  onSuccess?: () => void | Promise<void>;
}

/**
 * Create a new replication group inside an existing store. The backend
 * (`POST /api/stores/:sid/groups`) creates one replica per selected
 * node, starting from `replica_id`.
 */
export function AddGroupDialog({ isOpen, onClose, storeId, nodes, onSuccess }: AddGroupDialogProps) {
  const [groupId, setGroupId] = useState('');
  const [replicaId, setReplicaId] = useState('');
  const [selectedNodeIds, setSelectedNodeIds] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();

  const isNumeric = (v: string) => /^\d+$/.test(v.trim());
  const valid = isNumeric(groupId) && isNumeric(replicaId) && selectedNodeIds.length > 0;

  const reset = () => {
    setGroupId('');
    setReplicaId('');
    setSelectedNodeIds([]);
  };

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      await addGroup(storeId, {
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
      description={`Create a new replication group in store ${storeId}.`}
      confirmLabel="Create Group"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        <Input
          label="Group ID (numeric)"
          placeholder="80"
          inputMode="numeric"
          value={groupId}
          onChange={(e) => setGroupId(e.target.value)}
          autoFocus
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
            Nodes (select at least one — one replica is created per node)
          </label>
          {nodes.length === 0 ? (
            <div className="tw-text-sm tw-text-muted">No nodes available.</div>
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
