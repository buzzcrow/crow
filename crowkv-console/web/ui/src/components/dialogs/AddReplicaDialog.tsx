import { useState } from 'react';
import { Dialog } from '../Dialog';
import { Input, Select } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addReplica } from '../../api';
import { Node } from '../../types';

export interface AddReplicaDialogProps {
  isOpen: boolean;
  onClose: () => void;
  storeId: string;
  groupId: string;
  nodes: Node[];
  onSuccess?: () => void | Promise<void>;
}

/**
 * Dialog for adding a new replica to a group.
 */
export function AddReplicaDialog({ isOpen, onClose, storeId, groupId, nodes, onSuccess }: AddReplicaDialogProps) {
  const [nodeId, setNodeId] = useState('');
  const [replicaId, setReplicaId] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();

  const replicaIdValid = replicaId.trim() === '' || /^\d+$/.test(replicaId.trim());

  const handleSubmit = async () => {
    if (!nodeId || !replicaIdValid) return;

    setIsLoading(true);
    try {
      await addReplica(storeId, groupId, {
        node_id: nodeId,
        replica_id: replicaId.trim() || undefined,
      });

      success(`Replica added to node "${nodeId}" successfully`);
      setNodeId('');
      setReplicaId('');
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to add replica';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    setNodeId('');
    setReplicaId('');
    onClose();
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleClose}
      title="Add Replica"
      description={`Add a new replica to group "${groupId}" in store "${storeId}"`}
      confirmLabel="Add Replica"
      onConfirm={handleSubmit}
      confirmDisabled={!nodeId || !replicaIdValid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        {nodes.length > 0 ? (
          <Select
            label="Node"
            value={nodeId}
            onChange={(e) => setNodeId(e.target.value)}
            autoFocus
          >
            <option value="" disabled>Select a node</option>
            {nodes.map((node) => (
              <option key={node.id} value={node.id}>
                {node.id} ({node.host})
              </option>
            ))}
          </Select>
        ) : (
          <div className="tw-text-sm tw-text-muted">
            No nodes available.
          </div>
        )}
        <Input
          label="Replica ID (optional)"
          placeholder="Leave empty for auto-generated"
          value={replicaId}
          onChange={(e) => setReplicaId(e.target.value)}
        />
      </div>
    </Dialog>
  );
}
