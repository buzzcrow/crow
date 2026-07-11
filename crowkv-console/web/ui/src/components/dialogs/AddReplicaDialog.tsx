import { useEffect, useRef, useState } from 'react';
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
  defaultNodeId?: string;
  defaultReplicaId?: string;
  onSuccess?: () => void | Promise<void>;
}

/**
 * Dialog for adding a new replica to a group.
 */
export function AddReplicaDialog({
  isOpen,
  onClose,
  storeId,
  groupId,
  nodes,
  defaultNodeId = '',
  defaultReplicaId = '',
  onSuccess,
}: AddReplicaDialogProps) {
  const [nodeId, setNodeId] = useState(defaultNodeId);
  const [replicaId, setReplicaId] = useState(defaultReplicaId);
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  const replicaIdValid = replicaId.trim() === '' || /^\d+$/.test(replicaId.trim());
  const hasAvailableNodes = nodes.length > 0;
  const resolvedDefaultNodeId = defaultNodeId && nodes.some((node) => node.id === defaultNodeId)
    ? defaultNodeId
    : (nodes[0]?.id || '');

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setNodeId(resolvedDefaultNodeId);
      setReplicaId(defaultReplicaId);
    }
    wasOpenRef.current = isOpen;
  }, [defaultReplicaId, isOpen, resolvedDefaultNodeId]);

  useEffect(() => {
    if (!isOpen) return;
    if (nodeId && nodes.some((node) => node.id === nodeId)) return;
    setNodeId(resolvedDefaultNodeId);
  }, [isOpen, nodeId, nodes, resolvedDefaultNodeId]);

  const handleSubmit = async () => {
    if (!hasAvailableNodes || !nodeId || !replicaIdValid) return;

    setIsLoading(true);
    try {
      await addReplica(storeId, groupId, {
        node_id: nodeId,
        replica_id: replicaId.trim() || undefined,
      });

      success(`Replica added to node "${nodeId}" successfully`);
      setNodeId(resolvedDefaultNodeId);
      setReplicaId(defaultReplicaId);
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
    setNodeId(resolvedDefaultNodeId);
    setReplicaId(defaultReplicaId);
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
      confirmDisabled={!hasAvailableNodes || !nodeId || !replicaIdValid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        {hasAvailableNodes ? (
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
            No available node. Every node already has a replica in this group.
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
