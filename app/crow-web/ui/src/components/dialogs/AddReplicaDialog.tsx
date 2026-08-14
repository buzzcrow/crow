// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

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
  usedNodeIds?: Set<number>;
  defaultNodeId?: number;
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
  usedNodeIds = new Set(),
  defaultNodeId = 0,
  defaultReplicaId = '',
  onSuccess,
}: AddReplicaDialogProps) {
  const [nodeId, setNodeId] = useState<number>(defaultNodeId);
  const [replicaId, setReplicaId] = useState(defaultReplicaId);
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  const replicaIdValid = replicaId.trim() === '' || /^\d+$/.test(replicaId.trim());
  const availableNodes = nodes.filter((node) => !usedNodeIds.has(node.id));
  const hasAvailableNodes = availableNodes.length > 0;
  const resolvedDefaultNodeId = defaultNodeId && availableNodes.some((node) => node.id === defaultNodeId)
    ? defaultNodeId
    : (availableNodes[0]?.id || 0);

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setNodeId(resolvedDefaultNodeId);
      setReplicaId(defaultReplicaId);
    }
    wasOpenRef.current = isOpen;
  }, [defaultReplicaId, isOpen, resolvedDefaultNodeId]);

  useEffect(() => {
    if (!isOpen) return;
    if (nodeId && availableNodes.some((node) => node.id === nodeId)) return;
    setNodeId(resolvedDefaultNodeId);
  }, [isOpen, nodeId, availableNodes, resolvedDefaultNodeId]);

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
        {nodes.length === 0 ? (
          <div className="tw-text-sm tw-text-muted">
            No nodes available.
          </div>
        ) : (
          <Select
            label="Node"
            value={String(nodeId)}
            onChange={(e) => setNodeId(Number(e.target.value))}
            disabled={!hasAvailableNodes}
          >
            {availableNodes.length === 0 ? (
              <option value="" disabled>No available node</option>
            ) : (
              availableNodes.map((node) => (
                <option key={node.id} value={String(node.id)}>
                  {node.id} ({node.host})
                </option>
              ))
            )}
          </Select>
        )}
        {!hasAvailableNodes && nodes.length > 0 && (
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
