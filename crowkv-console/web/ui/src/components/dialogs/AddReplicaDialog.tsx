// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addReplica } from '../../api';
import { Node } from '../../types';

export interface AddReplicaDialogProps {
  isOpen: boolean;
  onClose: () => void;
  storeId: string;
  groupId: string;
  nodes: Node[];
  usedNodeIds?: Set<string>;
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
  usedNodeIds = new Set(),
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
  const availableNodes = nodes.filter((node) => !usedNodeIds.has(node.id));
  const hasAvailableNodes = availableNodes.length > 0;
  const resolvedDefaultNodeId = defaultNodeId && availableNodes.some((node) => node.id === defaultNodeId)
    ? defaultNodeId
    : (availableNodes[0]?.id || '');

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
        <div className="tw-space-y-2">
          <label className="tw-text-xs tw-font-medium tw-text-text">
            Node
          </label>
          {nodes.length === 0 ? (
            <div className="tw-text-sm tw-text-muted">
              No nodes available.
            </div>
          ) : (
            <div className="tw-max-h-40 tw-overflow-y-auto tw-border tw-border-border tw-rounded-md tw-p-2 tw-space-y-1">
              {availableNodes.map((node) => (
                <label
                  key={node.id}
                  className="tw-flex tw-items-center tw-gap-2 tw-p-2 tw-rounded tw-cursor-pointer hover:tw-bg-bg"
                >
                  <input
                    type="radio"
                    name="replica-node"
                    checked={nodeId === node.id}
                    onChange={() => setNodeId(node.id)}
                    className="tw-h-4 tw-w-4 tw-rounded-full tw-border-border tw-text-accent focus:tw-ring-accent"
                  />
                  <span className="tw-text-sm tw-text-text">
                    {node.id}
                    <span className="tw-text-xs tw-text-muted tw-ml-1">({node.host})</span>
                  </span>
                </label>
              ))}
              {nodes.filter((node) => usedNodeIds.has(node.id)).map((node) => (
                <label
                  key={node.id}
                  className="tw-flex tw-items-center tw-gap-2 tw-p-2 tw-rounded tw-cursor-not-allowed tw-opacity-50"
                >
                  <input
                    type="radio"
                    name="replica-node"
                    disabled
                    className="tw-h-4 tw-w-4 tw-rounded-full tw-border-border tw-text-accent"
                  />
                  <span className="tw-text-sm tw-text-text">
                    {node.id}
                    <span className="tw-text-xs tw-text-muted tw-ml-1">({node.host}) — already has a replica</span>
                  </span>
                </label>
              ))}
            </div>
          )}
          {!hasAvailableNodes && nodes.length > 0 && (
            <div className="tw-text-sm tw-text-muted">
              No available node. Every node already has a replica in this group.
            </div>
          )}
        </div>
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
