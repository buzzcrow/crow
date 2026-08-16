// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { deployDiskdb } from '../../api';

export interface DeployDiskdbDialogProps {
  isOpen: boolean;
  onClose: () => void;
  nodes: { id: number; label?: string }[];
  defaultNodeId?: number;
  onSuccess?: () => void | Promise<void>;
}

export function DeployDiskdbDialog({
  isOpen,
  onClose,
  nodes,
  defaultNodeId,
  onSuccess,
}: DeployDiskdbDialogProps) {
  const [nodeId, setNodeId] = useState('');
  const [rpcPort, setRpcPort] = useState('29920');
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      const initial = defaultNodeId != null
        ? String(defaultNodeId)
        : nodes.length > 0 ? String(nodes[0].id) : '';
      setNodeId(initial);
      setRpcPort('29920');
    }
    wasOpenRef.current = isOpen;
  }, [isOpen, nodes, defaultNodeId]);

  const isPort = (v: string) => /^\d+$/.test(v) && Number(v) > 0 && Number(v) < 65536;
  const valid = nodeId !== '' && isPort(rpcPort);

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      await deployDiskdb(Number(nodeId), {
        rpc_port: Number(rpcPort),
      });
      success(`DiskDB deployed on node ${nodeId}`);
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to deploy DiskDB';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="Deploy DiskDB"
      description="Spawn a DiskDB instance on a node. The binary and config are pre-copied to the node's bin/ and conf/ folders."
      confirmLabel="Deploy"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        <div className="tw-space-y-1">
          <label className="tw-text-sm tw-font-medium tw-text-text">Node</label>
          <select
            value={nodeId}
            onChange={(e) => setNodeId(e.target.value)}
            className="tw-w-full tw-px-3 tw-py-2 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-text-sm tw-text-text focus:tw-outline-none focus:tw-ring-2 focus:tw-ring-accent"
            autoFocus
          >
            {nodes.length === 0 && <option value="">No nodes available</option>}
            {nodes.map((n) => (
              <option key={n.id} value={String(n.id)}>
                {n.label ?? `Node ${n.id}`}
              </option>
            ))}
          </select>
        </div>
        <Input
          label="RPC Port (gRPC)"
          inputMode="numeric"
          value={rpcPort}
          onChange={(e) => setRpcPort(e.target.value)}
        />
      </div>
    </Dialog>
  );
}
