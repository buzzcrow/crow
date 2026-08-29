// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { deployServer } from '../../api';

export interface DeployServerDialogProps {
  isOpen: boolean;
  onClose: () => void;
  nodeId: number;
  defaultRestPort?: string;
  defaultRpcPort?: string;
  onSuccess?: (ports: { restPort: number; rpcPort: number }) => void | Promise<void>;
}

/**
 * Deploy a `crowdb-kv-server` instance on a node. Required before that
 * node can host any store / group / replica. Backend contract:
 * `crowdb-console/web/src/lifecycle.rs::DeployNodeServerBody`.
 */
export function DeployServerDialog({
  isOpen,
  onClose,
  nodeId,
  defaultRestPort = '19910',
  defaultRpcPort = '19920',
  onSuccess,
}: DeployServerDialogProps) {
  const [restPort, setRestPort] = useState(defaultRestPort);
  const [rpcPort, setRpcPort] = useState(defaultRpcPort);
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setRestPort(defaultRestPort);
      setRpcPort(defaultRpcPort);
    }
    wasOpenRef.current = isOpen;
  }, [defaultRpcPort, defaultRestPort, isOpen, nodeId]);

  const isPort = (v: string) => /^\d+$/.test(v) && Number(v) > 0 && Number(v) < 65536;
  const valid = isPort(restPort) && isPort(rpcPort) && restPort !== rpcPort;

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      const deployedPorts = {
        restPort: Number(restPort),
        rpcPort: Number(rpcPort),
      };
      await deployServer(nodeId, {
        rest_port: deployedPorts.restPort,
        rpc_port: deployedPorts.rpcPort,
      });
      success(`CrowDB Storage deployed on ${nodeId}`);
      onClose();
      await onSuccess?.(deployedPorts);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to deploy CrowDB Storage';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={`Deploy CrowDB Storage on ${nodeId}`}
      description="Spawn a CrowDB Storage instance on this node. Required before stores or replicas can be created."
      confirmLabel="Deploy"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        <Input
          label="REST Port"
          inputMode="numeric"
          value={restPort}
          onChange={(e) => setRestPort(e.target.value)}
          autoFocus
        />
        <Input
          label="RPC Port"
          inputMode="numeric"
          value={rpcPort}
          onChange={(e) => setRpcPort(e.target.value)}
        />
      </div>
    </Dialog>
  );
}
