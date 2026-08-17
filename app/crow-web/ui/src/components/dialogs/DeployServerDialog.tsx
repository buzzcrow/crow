// Copyright 2026-present buzzcrow <buzzcrow@126.com>
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
 * Deploy a `crow-kv-server` instance on a node. Required before that
 * node can host any store / group / replica. Backend contract:
 * `crow-console/web/src/lifecycle.rs::DeployNodeServerBody`.
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
      success(`Crow Storage deployed on ${nodeId}`);
      onClose();
      await onSuccess?.(deployedPorts);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to deploy Crow Storage';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={`Deploy Crow Storage on ${nodeId}`}
      description="Spawn a Crow Storage instance on this node. Required before stores or replicas can be created."
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
