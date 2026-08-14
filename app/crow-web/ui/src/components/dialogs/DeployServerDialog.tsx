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
  defaultMgmtPort?: string;
  defaultGrpcPort?: string;
  onSuccess?: (ports: { mgmtPort: number; grpcPort: number }) => void | Promise<void>;
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
  defaultMgmtPort = '19910',
  defaultGrpcPort = '19920',
  onSuccess,
}: DeployServerDialogProps) {
  const [mgmtPort, setMgmtPort] = useState(defaultMgmtPort);
  const [grpcPort, setGrpcPort] = useState(defaultGrpcPort);
  const [binary, setBinary] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setMgmtPort(defaultMgmtPort);
      setGrpcPort(defaultGrpcPort);
      setBinary('');
    }
    wasOpenRef.current = isOpen;
  }, [defaultGrpcPort, defaultMgmtPort, isOpen, nodeId]);

  const isPort = (v: string) => /^\d+$/.test(v) && Number(v) > 0 && Number(v) < 65536;
  const valid = isPort(mgmtPort) && isPort(grpcPort) && mgmtPort !== grpcPort;

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      const deployedPorts = {
        mgmtPort: Number(mgmtPort),
        grpcPort: Number(grpcPort),
      };
      await deployServer(nodeId, {
        mgmt_port: deployedPorts.mgmtPort,
        grpc_port: deployedPorts.grpcPort,
        ...(binary.trim() ? { binary: binary.trim() } : {}),
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
          label="Management Port"
          inputMode="numeric"
          value={mgmtPort}
          onChange={(e) => setMgmtPort(e.target.value)}
          autoFocus
        />
        <Input
          label="gRPC Port"
          inputMode="numeric"
          value={grpcPort}
          onChange={(e) => setGrpcPort(e.target.value)}
        />
        <Input
          label="Binary Path (optional)"
          placeholder="leave empty to use $CROW_KV_SERVER_BIN"
          value={binary}
          onChange={(e) => setBinary(e.target.value)}
        />
      </div>
    </Dialog>
  );
}
