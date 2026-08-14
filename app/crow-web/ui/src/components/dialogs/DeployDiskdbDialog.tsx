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
  onSuccess?: () => void | Promise<void>;
}

export function DeployDiskdbDialog({
  isOpen,
  onClose,
  nodes,
  onSuccess,
}: DeployDiskdbDialogProps) {
  const [nodeId, setNodeId] = useState('');
  const [mgmtPort, setMgmtPort] = useState('29910');
  const [grpcPort, setGrpcPort] = useState('29920');
  const [binary, setBinary] = useState('');
  const [listenAddr, setListenAddr] = useState('');
  const [httpAddr, setHttpAddr] = useState('');
  const [config, setConfig] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setNodeId(nodes.length > 0 ? String(nodes[0].id) : '');
      setMgmtPort('29910');
      setGrpcPort('29920');
      setBinary('');
      setListenAddr('');
      setHttpAddr('');
      setConfig('');
    }
    wasOpenRef.current = isOpen;
  }, [isOpen, nodes]);

  const isPort = (v: string) => /^\d+$/.test(v) && Number(v) > 0 && Number(v) < 65536;
  const valid = nodeId !== '' && isPort(mgmtPort) && isPort(grpcPort) && mgmtPort !== grpcPort;

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      await deployDiskdb(Number(nodeId), {
        mgmt_port: Number(mgmtPort),
        grpc_port: Number(grpcPort),
        ...(binary.trim() ? { binary: binary.trim() } : {}),
        ...(listenAddr.trim() ? { listen_addr: listenAddr.trim() } : {}),
        ...(httpAddr.trim() ? { http_addr: httpAddr.trim() } : {}),
        ...(config.trim() ? { config: config.trim() } : {}),
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
      description="Spawn a DiskDB instance on a node. Required before disk capacity can be managed."
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
          label="Management Port"
          inputMode="numeric"
          value={mgmtPort}
          onChange={(e) => setMgmtPort(e.target.value)}
        />
        <Input
          label="gRPC Port"
          inputMode="numeric"
          value={grpcPort}
          onChange={(e) => setGrpcPort(e.target.value)}
        />
        <Input
          label="Binary Path (optional)"
          placeholder="leave empty to use $CROW_DISKDB_BIN"
          value={binary}
          onChange={(e) => setBinary(e.target.value)}
        />
        <Input
          label="Listen Address (optional)"
          placeholder="0.0.0.0"
          value={listenAddr}
          onChange={(e) => setListenAddr(e.target.value)}
        />
        <Input
          label="HTTP Address (optional)"
          placeholder="0.0.0.0:29930"
          value={httpAddr}
          onChange={(e) => setHttpAddr(e.target.value)}
        />
        <Input
          label="Config Path (optional)"
          placeholder="path to diskdb config file"
          value={config}
          onChange={(e) => setConfig(e.target.value)}
        />
      </div>
    </Dialog>
  );
}
