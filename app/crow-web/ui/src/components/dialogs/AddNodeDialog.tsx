// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input, Select } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addNode, deployServer } from '../../api';
import { Rack } from '../../types';
import { nextIdFromSuffix } from './defaults';

export interface AddNodeDialogProps {
  isOpen: boolean;
  onClose: () => void;
  racks: Rack[];
  defaultRackId?: string;
  existingNodeIds?: string[];
  defaultHost?: string;
  defaultMgmtPort?: string;
  defaultGrpcPort?: string;
  onCreatedRackId?: (rackId: number) => void;
  onSuccess?: () => void | Promise<void>;
}

/**
 * Dialog for adding a new node.
 */
export function AddNodeDialog({
  isOpen,
  onClose,
  racks,
  defaultRackId,
  existingNodeIds = [],
  defaultHost = '127.0.0.1',
  defaultMgmtPort = '19910',
  defaultGrpcPort = '19920',
  onCreatedRackId,
  onSuccess,
}: AddNodeDialogProps) {
  const initialRackId = defaultRackId || racks[0]?.id || '';
  const initialNodeId = nextIdFromSuffix(existingNodeIds, 1);
  const [rackId, setRackId] = useState(initialRackId);
  const [nodeId, setNodeId] = useState(initialNodeId);
  const [host, setHost] = useState(defaultHost);
  const [sshUser, setSshUser] = useState('');
  const [sshKeyPath, setSshKeyPath] = useState('');
  const [enableCrowKV, setEnableCrowKV] = useState(true);
  const [mgmtPort, setMgmtPort] = useState(defaultMgmtPort);
  const [grpcPort, setGrpcPort] = useState(defaultGrpcPort);
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();

  useEffect(() => {
    if (!isOpen) return;
    setMgmtPort(defaultMgmtPort);
    setGrpcPort(defaultGrpcPort);
    setEnableCrowKV(true);
  }, [defaultGrpcPort, defaultMgmtPort, isOpen]);

  const isPort = (value: string) => /^\d+$/.test(value) && Number(value) > 0 && Number(value) < 65536;
  const deployPortsValid = isPort(mgmtPort) && isPort(grpcPort) && mgmtPort !== grpcPort;

  const handleSubmit = async () => {
    if (!rackId || !nodeId.trim() || !host.trim() || (enableCrowKV && !deployPortsValid)) return;

    setIsLoading(true);
    try {
      const trimmedNodeId = nodeId.trim();
      const numericNodeId = Number(trimmedNodeId);
      await addNode({
        id: numericNodeId,
        rack_id: Number(rackId),
        host: host.trim(),
        ssh_port: 22,
        ssh_user: sshUser.trim(),
        ...(sshKeyPath.trim() ? { ssh_key: sshKeyPath.trim() } : {}),
      });

      if (enableCrowKV) {
        await deployServer(numericNodeId, {
          mgmt_port: Number(mgmtPort),
          grpc_port: Number(grpcPort),
        });
      }

      success(enableCrowKV ? `Node "${trimmedNodeId}" created and Crow Storage enabled` : `Node "${trimmedNodeId}" created successfully`);
      onCreatedRackId?.(Number(rackId));
      setRackId(initialRackId);
      setNodeId(initialNodeId);
      setHost(defaultHost);
      setSshUser('');
      setSshKeyPath('');
      setEnableCrowKV(true);
      setMgmtPort(defaultMgmtPort);
      setGrpcPort(defaultGrpcPort);
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create node';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    setRackId(initialRackId);
    setNodeId(initialNodeId);
    setHost(defaultHost);
    setSshUser('');
    setSshKeyPath('');
    setEnableCrowKV(true);
    setMgmtPort(defaultMgmtPort);
    setGrpcPort(defaultGrpcPort);
    onClose();
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleClose}
      title="Add Node"
      description="Add a new physical node to your infrastructure"
      confirmLabel="Create Node"
      onConfirm={handleSubmit}
      confirmDisabled={!rackId || !nodeId.trim() || !host.trim() || isLoading || (enableCrowKV && !deployPortsValid)}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        {racks.length > 0 ? (
          <Select
            label="Rack"
            value={rackId}
            onChange={(e) => setRackId(e.target.value)}
          >
            <option value="" disabled>Select a rack</option>
            {racks.map((rack) => (
              <option key={rack.id} value={rack.id}>
                {rack.name || rack.id}
              </option>
            ))}
          </Select>
        ) : (
          <div className="tw-text-sm tw-text-muted">
            No racks available. Create a rack first.
          </div>
        )}
        <Input
          label="Node ID"
          placeholder="N-01"
          value={nodeId}
          onChange={(e) => setNodeId(e.target.value)}
          autoFocus
        />
        <Input
          label="Host"
          placeholder="192.168.1.100 or example.com"
          value={host}
          onChange={(e) => setHost(e.target.value)}
        />
        <Input
          label="SSH User (optional)"
          placeholder="root"
          value={sshUser}
          onChange={(e) => setSshUser(e.target.value)}
        />
        <Input
          label="SSH Key Path (optional)"
          placeholder="~/.ssh/id_rsa"
          value={sshKeyPath}
          onChange={(e) => setSshKeyPath(e.target.value)}
        />
        <label className="tw-flex tw-items-center tw-gap-2 tw-text-sm tw-text-text">
          <input
            type="checkbox"
            checked={enableCrowKV}
            onChange={(e) => setEnableCrowKV(e.target.checked)}
            className="tw-h-4 tw-w-4 tw-rounded tw-border tw-border-border tw-bg-bg tw-text-accent focus:tw-ring-accent"
          />
          <span>Enable Crow Storage on this node</span>
        </label>
        {enableCrowKV && (
          <>
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
          </>
        )}
      </div>
    </Dialog>
  );
}
