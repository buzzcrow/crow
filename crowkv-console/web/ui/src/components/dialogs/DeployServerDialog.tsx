import { useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { deployServer } from '../../api';

export interface DeployServerDialogProps {
  isOpen: boolean;
  onClose: () => void;
  nodeId: string;
  onSuccess?: () => void | Promise<void>;
}

/**
 * Deploy a `crowkv-server` instance on a node. Required before that
 * node can host any store / group / replica. Backend contract:
 * `crowkv-console/web/src/lifecycle.rs::DeployNodeServerBody`.
 */
export function DeployServerDialog({ isOpen, onClose, nodeId, onSuccess }: DeployServerDialogProps) {
  const [mgmtPort, setMgmtPort] = useState('9910');
  const [grpcPort, setGrpcPort] = useState('9920');
  const [binary, setBinary] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();

  const isPort = (v: string) => /^\d+$/.test(v) && Number(v) > 0 && Number(v) < 65536;
  const valid = isPort(mgmtPort) && isPort(grpcPort) && mgmtPort !== grpcPort;

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      await deployServer(nodeId, {
        mgmt_port: Number(mgmtPort),
        grpc_port: Number(grpcPort),
        ...(binary.trim() ? { binary: binary.trim() } : {}),
      });
      success(`Server deployed on ${nodeId}`);
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to deploy server';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={`Deploy Server on ${nodeId}`}
      description="Spawn a crowkv-server instance on this node. Required before stores or replicas can be created."
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
          placeholder="leave empty to use $CROWKV_SERVER_BIN"
          value={binary}
          onChange={(e) => setBinary(e.target.value)}
        />
      </div>
    </Dialog>
  );
}
