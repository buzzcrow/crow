import { useState } from 'react';
import { Dialog } from '../Dialog';
import { Input, Select } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addNode } from '../../api';
import { Rack } from '../../types';

export interface AddNodeDialogProps {
  isOpen: boolean;
  onClose: () => void;
  racks: Rack[];
  defaultRackId?: string;
  onSuccess?: () => void | Promise<void>;
}

/**
 * Dialog for adding a new node.
 */
export function AddNodeDialog({ isOpen, onClose, racks, defaultRackId, onSuccess }: AddNodeDialogProps) {
  const [rackId, setRackId] = useState(defaultRackId || '');
  const [nodeId, setNodeId] = useState('');
  const [host, setHost] = useState('');
  const [sshUser, setSshUser] = useState('');
  const [sshKeyPath, setSshKeyPath] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();

  const handleSubmit = async () => {
    if (!rackId || !nodeId.trim() || !host.trim()) return;

    setIsLoading(true);
    try {
      await addNode({
        id: nodeId.trim(),
        rack_id: rackId,
        host: host.trim(),
        ssh_port: 22,
        ssh_user: sshUser.trim(),
        ...(sshKeyPath.trim() ? { ssh_key: sshKeyPath.trim() } : {}),
      });

      success(`Node "${nodeId}" created successfully`);
      setRackId(defaultRackId || '');
      setNodeId('');
      setHost('');
      setSshUser('');
      setSshKeyPath('');
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
    setRackId(defaultRackId || '');
    setNodeId('');
    setHost('');
    setSshUser('');
    setSshKeyPath('');
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
      confirmDisabled={!rackId || !nodeId.trim() || !host.trim() || isLoading}
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
          placeholder="node-01"
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
      </div>
    </Dialog>
  );
}
