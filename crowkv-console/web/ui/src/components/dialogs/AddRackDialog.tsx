import { useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addRack } from '../../api';

export interface AddRackDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess?: () => void | Promise<void>;
}

/**
 * Dialog for adding a new rack.
 */
export function AddRackDialog({ isOpen, onClose, onSuccess }: AddRackDialogProps) {
  const [rackId, setRackId] = useState('');
  const [name, setName] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();

  const handleSubmit = async () => {
    if (!rackId.trim()) return;

    setIsLoading(true);
    try {
      await addRack({
        id: rackId.trim(),
        name: name.trim() || undefined,
      });

      success(`Rack "${rackId}" created successfully`);
      setRackId('');
      setName('');
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create rack';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    setRackId('');
    setName('');
    onClose();
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleClose}
      title="Add Rack"
      description="Create a new physical rack in your infrastructure"
      confirmLabel="Create Rack"
      onConfirm={handleSubmit}
      confirmDisabled={!rackId.trim() || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        <Input
          label="Rack ID"
          placeholder="rack-01"
          value={rackId}
          onChange={(e) => setRackId(e.target.value)}
          autoFocus
        />
        <Input
          label="Name (optional)"
          placeholder="Main Rack"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
      </div>
    </Dialog>
  );
}
