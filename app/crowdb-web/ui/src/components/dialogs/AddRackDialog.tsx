// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo, useRef, useState, useEffect } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addRack } from '../../api';
import { nextIdFromSuffix } from './defaults';

export interface AddRackDialogProps {
  isOpen: boolean;
  onClose: () => void;
  existingRackIds?: string[];
  onSuccess?: () => void | Promise<void>;
}

/**
 * Dialog for adding a new rack.
 */
export function AddRackDialog({ isOpen, onClose, existingRackIds = [], onSuccess }: AddRackDialogProps) {
  const defaultRackId = useMemo(() => nextIdFromSuffix(existingRackIds, 1), [existingRackIds]);
  const [rackId, setRackId] = useState(defaultRackId);
  const [name, setName] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setRackId(defaultRackId);
      setName('');
    }
    wasOpenRef.current = isOpen;
  }, [isOpen, defaultRackId]);

  const handleSubmit = async () => {
    if (!rackId.trim()) return;

    setIsLoading(true);
    try {
      await addRack({
        id: Number(rackId.trim()),
        name: name.trim() || undefined,
      });

      success(`Rack "${rackId}" created successfully`);
      setRackId(defaultRackId);
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
    setRackId(defaultRackId);
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
          placeholder="R-01"
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
