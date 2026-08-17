// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useMemo, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addDiskGroup } from '../../api';
import { nextNumericId } from './defaults';

export interface AddDiskGroupDialogProps {
  isOpen: boolean;
  onClose: () => void;
  nodeId: number;
  existingDgIds: number[];
  onSuccess?: () => void | Promise<void>;
}

export function AddDiskGroupDialog({
  isOpen,
  onClose,
  nodeId,
  existingDgIds,
  onSuccess,
}: AddDiskGroupDialogProps) {
  const defaultDgId = useMemo(() => nextNumericId(existingDgIds, 1), [existingDgIds]);
  const [dgId, setDgId] = useState(defaultDgId);
  const [name, setName] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setDgId(defaultDgId);
      setName('');
    }
    wasOpenRef.current = isOpen;
  }, [isOpen, defaultDgId]);

  const isNumeric = (v: string) => /^\d+$/.test(v.trim());
  const valid = isNumeric(dgId) && Number(dgId) > 0;

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      await addDiskGroup(nodeId, {
        id: Number(dgId.trim()),
        name: name.trim() || undefined,
      });
      success(`Disk-group ${dgId} created on node ${nodeId}`);
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create disk-group';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="Add Disk Group"
      description={`Create a new disk-group on node ${nodeId}. Disk-groups are the allocation units managed by DiskDB.`}
      confirmLabel="Create Disk Group"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        <Input
          label="Disk Group ID (numeric)"
          placeholder="1"
          inputMode="numeric"
          value={dgId}
          onChange={(e) => setDgId(e.target.value)}
          autoFocus
        />
        <Input
          label="Name (optional)"
          placeholder="ssd-group-1"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
      </div>
    </Dialog>
  );
}
