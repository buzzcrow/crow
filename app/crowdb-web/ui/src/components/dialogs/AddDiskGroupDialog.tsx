// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useMemo, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addDiskGroup, listNodeDiskGroups } from '../../api';
import { minUnusedId } from './defaults';

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
  const [dgId, setDgId] = useState('');
  const [name, setName] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const userEditedIdRef = useRef(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  // Fetch fresh DG list when the dialog opens, then compute the next
  // available ID. This avoids reusing an existing active DG id even if
  // the polled nodeDiskGroups state is stale. Uses a ref to track user
  // edits so the async fetch doesn't clobber a user-typed value.
  useEffect(() => {
    if (!isOpen || wasOpenRef.current) {
      wasOpenRef.current = isOpen;
      return;
    }
    wasOpenRef.current = isOpen;
    setName('');
    userEditedIdRef.current = false;
    setDgId('');
    listNodeDiskGroups(nodeId)
      .then((dgs) => {
        const ids = dgs.map((dg) => dg.id);
        if (!userEditedIdRef.current) setDgId(minUnusedId([...existingDgIds, ...ids], 1));
      })
      .catch(() => {
        if (!userEditedIdRef.current) setDgId(minUnusedId(existingDgIds, 1));
      });
  }, [isOpen, nodeId, existingDgIds]);

  const defaultDgId = useMemo(() => minUnusedId(existingDgIds, 1), [existingDgIds]);

  // Fallback: if the fetch hasn't completed yet, use the polled default.
  useEffect(() => {
    if (isOpen && !dgId && !userEditedIdRef.current) {
      setDgId(defaultDgId);
    }
  }, [isOpen, dgId, defaultDgId]);

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
          label="Disk Group ID (auto-assigned)"
          inputMode="numeric"
          value={dgId}
          onChange={(e) => { setDgId(e.target.value); userEditedIdRef.current = true; }}
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
