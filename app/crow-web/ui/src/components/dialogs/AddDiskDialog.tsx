// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input, Select } from '../ui/Input';
import { Button } from '../ui/Button';
import { useToast } from '../../contexts/ToastContext';
import { addDisksBatch } from '../../api';
import type { AddDiskRequest } from '../../api';

export interface AddDiskDialogProps {
  isOpen: boolean;
  onClose: () => void;
  nodeId: number;
  dgId: number;
  onSuccess?: () => void | Promise<void>;
}

// R77 defaults: 4 TiB capacity / 32 GiB zone / 1 MiB unit (locked).
const DEFAULT_CAPACITY_GIB = 4 * 1024;
const DEFAULT_ZONE_SIZE_MIB = 32 * 1024;
const DEFAULT_UNIT_SIZE_BYTES = 1024 * 1024;

function randomDiskId(): string {
  // crow-protocol DiskId format: 16 hex chars + dash + 16 hex chars.
  const hex16 = () => Array.from({ length: 16 }, () => '0123456789abcdef'[Math.floor(Math.random() * 16)]).join('');
  return `${hex16()}-${hex16()}`;
}

interface DiskRow {
  disk_id: string;
  disk_type: string;
}

export function AddDiskDialog({
  isOpen,
  onClose,
  nodeId,
  dgId,
  onSuccess,
}: AddDiskDialogProps) {
  const [rows, setRows] = useState<DiskRow[]>([{ disk_id: randomDiskId(), disk_type: 'Ssd' }]);
  const [capacityGiB, setCapacityGiB] = useState(String(DEFAULT_CAPACITY_GIB));
  const [zoneSizeMiB, setZoneSizeMiB] = useState(String(DEFAULT_ZONE_SIZE_MIB));
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setRows([{ disk_id: randomDiskId(), disk_type: 'Ssd' }]);
      setCapacityGiB(String(DEFAULT_CAPACITY_GIB));
      setZoneSizeMiB(String(DEFAULT_ZONE_SIZE_MIB));
    }
    wasOpenRef.current = isOpen;
  }, [isOpen]);

  const isPositiveInt = (v: string) => /^\d+$/.test(v) && Number(v) > 0;
  const valid = rows.length > 0
    && rows.every((r) => r.disk_id.trim().length > 0)
    && isPositiveInt(capacityGiB)
    && isPositiveInt(zoneSizeMiB);

  const addRow = () => setRows((prev) => [...prev, { disk_id: randomDiskId(), disk_type: 'Ssd' }]);
  const removeRow = (idx: number) => setRows((prev) => prev.filter((_, i) => i !== idx));
  const updateRow = (idx: number, patch: Partial<DiskRow>) =>
    setRows((prev) => prev.map((r, i) => (i === idx ? { ...r, ...patch } : r)));

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      const capacityBytes = Number(capacityGiB) * 1024 * 1024 * 1024;
      const zoneSizeBytes = Number(zoneSizeMiB) * 1024 * 1024;
      const disks: AddDiskRequest[] = rows.map((r) => ({
        disk_id: r.disk_id.trim(),
        disk_type: r.disk_type,
        capacity_bytes: capacityBytes,
        zone_size_bytes: zoneSizeBytes,
        unit_size_bytes: DEFAULT_UNIT_SIZE_BYTES,
      }));
      const result = await addDisksBatch(nodeId, dgId, { disks });
      const sysdataErrs = result.sysdata_errors?.length ?? 0;
      const msg = sysdataErrs > 0
        ? `Added ${result.added.length} disks (${sysdataErrs} sysdata sync warnings)`
        : `Added ${result.added.length} disks to DG-${dgId}`;
      success(msg);
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to add disks';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="Add Disks"
      description={`Add disks to disk-group ${dgId} on node ${nodeId}. Unit size is fixed at 1 MiB.`}
      confirmLabel="Add Disks"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-3">
        <div className="tw-flex tw-gap-3">
          <div className="tw-flex-1">
            <Input
              label="Disk Size (GiB)"
              inputMode="numeric"
              value={capacityGiB}
              onChange={(e) => setCapacityGiB(e.target.value)}
            />
          </div>
          <div className="tw-flex-1">
            <Input
              label="Zone Size (MiB)"
              inputMode="numeric"
              value={zoneSizeMiB}
              onChange={(e) => setZoneSizeMiB(e.target.value)}
            />
          </div>
        </div>
        {rows.map((row, idx) => (
          <div key={idx} className="tw-flex tw-items-start tw-gap-2">
            <div className="tw-flex-1">
              <Input
                label={idx === 0 ? 'Disk ID (UUID)' : undefined}
                placeholder="0123456789abcdef-0123456789abcdef"
                value={row.disk_id}
                onChange={(e) => updateRow(idx, { disk_id: e.target.value })}
              />
            </div>
            <div className="tw-w-28">
              <Select
                label={idx === 0 ? 'Type' : undefined}
                value={row.disk_type}
                onChange={(e) => updateRow(idx, { disk_type: e.target.value })}
              >
                <option value="Ssd">Ssd</option>
                <option value="Hdd">Hdd</option>
              </Select>
            </div>
            {rows.length > 1 && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => removeRow(idx)}
                aria-label="Remove disk row"
                className="tw-mt-6"
              >
                ✕
              </Button>
            )}
          </div>
        ))}
        <Button variant="secondary" size="sm" onClick={addRow}>
          + Add another disk
        </Button>
      </div>
    </Dialog>
  );
}
