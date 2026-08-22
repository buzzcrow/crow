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
const DEFAULT_CAPACITY_TIB = 4;
const DEFAULT_ZONE_SIZE_GIB = 32;
const DEFAULT_UNIT_SIZE_BYTES = 1024 * 1024;

function randomDiskId(): string {
  // crow-protocol DiskId format: 16 hex chars + dash + 16 hex chars.
  const hex16 = () => Array.from({ length: 16 }, () => '0123456789abcdef'[Math.floor(Math.random() * 16)]).join('');
  return `${hex16()}-${hex16()}`;
}

interface DiskRow {
  disk_id: string;
  disk_type: string;
  device_path: string;
}

export function AddDiskDialog({
  isOpen,
  onClose,
  nodeId,
  dgId,
  onSuccess,
}: AddDiskDialogProps) {
  const [rows, setRows] = useState<DiskRow[]>([{ disk_id: randomDiskId(), disk_type: 'Ssd', device_path: '' }]);
  const [capacityTiB, setCapacityTiB] = useState(String(DEFAULT_CAPACITY_TIB));
  const [zoneSizeGiB, setZoneSizeGiB] = useState(String(DEFAULT_ZONE_SIZE_GIB));
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setRows([{ disk_id: randomDiskId(), disk_type: 'Ssd', device_path: '' }]);
      setCapacityTiB(String(DEFAULT_CAPACITY_TIB));
      setZoneSizeGiB(String(DEFAULT_ZONE_SIZE_GIB));
    }
    wasOpenRef.current = isOpen;
  }, [isOpen]);

  const isPositiveInt = (v: string) => /^\d+$/.test(v) && Number(v) > 0;
  const valid = rows.length > 0
    && rows.every((r) => r.disk_id.trim().length > 0)
    && isPositiveInt(capacityTiB)
    && isPositiveInt(zoneSizeGiB);

  const addRow = () =>
    setRows((prev) => [...prev, { disk_id: randomDiskId(), disk_type: 'Ssd', device_path: '' }]);
  const removeRow = (idx: number) => setRows((prev) => prev.filter((_, i) => i !== idx));
  const updateRow = (idx: number, patch: Partial<DiskRow>) =>
    setRows((prev) => prev.map((r, i) => (i === idx ? { ...r, ...patch } : r)));

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      const capacityBytes = Number(capacityTiB) * 1024 * 1024 * 1024 * 1024;
      const zoneSizeBytes = Number(zoneSizeGiB) * 1024 * 1024 * 1024;
      const disks: AddDiskRequest[] = rows.map((r) => ({
        disk_id: r.disk_id.trim(),
        disk_type: r.disk_type,
        capacity_bytes: capacityBytes,
        zone_size_bytes: zoneSizeBytes,
        unit_size_bytes: DEFAULT_UNIT_SIZE_BYTES,
        device_path: r.device_path.trim() || undefined,
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
              label="Disk Size (TiB)"
              inputMode="numeric"
              value={capacityTiB}
              onChange={(e) => setCapacityTiB(e.target.value)}
            />
          </div>
          <div className="tw-flex-1">
            <Input
              label="Zone Size (GiB)"
              inputMode="numeric"
              value={zoneSizeGiB}
              onChange={(e) => setZoneSizeGiB(e.target.value)}
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
            <div className="tw-flex-1">
              <Input
                label={idx === 0 ? 'Device Path (optional)' : undefined}
                placeholder="/dev/nvme0n1"
                value={row.device_path}
                onChange={(e) => updateRow(idx, { device_path: e.target.value })}
              />
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
