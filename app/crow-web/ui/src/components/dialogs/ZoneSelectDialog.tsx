// Copyright 2026-present buzzcrow <buzzcrow@126.com>

import { useEffect, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';

export interface ZoneSelectDialogProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  description: string;
  confirmLabel: string;
  diskId: string;
  zoneCount?: number;
  onConfirm: (diskId: string, zoneIndices: number[] | null) => Promise<void>;
}

function parseZoneRanges(input: string): number[] | null {
  const trimmed = input.trim().toLowerCase();
  if (!trimmed || trimmed === 'all') return null; // null = all zones
  const parts = trimmed.split(',').map((s) => s.trim()).filter(Boolean);
  if (parts.length === 0) return null;
  const result: number[] = [];
  for (const part of parts) {
    const rangeMatch = part.match(/^(\d+)\s*-\s*(\d+)$/);
    if (rangeMatch) {
      const lo = Number(rangeMatch[1]);
      const hi = Number(rangeMatch[2]);
      if (lo > hi) return [];
      for (let i = lo; i <= hi; i++) result.push(i);
    } else if (/^\d+$/.test(part)) {
      result.push(Number(part));
    } else {
      return []; // parse error
    }
  }
  return result;
}

export function ZoneSelectDialog({
  isOpen,
  onClose,
  title,
  description,
  confirmLabel,
  diskId,
  zoneCount,
  onConfirm,
}: ZoneSelectDialogProps) {
  const [zoneInput, setZoneInput] = useState('all');
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { error } = useToast();

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) {
      setZoneInput('all');
    }
    wasOpenRef.current = isOpen;
  }, [isOpen]);

  const parsed = parseZoneRanges(zoneInput);
  // parseZoneRanges returns null for "all"/empty, [] for parse error,
  // or a non-empty array for valid specific zones.
  const valid = parsed === null || parsed.length > 0;

  const handleSubmit = async () => {
    if (!valid) {
      error('Invalid zone format. Use e.g. "1-100,299,400-410" or "all".');
      return;
    }
    setIsLoading(true);
    try {
      await onConfirm(diskId, parsed);
      onClose();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Operation failed';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  const zoneHint = zoneCount != null ? ` (disk has ${zoneCount} zones)` : '';

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={title}
      description={description}
      confirmLabel={confirmLabel}
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        <Input
          label={`Zones${zoneHint}`}
          placeholder="all  or  e.g. 1-100,299,400-410"
          value={zoneInput}
          onChange={(e) => setZoneInput(e.target.value)}
          autoFocus
        />
        <p className="tw-text-xs tw-text-muted">
          Enter <code>all</code> for all zones, or specify ranges and individual zones
          separated by commas (e.g. <code>1-100,299,400-410</code>).
        </p>
      </div>
    </Dialog>
  );
}
