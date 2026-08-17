// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useMemo, useState } from 'react';
import { Dialog } from '../Dialog';
import { Select } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { moveDisk } from '../../api';
import { Rack, DiskGroupEntry } from '../../types';

export interface MoveDiskDialogProps {
  isOpen: boolean;
  onClose: () => void;
  diskId: string;
  currentRackId: number;
  currentNodeId: number;
  currentDgId: number;
  racks: Rack[];
  /** All disk-groups across all nodes, keyed by node id. */
  diskGroupsByNode: Record<number, DiskGroupEntry[]>;
  onSuccess?: () => void | Promise<void>;
}

export function MoveDiskDialog({
  isOpen,
  onClose,
  diskId,
  currentRackId,
  currentNodeId,
  currentDgId,
  racks,
  diskGroupsByNode,
  onSuccess,
}: MoveDiskDialogProps) {
  const [targetRackId, setTargetRackId] = useState(String(currentRackId));
  const [targetNodeId, setTargetNodeId] = useState(String(currentNodeId));
  const [targetDgId, setTargetDgId] = useState(String(currentDgId));
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();

  const nodesForRack = useMemo(() => {
    const rack = racks.find((r) => String(r.id) === targetRackId);
    return rack?.nodes || [];
  }, [racks, targetRackId]);

  const dgsForNode = useMemo(() => {
    const nodeId = Number(targetNodeId);
    return diskGroupsByNode[nodeId] || [];
  }, [diskGroupsByNode, targetNodeId]);

  useEffect(() => {
    if (!isOpen) return;
    setTargetRackId(String(currentRackId));
    setTargetNodeId(String(currentNodeId));
    setTargetDgId(String(currentDgId));
  }, [isOpen, currentRackId, currentNodeId, currentDgId]);

  const isSamePlacement =
    Number(targetRackId) === currentRackId &&
    Number(targetNodeId) === currentNodeId &&
    Number(targetDgId) === currentDgId;

  const valid =
    targetRackId &&
    targetNodeId &&
    targetDgId &&
    !isSamePlacement &&
    dgsForNode.some((dg) => String(dg.id) === targetDgId);

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      await moveDisk(diskId, {
        new_rack_id: Number(targetRackId),
        new_node_id: Number(targetNodeId),
        new_disk_group_id: Number(targetDgId),
      });
      success(`Disk ${diskId.slice(0, 8)}… moved to DG-${targetDgId} on node ${targetNodeId}`);
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to move disk';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="Move Disk"
      description={`Move disk "${diskId.slice(0, 12)}…" to a new disk-group. The disk's zone/busy/free records are copied to the new placement.`}
      confirmLabel="Move Disk"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        <Select
          label="Target Rack"
          value={targetRackId}
          onChange={(e) => {
            setTargetRackId(e.target.value);
            const rackNodes = racks.find((r) => String(r.id) === e.target.value)?.nodes || [];
            if (rackNodes.length > 0) {
              setTargetNodeId(String(rackNodes[0].id));
              const dgs = diskGroupsByNode[rackNodes[0].id] || [];
              if (dgs.length > 0) setTargetDgId(String(dgs[0].id));
              else setTargetDgId('');
            }
          }}
        >
          <option value="" disabled>Select a rack</option>
          {racks.map((rack) => (
            <option key={rack.id} value={rack.id}>
              {rack.name || rack.id}
            </option>
          ))}
        </Select>
        <Select
          label="Target Node"
          value={targetNodeId}
          onChange={(e) => {
            setTargetNodeId(e.target.value);
            const dgs = diskGroupsByNode[Number(e.target.value)] || [];
            if (dgs.length > 0) setTargetDgId(String(dgs[0].id));
            else setTargetDgId('');
          }}
        >
          <option value="" disabled>Select a node</option>
          {nodesForRack.map((node) => (
            <option key={node.id} value={node.id}>
              N-{node.id}
            </option>
          ))}
        </Select>
        <Select
          label="Target Disk Group"
          value={targetDgId}
          onChange={(e) => setTargetDgId(e.target.value)}
        >
          <option value="" disabled>Select a disk group</option>
          {dgsForNode.map((dg) => (
            <option key={dg.id} value={dg.id}>
              {dg.name ? `${dg.name} (DG-${dg.id})` : `DG-${dg.id}`}
            </option>
          ))}
        </Select>
        {isSamePlacement && (
          <div className="tw-text-xs tw-text-muted">
            Select a different placement to move the disk.
          </div>
        )}
      </div>
    </Dialog>
  );
}
