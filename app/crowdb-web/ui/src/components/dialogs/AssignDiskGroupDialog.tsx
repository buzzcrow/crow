// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useMemo, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Select } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { setDiskGroupOwner, setDiskGroupBind } from '../../api';
import type { DiskdbInstanceInfo, EnrichedStoreView } from '../../types';

export interface AssignDiskGroupDialogProps {
  isOpen: boolean;
  onClose: () => void;
  rackId: number;
  nodeId: number;
  dgId: number;
  dgName?: string;
  instances: DiskdbInstanceInfo[];
  stores: EnrichedStoreView[];
  onSuccess?: () => void | Promise<void>;
}

export function AssignDiskGroupDialog({
  isOpen,
  onClose,
  rackId,
  nodeId,
  dgId,
  dgName,
  instances,
  stores,
  onSuccess,
}: AssignDiskGroupDialogProps) {
  const [instanceId, setInstanceId] = useState('');
  const [storeId, setStoreId] = useState('');
  const [groupId, setGroupId] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();
  // Apply default selections once per open. The dialog's `instances` and
  // `stores` props get fresh array references on every background poll
  // (useCapacityTree / useLogicalTree refresh every 5s); depending on
  // them directly re-ran this effect mid-dialog and wiped the user's
  // group selection, leaving the confirm button disabled.
  const defaultsApplied = useRef(false);
  useEffect(() => {
    if (!isOpen || defaultsApplied.current) return;
    if (instances.length === 0 && stores.length === 0) return;
    setInstanceId(instances.length > 0 ? String(instances[0].instance_id) : '');
    setStoreId(stores.length > 0 ? String(stores[0].store_id) : '');
    setGroupId('');
    defaultsApplied.current = true;
  }, [isOpen, instances, stores]);

  const groupsForStore = useMemo(() => {
    const store = stores.find((s) => String(s.store_id) === storeId);
    return store?.groups || [];
  }, [stores, storeId]);

  const valid = instanceId && storeId && groupId;

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      const leaseMs = Date.now() + 3_600_000;
      await setDiskGroupOwner(rackId, nodeId, dgId, {
        instance_id: instanceId,
        lease_expiry_ms: leaseMs,
      });
      await setDiskGroupBind(rackId, nodeId, dgId, {
        store_id: Number(storeId),
        group_id: Number(groupId),
      });
      success(`DG-${dgId} assigned to diskdb-${instanceId} (bound to store ${storeId} group ${groupId})`);
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to assign disk-group';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  const dgLabel = dgName ? `${dgName} (DG-${dgId})` : `DG-${dgId}`;

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="Assign Disk Group"
      description={`Assign ${dgLabel} to a diskdb instance and bind it to a paxos data group. The diskdb instance will take ownership on its next keepalive sync and start reporting capacity.`}
      confirmLabel="Assign"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        {instances.length === 0 && (
          <div className="tw-text-sm tw-text-muted">
            No diskdb instances registered. Deploy a diskdb instance on a node first.
          </div>
        )}
        <Select
          label="DiskDB Instance"
          value={instanceId}
          onChange={(e) => setInstanceId(e.target.value)}
        >
          <option value="" disabled>Select a diskdb instance</option>
          {instances.map((inst) => (
            <option key={inst.instance_id} value={inst.instance_id}>
              diskdb-{inst.instance_id} ({inst.owned_dg_ids.length} DG(s))
            </option>
          ))}
        </Select>
        <Select
          label="Paxos Store"
          value={storeId}
          onChange={(e) => {
            setStoreId(e.target.value);
            setGroupId('');
          }}
        >
          <option value="" disabled>Select a store</option>
          {stores.map((store) => (
            <option key={store.store_id} value={store.store_id}>
              Store {store.store_id} ({store.groups.length} group(s))
            </option>
          ))}
        </Select>
        <Select
          label="Paxos Data Group"
          value={groupId}
          onChange={(e) => setGroupId(e.target.value)}
          disabled={groupsForStore.length === 0}
        >
          <option value="" disabled>Select a data group</option>
          {groupsForStore.map((group) => (
            <option key={group.group_id} value={group.group_id}>
              Group {group.group_id}
            </option>
          ))}
        </Select>
        {groupsForStore.length === 0 && storeId && (
          <div className="tw-text-xs tw-text-muted">
            No groups in store {storeId}. Create a group first.
          </div>
        )}
      </div>
    </Dialog>
  );
}
