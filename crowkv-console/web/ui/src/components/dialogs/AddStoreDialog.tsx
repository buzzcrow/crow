import { useEffect, useRef, useState } from 'react';
import { Dialog } from '../Dialog';
import { Input } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addStore } from '../../api';
import { Node, CrowKVServerView } from '../../types';
import { isCrowKVServerAvailable } from '../../data/crowkvServers';

export interface AddStoreDialogProps {
  isOpen: boolean;
  onClose: () => void;
  nodes: Node[];
  servers?: CrowKVServerView[];
  defaultStoreId?: string;
  defaultNodeIds?: string[];
  onSuccess?: () => void | Promise<void>;
}

/**
 * Create a new empty cluster-wide KV store. Group and replica creation are
 * separate follow-up steps.
 *
 * Backend contract: `crowkv-console/web/src/mgmt.rs::CreateStoreBody`.
 */
export function AddStoreDialog({
  isOpen,
  onClose,
  nodes,
  servers = [],
  defaultStoreId = '',
  defaultNodeIds = [],
  onSuccess,
}: AddStoreDialogProps) {
  const availableNodes = nodes.filter((node) =>
    servers.some((server) => server.node_id === node.id && isCrowKVServerAvailable(server)),
  );
  const defaultSelectedNodeIds = defaultNodeIds.filter((id) => availableNodes.some((n) => n.id === id));
  const [storeId, setStoreId] = useState(defaultStoreId);
  const [selectedNodeIds, setSelectedNodeIds] = useState<string[]>(defaultSelectedNodeIds);
  const [isLoading, setIsLoading] = useState(false);
  const wasOpenRef = useRef(false);
  const { success, error } = useToast();

  const isNumeric = (v: string) => /^\d+$/.test(v.trim());
  const valid = isNumeric(storeId) && selectedNodeIds.length > 0;

  const reset = () => {
    setStoreId(defaultStoreId);
    setSelectedNodeIds(defaultSelectedNodeIds);
  };

  useEffect(() => {
    if (isOpen && !wasOpenRef.current) reset();
    wasOpenRef.current = isOpen;
  }, [defaultSelectedNodeIds, defaultStoreId, isOpen]);

  const handleSubmit = async () => {
    if (!valid) return;
    setIsLoading(true);
    try {
      await addStore({
        store_id: storeId.trim(),
        nodes: selectedNodeIds,
      });
      success(`KV Store ${storeId} created successfully`);
      reset();
      onClose();
      await onSuccess?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create KV store';
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    reset();
    onClose();
  };

  const toggleNode = (nodeId: string) => {
    setSelectedNodeIds((prev) =>
      prev.includes(nodeId) ? prev.filter((id) => id !== nodeId) : [...prev, nodeId],
    );
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleClose}
      title="Add KV Store"
      description="Create a new empty KV store on the selected CrowKV nodes. Groups and replicas are created separately."
      confirmLabel="Create KV Store"
      onConfirm={handleSubmit}
      confirmDisabled={!valid || isLoading}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        <Input
          label="KV Store ID (numeric)"
          placeholder="7"
          inputMode="numeric"
          value={storeId}
          onChange={(e) => setStoreId(e.target.value)}
          autoFocus
        />
        <div className="tw-space-y-2">
          <label className="tw-text-xs tw-font-medium tw-text-text">
            CrowKV Nodes (select at least one)
          </label>
          {availableNodes.length === 0 ? (
            <div className="tw-text-sm tw-text-muted">
              No reachable CrowKV nodes available. Deploy a CrowKV server and wait until it is Running and Up.
            </div>
          ) : (
            <div className="tw-max-h-40 tw-overflow-y-auto tw-border tw-border-border tw-rounded-md tw-p-2 tw-space-y-1">
              {availableNodes.map((node) => (
                <label
                  key={node.id}
                  className="tw-flex tw-items-center tw-gap-2 tw-p-2 tw-rounded tw-cursor-pointer hover:tw-bg-bg"
                >
                  <input
                    type="checkbox"
                    checked={selectedNodeIds.includes(node.id)}
                    onChange={() => toggleNode(node.id)}
                    className="tw-h-4 tw-w-4 tw-rounded tw-border-border tw-text-accent focus:tw-ring-accent"
                  />
                  <span className="tw-text-sm tw-text-text">
                    {node.id}
                    <span className="tw-text-xs tw-text-muted tw-ml-1">({node.host})</span>
                  </span>
                </label>
              ))}
            </div>
          )}
        </div>
      </div>
    </Dialog>
  );
}
