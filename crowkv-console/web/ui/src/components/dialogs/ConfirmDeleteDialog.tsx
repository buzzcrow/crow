// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { AlertTriangle } from 'lucide-react';
import { Dialog } from '../Dialog';
import { useToast } from '../../contexts/ToastContext';

export interface ConfirmDeleteDialogProps {
  isOpen: boolean;
  onClose: () => void;
  /** Type of resource being deleted (rack, node, store, group, replica). */
  resourceType: string;
  /** ID/name of the resource being deleted. */
  resourceId: string;
  /** Called when deletion is confirmed. */
  onDelete: () => void | Promise<void>;
  /** Optional success message override. */
  successMessage?: string;
}

/**
 * Generic confirmation dialog for delete operations.
 */
export function ConfirmDeleteDialog({
  isOpen,
  onClose,
  resourceType,
  resourceId,
  onDelete,
  successMessage,
}: ConfirmDeleteDialogProps) {
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();

  const handleDelete = async () => {
    setIsLoading(true);
    try {
      await onDelete();
      success(successMessage || `${resourceType} "${resourceId}" deleted successfully`);
      onClose();
    } catch (err) {
      const message = err instanceof Error ? err.message : `Failed to delete ${resourceType}`;
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={`Delete ${resourceType}`}
      description={`Are you sure you want to delete ${resourceType} "${resourceId}"? This action cannot be undone.`}
      confirmLabel={`Delete ${resourceType}`}
      onConfirm={handleDelete}
      confirmLoading={isLoading}
      destructive
    >
      <div
        className="tw-flex tw-items-start tw-gap-2 tw-p-3 tw-rounded-md tw-bg-failed/10 tw-border tw-border-failed/30 tw-text-failed"
        role="alert"
      >
        <AlertTriangle className="tw-h-4 tw-w-4 tw-flex-shrink-0 tw-mt-0.5" aria-hidden="true" />
        <div className="tw-text-xs">
          This is a destructive operation and cannot be undone.
        </div>
      </div>
    </Dialog>
  );
}
