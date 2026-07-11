import { ReactNode, useRef, useEffect, useCallback } from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  X,
} from 'lucide-react';
import { Button } from './ui/Button';
import { BulkOperationState, BulkItemResult } from '../hooks/useBulkOperations';

/**
 * Get all focusable elements within a container
 */
function getFocusableElements(container: HTMLElement): HTMLElement[] {
  const focusableSelectors = [
    'a[href]',
    'button:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
  ];
  return Array.from(container.querySelectorAll(focusableSelectors.join(','))) as HTMLElement[];
}

/**
 * Hook for trapping focus within a container
 */
function useFocusTrap(containerRef: React.RefObject<HTMLElement>, isActive: boolean) {
  const previousFocusRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (!isActive || !containerRef.current) return;

    // Save previous focus
    previousFocusRef.current = document.activeElement as HTMLElement;

    // Focus the first focusable element or the container itself
    const focusable = getFocusableElements(containerRef.current);
    if (focusable.length > 0) {
      focusable[0].focus();
    } else {
      containerRef.current.focus();
    }

    return () => {
      // Restore previous focus when trap is deactivated
      if (previousFocusRef.current) {
        previousFocusRef.current.focus();
      }
    };
  }, [isActive, containerRef]);

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (!isActive || !containerRef.current || e.key !== 'Tab') return;

    const focusable = getFocusableElements(containerRef.current);
    if (focusable.length === 0) return;

    const currentIndex = focusable.indexOf(document.activeElement as HTMLElement);

    if (e.shiftKey) {
      // Move to previous or wrap to end
      if (currentIndex <= 0) {
        e.preventDefault();
        focusable[focusable.length - 1].focus();
      }
    } else {
      // Move to next or wrap to start
      if (currentIndex === -1 || currentIndex >= focusable.length - 1) {
        e.preventDefault();
        focusable[0].focus();
      }
    }
  }, [isActive, containerRef]);

  return { handleKeyDown };
}

export interface BulkActionDialogProps<T> {
  isOpen: boolean;
  onClose: () => void;
  /** Title shown at the top of the dialog. */
  title: string;
  /** Optional human description shown above the item list. */
  description?: ReactNode;
  /** Items to act on. */
  items: T[];
  /** Render the display label for an item. */
  renderItem: (item: T) => ReactNode;
  /** Whether the action is destructive — surfaces a warning banner. */
  destructive?: boolean;
  /** Live progress state (from useBulkOperations). */
  state: BulkOperationState<T>;
  /** Invoked when the user confirms the action. */
  onConfirm: () => void | Promise<void>;
  /** Override the confirm button label. */
  confirmLabel?: string;
}

function statusIcon(status: BulkItemResult<unknown>['status']) {
  switch (status) {
    case 'pending':
      return <Clock className="tw-h-4 tw-w-4 tw-text-muted" />;
    case 'running':
      return <Loader2 className="tw-h-4 tw-w-4 tw-text-accent tw-animate-spin" />;
    case 'success':
      return <CheckCircle2 className="tw-h-4 tw-w-4 tw-text-healthy" />;
    case 'failure':
      return <XCircle className="tw-h-4 tw-w-4 tw-text-failed" />;
  }
}

/**
 * Confirmation dialog for bulk operations. Reflects useBulkOperations state:
 * pre-confirm shows the item list + warning; during-run shows per-item
 * progress; post-run keeps the dialog open with per-item results until
 * dismissed.
 */
export function BulkActionDialog<T>({
  isOpen,
  onClose,
  title,
  description,
  items,
  renderItem,
  destructive = false,
  state,
  onConfirm,
  confirmLabel,
}: BulkActionDialogProps<T>) {
  const containerRef = useRef<HTMLDivElement>(null);
  const { handleKeyDown } = useFocusTrap(containerRef, isOpen);

  // Handle Escape key to close
  useEffect(() => {
    if (!isOpen) return;

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !state.isRunning) {
        onClose();
      }
    };

    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [isOpen, onClose, state.isRunning]);

  if (!isOpen) return null;

  const hasStarted = state.total > 0;
  const isDone = hasStarted && !state.isRunning;
  const progress = state.total > 0 ? Math.round((state.completed / state.total) * 100) : 0;

  // While running, prefer the in-progress result list to the original items
  // so the displayed items match what the parent passed in.
  const displayed: { item: T; result?: BulkItemResult<T> }[] = hasStarted
    ? state.results.map((r) => ({ item: r.item, result: r }))
    : items.map((item) => ({ item }));

  return (
    <div
      className="tw-fixed tw-inset-0 tw-z-[100] tw-flex tw-items-center tw-justify-center tw-bg-black/60 tw-animate-fade-in"
      role="dialog"
      aria-modal="true"
      aria-labelledby="bulk-action-title"
      aria-describedby={description ? "bulk-action-description" : undefined}
      onKeyDown={(e) => {
        handleKeyDown(e as unknown as KeyboardEvent);
      }}
      onClick={(e) => {
        if (e.target === e.currentTarget && !state.isRunning) onClose();
      }}
    >
      <div
        ref={containerRef}
        className="tw-w-full tw-max-w-lg tw-bg-panel tw-border tw-border-border tw-rounded-lg tw-shadow-2xl tw-animate-scale-in tw-flex tw-flex-col tw-overflow-hidden"
        tabIndex={-1}
      >
        {/* Header */}
        <div className="tw-flex tw-items-center tw-justify-between tw-px-4 tw-py-3 tw-border-b tw-border-border">
          <h2 id="bulk-action-title" className="tw-text-sm tw-font-semibold tw-text-text">
            {title}
          </h2>
          <button
            onClick={onClose}
            disabled={state.isRunning}
            className="tw-text-muted hover:tw-text-text tw-transition-colors disabled:tw-opacity-50 disabled:tw-cursor-not-allowed"
            aria-label="Close dialog"
          >
            <X className="tw-h-4 tw-w-4" />
          </button>
        </div>

        {/* Body */}
        <div className="tw-px-4 tw-py-3 tw-flex tw-flex-col tw-gap-3">
          {description && (
            <div id="bulk-action-description" className="tw-text-sm tw-text-muted">
              {description}
            </div>
          )}

          {destructive && !hasStarted && (
            <div
              className="tw-flex tw-items-start tw-gap-2 tw-p-3 tw-rounded-md tw-bg-failed/10 tw-border tw-border-failed/30 tw-text-failed"
              role="alert"
            >
              <AlertTriangle className="tw-h-4 tw-w-4 tw-flex-shrink-0 tw-mt-0.5" aria-hidden="true" />
              <div className="tw-text-xs">
                This is a destructive operation and cannot be undone.
              </div>
            </div>
          )}

          {hasStarted && (
            <div className="tw-space-y-1" role="status" aria-live="polite">
              <div className="tw-flex tw-justify-between tw-text-xs tw-text-muted">
                <span>
                  {state.completed} / {state.total} completed
                </span>
                <span>{progress}%</span>
              </div>
              <div className="tw-h-1 tw-bg-bg tw-rounded-full tw-overflow-hidden">
                <div
                  className="tw-h-full tw-bg-accent tw-transition-all"
                  style={{ width: `${progress}%` }}
                  aria-hidden="true"
                />
              </div>
            </div>
          )}

          <div
            className="tw-max-h-64 tw-overflow-y-auto tw-border tw-border-border tw-rounded-md tw-divide-y tw-divide-border"
            role="list"
            aria-label="Items"
          >
            {displayed.map((entry, idx) => (
              <div
                key={idx}
                className="tw-flex tw-items-center tw-gap-2 tw-px-3 tw-py-2 tw-text-sm"
                role="listitem"
              >
                {entry.result ? (
                  <span className="tw-flex-shrink-0" aria-hidden="true">
                    {statusIcon(entry.result.status)}
                  </span>
                ) : (
                  <span className="tw-flex-shrink-0 tw-h-4 tw-w-4" aria-hidden="true" />
                )}
                <div className="tw-flex-1 tw-min-w-0 tw-text-text tw-truncate">
                  {renderItem(entry.item)}
                </div>
                {entry.result?.status === 'failure' && entry.result.error && (
                  <span
                    className="tw-text-xs tw-text-failed tw-truncate tw-max-w-[50%]"
                    title={entry.result.error}
                  >
                    {entry.result.error}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Footer */}
        <div className="tw-flex tw-items-center tw-justify-end tw-gap-2 tw-px-4 tw-py-3 tw-border-t tw-border-border">
          {!hasStarted ? (
            <>
              <Button variant="ghost" onClick={onClose}>
                Cancel
              </Button>
              <Button
                variant={destructive ? 'destructive' : 'default'}
                onClick={onConfirm}
                disabled={items.length === 0}
              >
                {confirmLabel || `Confirm (${items.length})`}
              </Button>
            </>
          ) : (
            <Button onClick={onClose} disabled={state.isRunning}>
              {isDone ? 'Close' : 'Running...'}
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}
