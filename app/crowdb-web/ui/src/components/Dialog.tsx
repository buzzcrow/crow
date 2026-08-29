// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { ReactNode, useRef, useEffect, useCallback } from 'react';
import { X } from 'lucide-react';
import { cn } from '../utils/cn';
import { Button } from './ui/Button';

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

export interface DialogProps {
  isOpen: boolean;
  onClose: () => void;
  /** Title shown at the top of the dialog. */
  title: string;
  /** Optional human description shown in the header. */
  description?: string;
  /** Dialog body content. */
  children: ReactNode;
  /** Optional footer content. If not provided, shows Cancel/Confirm buttons. */
  footer?: ReactNode;
  /** Override the confirm button label (when using default footer). */
  confirmLabel?: string;
  /** Override the cancel button label (when using default footer). */
  cancelLabel?: string;
  /** Called when confirm button is clicked (when using default footer). */
  onConfirm?: () => void | Promise<void>;
  /** Whether the confirm button should be disabled. */
  confirmDisabled?: boolean;
  /** Whether confirm is loading. */
  confirmLoading?: boolean;
  /** Whether the action is destructive. */
  destructive?: boolean;
  /** Optional size variant. */
  size?: 'sm' | 'md' | 'lg' | 'xl';
  /** Optional className for the dialog container. */
  className?: string;
}

/**
 * Reusable modal dialog component with focus trapping, accessible controls,
 * and consistent styling.
 */
export function Dialog({
  isOpen,
  onClose,
  title,
  description,
  children,
  footer,
  confirmLabel = 'Confirm',
  cancelLabel = 'Cancel',
  onConfirm,
  confirmDisabled = false,
  confirmLoading = false,
  destructive = false,
  size = 'md',
  className,
}: DialogProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const { handleKeyDown } = useFocusTrap(containerRef, isOpen);

  // Handle Escape key to close
  useEffect(() => {
    if (!isOpen) return;

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };

    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const sizeClasses: Record<string, string> = {
    sm: 'tw-max-w-sm',
    md: 'tw-max-w-md',
    lg: 'tw-max-w-lg',
    xl: 'tw-max-w-xl',
  };

  return (
    <div
      className="tw-fixed tw-inset-0 tw-z-[100] tw-flex tw-items-center tw-justify-center tw-bg-black/60 tw-animate-fade-in"
      role="dialog"
      aria-modal="true"
      aria-labelledby="dialog-title"
      aria-describedby={description ? "dialog-description" : undefined}
      onKeyDown={(e) => {
        handleKeyDown(e as unknown as KeyboardEvent);
      }}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={containerRef}
        className={cn(
          'tw-w-full tw-max-h-[85vh] tw-bg-panel tw-border tw-border-border tw-rounded-lg tw-shadow-2xl tw-animate-scale-in tw-flex tw-flex-col tw-overflow-hidden',
          sizeClasses[size],
          className
        )}
        tabIndex={-1}
      >
        {/* Header */}
        <div className="tw-flex tw-items-center tw-justify-between tw-px-4 tw-py-3 tw-border-b tw-border-border">
          <div className="tw-flex-1 tw-min-w-0">
            <h2 id="dialog-title" className="tw-text-sm tw-font-semibold tw-text-text">
              {title}
            </h2>
            {description && (
              <p id="dialog-description" className="tw-text-xs tw-text-muted tw-mt-1">
                {description}
              </p>
            )}
          </div>
          <button
            onClick={onClose}
            className="tw-text-muted hover:tw-text-text tw-transition-colors tw-ml-2"
            aria-label="Close dialog"
          >
            <X className="tw-h-4 tw-w-4" />
          </button>
        </div>

        {/* Body */}
        <div className="tw-px-4 tw-py-3 tw-flex-1 tw-overflow-y-auto">
          {children}
        </div>

        {/* Footer */}
        <div className="tw-flex tw-items-center tw-justify-end tw-gap-2 tw-px-4 tw-py-3 tw-border-t tw-border-border">
          {footer !== undefined ? (
            footer
          ) : (
            <>
              <Button variant="ghost" onClick={onClose}>
                {cancelLabel}
              </Button>
              {onConfirm && (
                <Button
                  variant={destructive ? 'destructive' : 'default'}
                  onClick={onConfirm}
                  disabled={confirmDisabled}
                  isLoading={confirmLoading}
                >
                  {confirmLabel}
                </Button>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
