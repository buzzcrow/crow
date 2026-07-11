'use client';
import { X, CheckCircle2, AlertTriangle, XCircle, Info } from 'lucide-react';
import { useToast } from '../contexts/ToastContext';
import { cn } from '../utils/cn';
import { Button } from './ui/Button';

const toastIcons = {
  success: <CheckCircle2 className="tw-h-5 tw-w-5 tw-text-green-500" />,
  error: <XCircle className="tw-h-5 tw-w-5 tw-text-red-500" />,
  warning: <AlertTriangle className="tw-h-5 tw-w-5 tw-text-yellow-500" />,
  info: <Info className="tw-h-5 tw-w-5 tw-text-blue-500" />,
};

const toastBgColors = {
  success: 'tw-bg-green-50 tw-border-green-200 dark:tw-bg-green-950 dark:tw-border-green-900',
  error: 'tw-bg-red-50 tw-border-red-200 dark:tw-bg-red-950 dark:tw-border-red-900',
  warning: 'tw-bg-yellow-50 tw-border-yellow-200 dark:tw-bg-yellow-950 dark:tw-border-yellow-900',
  info: 'tw-bg-blue-50 tw-border-blue-200 dark:tw-bg-blue-950 dark:tw-border-blue-900',
};

const toastTextColors = {
  success: 'tw-text-green-900 dark:tw-text-green-100',
  error: 'tw-text-red-900 dark:tw-text-red-100',
  warning: 'tw-text-yellow-900 dark:tw-text-yellow-100',
  info: 'tw-text-blue-900 dark:tw-text-blue-100',
};

export function ToastContainer() {
  const { toasts, removeToast } = useToast();

  if (toasts.length === 0) return null;

  return (
    <div className="tw-fixed tw-bottom-6 tw-right-6 tw-z-50 tw-flex tw-flex-col tw-gap-3 tw-w-full tw-max-w-sm">
      {toasts.map(toast => (
        <div
          key={toast.id}
          className={cn(
            'tw-border tw-rounded-lg tw-shadow-lg tw-p-4 tw-flex tw-items-start tw-gap-3 tw-animate-slide-in-right',
            toastBgColors[toast.type]
          )}
          role="alert"
        >
          <div className="tw-flex-shrink-0">{toastIcons[toast.type]}</div>
          <div className="tw-flex-1 tw-min-w-0">
            <p className={cn('tw-text-sm tw-font-medium', toastTextColors[toast.type])}>{toast.message}</p>
            {toast.action && (
              <div className="tw-mt-2">
                <Button
                  variant="link"
                  size="sm"
                  className="tw-p-0 tw-h-auto tw-text-sm"
                  onClick={toast.action.onClick}
                >
                  {toast.action.label}
                </Button>
              </div>
            )}
          </div>
          <button
            onClick={() => removeToast(toast.id)}
            className="tw-flex-shrink-0 tw-text-gray-400 hover:tw-text-gray-600 dark:tw-text-gray-500 dark:hover:tw-text-gray-300 tw-transition-colors"
            aria-label="Close notification"
          >
            <X className="tw-h-4 tw-w-4" />
          </button>
        </div>
      ))}
    </div>
  );
}
