import { createContext, useContext, useState, useCallback, ReactNode } from 'react';
import { nextId } from '../utils/ids';

export type ToastType = 'success' | 'error' | 'warning' | 'info';

export interface Toast {
  id: string;
  type: ToastType;
  message: string;
  duration?: number;
  action?: {
    label: string;
    onClick: () => void;
  };
}

interface ToastContextType {
  toasts: Toast[];
  addToast: (toast: Omit<Toast, 'id'>) => string;
  removeToast: (id: string) => void;
  clearToasts: () => void;
  success: (message: string, options?: Omit<Omit<Toast, 'id'>, 'type' | 'message'>) => string;
  error: (message: string, options?: Omit<Omit<Toast, 'id'>, 'type' | 'message'>) => string;
  warning: (message: string, options?: Omit<Omit<Toast, 'id'>, 'type' | 'message'>) => string;
  info: (message: string, options?: Omit<Omit<Toast, 'id'>, 'type' | 'message'>) => string;
}

const ToastContext = createContext<ToastContextType | undefined>(undefined);

interface ToastProviderProps {
  children: ReactNode;
  defaultDuration?: number;
}

export function ToastProvider({ children, defaultDuration = 4000 }: ToastProviderProps) {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const removeToast = useCallback((id: string) => {
    setToasts(prev => prev.filter(toast => toast.id !== id));
  }, []);

  const addToast = useCallback(
    (toast: Omit<Toast, 'id'>): string => {
      const id = nextId('toast');
      const newToast: Toast = {
        id,
        duration: defaultDuration,
        ...toast,
      };

      setToasts(prev => [...prev, newToast]);

      // Auto-remove toast after duration if not persistent (duration 0 = persistent)
      if (newToast.duration && newToast.duration > 0) {
        setTimeout(() => {
          removeToast(id);
        }, newToast.duration);
      }

      return id;
    },
    [defaultDuration, removeToast]
  );

  const clearToasts = useCallback(() => {
    setToasts([]);
  }, []);

  // Helper methods for different toast types
  const success = useCallback(
    (message: string, options?: Omit<Omit<Toast, 'id'>, 'type' | 'message'>) => {
      return addToast({ type: 'success', message, ...options });
    },
    [addToast]
  );

  const error = useCallback(
    (message: string, options?: Omit<Omit<Toast, 'id'>, 'type' | 'message'>) => {
      return addToast({ type: 'error', message, duration: 0, ...options }); // Errors persist by default
    },
    [addToast]
  );

  const warning = useCallback(
    (message: string, options?: Omit<Omit<Toast, 'id'>, 'type' | 'message'>) => {
      return addToast({ type: 'warning', message, ...options });
    },
    [addToast]
  );

  const info = useCallback(
    (message: string, options?: Omit<Omit<Toast, 'id'>, 'type' | 'message'>) => {
      return addToast({ type: 'info', message, ...options });
    },
    [addToast]
  );

  return (
    <ToastContext.Provider
      value={{
        toasts,
        addToast,
        removeToast,
        clearToasts,
        success,
        error,
        warning,
        info,
      }}
    >
      {children}
    </ToastContext.Provider>
  );
}

export function useToast() {
  const context = useContext(ToastContext);
  if (context === undefined) {
    throw new Error('useToast must be used within a ToastProvider');
  }
  return context;
}
