import React from 'react';
import { cn } from '../../utils/cn';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  error?: string;
  label?: string;
}

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, type, error, label, id, ...props }, ref) => {
    const inputId = id || label?.toLowerCase().replace(/\s+/g, '-');

    return (
      <div className="tw-flex tw-flex-col tw-gap-1">
        {label && (
          <label
            htmlFor={inputId}
            className="tw-text-xs tw-font-medium tw-text-text"
          >
            {label}
          </label>
        )}
        <input
          id={inputId}
          type={type}
          className={cn(
            'tw-h-10 tw-w-full tw-rounded-md tw-border tw-border-border tw-bg-bg tw-px-3 tw-py-2 tw-text-sm tw-text-text tw-placeholder:text-muted tw-focus:outline-none tw-focus:ring-2 tw-focus:ring-accent tw-disabled:cursor-not-allowed tw-disabled:opacity-50',
            error && 'tw-border-failed tw-focus:ring-failed',
            className
          )}
          ref={ref}
          {...props}
        />
        {error && (
          <p className="tw-text-xs tw-text-failed">{error}</p>
        )}
      </div>
    );
  }
);

Input.displayName = 'Input';

interface SelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {
  error?: string;
  label?: string;
}

export const Select = React.forwardRef<HTMLSelectElement, SelectProps>(
  ({ className, error, label, id, ...props }, ref) => {
    const inputId = id || label?.toLowerCase().replace(/\s+/g, '-');

    return (
      <div className="tw-flex tw-flex-col tw-gap-1">
        {label && (
          <label
            htmlFor={inputId}
            className="tw-text-xs tw-font-medium tw-text-text"
          >
            {label}
          </label>
        )}
        <select
          id={inputId}
          className={cn(
            'tw-h-10 tw-w-full tw-rounded-md tw-border tw-border-border tw-bg-bg tw-px-3 tw-py-2 tw-text-sm tw-text-text tw-focus:outline-none tw-focus:ring-2 tw-focus:ring-accent tw-disabled:cursor-not-allowed tw-disabled:opacity-50',
            error && 'tw-border-failed tw-focus:ring-failed',
            className
          )}
          ref={ref}
          {...props}
        />
        {error && (
          <p className="tw-text-xs tw-text-failed">{error}</p>
        )}
      </div>
    );
  }
);

Select.displayName = 'Select';
