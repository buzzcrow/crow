// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import React from 'react';
import { cn } from '../../utils/cn';
import { Loader2 } from 'lucide-react';

type ButtonVariant = 'default' | 'secondary' | 'destructive' | 'ghost' | 'outline' | 'link';
type ButtonSize = 'sm' | 'md' | 'lg' | 'icon';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  isLoading?: boolean;
  leftIcon?: React.ReactNode;
  rightIcon?: React.ReactNode;
}

const variantClasses: Record<ButtonVariant, string> = {
  default: 'tw-bg-accent tw-text-white tw-hover:bg-accent/90',
  secondary: 'tw-bg-panel tw-text-text tw-hover:bg-panel/80 tw-border tw-border-border',
  destructive: 'tw-bg-red-600 tw-text-white tw-hover:bg-red-700',
  ghost: 'tw-hover:bg-panel tw-hover:text-text',
  outline: 'tw-border tw-border-border tw-bg-transparent tw-hover:bg-panel',
  link: 'tw-text-accent tw-underline-offset-4 tw-hover:underline tw-bg-transparent',
};

const sizeClasses: Record<ButtonSize, string> = {
  sm: 'tw-h-8 tw-px-3 tw-text-xs tw-rounded-md',
  md: 'tw-h-10 tw-px-4 tw-py-2 tw-rounded-md',
  lg: 'tw-h-12 tw-px-6 tw-rounded-md',
  icon: 'tw-h-10 tw-w-10 tw-rounded-md',
};

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      variant = 'default',
      size = 'md',
      isLoading = false,
      leftIcon,
      rightIcon,
      children,
      disabled,
      ...props
    },
    ref
  ) => {
    return (
      <button
        className={cn(
          'tw-inline-flex tw-items-center tw-justify-center tw-gap-2 tw-font-medium tw-transition-colors tw-focus-visible:outline-none tw-focus-visible:ring-2 tw-focus-visible:ring-accent tw-disabled:pointer-events-none tw-disabled:opacity-50',
          variantClasses[variant],
          sizeClasses[size],
          className
        )}
        ref={ref}
        disabled={disabled || isLoading}
        {...props}
      >
        {isLoading && <Loader2 className="tw-h-4 tw-w-4 tw-animate-spin" />}
        {!isLoading && leftIcon}
        {children}
        {rightIcon}
      </button>
    );
  }
);

Button.displayName = 'Button';
