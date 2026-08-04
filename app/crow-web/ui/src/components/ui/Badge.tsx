// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import React from 'react';
import { cn } from '../../utils/cn';
import { CheckCircle2, AlertTriangle, XCircle, HelpCircle, Crown, Users } from 'lucide-react';
import { ReplicaRole, ReplicaState, GroupHealth, NodeHealth } from '../../types';
import { toDisplayState, toUiHealth, toUiRole } from '../../utils/entityDisplay';

type BadgeVariant = 'default' | 'secondary' | 'outline' | 'health' | 'role';
type BadgeSize = 'sm' | 'md' | 'lg';

interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  variant?: BadgeVariant;
  size?: BadgeSize;
  healthStatus?: 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';
  role?: 'Leader' | 'Follower' | 'Remote';
  icon?: React.ReactNode;
  compact?: boolean;
}

const variantClasses: Record<BadgeVariant, string> = {
  default: 'tw-bg-accent tw-text-white',
  secondary: 'tw-bg-panel tw-text-text',
  outline: 'tw-border tw-border-border tw-bg-transparent',
  health: '', // Health status will set colors dynamically
  role: '', // Role will set colors dynamically
};

const sizeClasses: Record<BadgeSize, string> = {
  sm: 'tw-px-1.5 tw-py-0.5 tw-text-xs tw-rounded',
  md: 'tw-px-2.5 tw-py-0.5 tw-text-sm tw-rounded-md',
  lg: 'tw-px-3 tw-py-1 tw-text-base tw-rounded-lg',
};

const healthColors = {
  Healthy: 'tw-bg-green-500/10 tw-text-green-500 tw-border tw-border-green-500/30',
  Degraded: 'tw-bg-yellow-500/10 tw-text-yellow-500 tw-border tw-border-yellow-500/30',
  Failed: 'tw-bg-red-500/10 tw-text-red-500 tw-border tw-border-red-500/30',
  Unknown: 'tw-bg-gray-500/10 tw-text-gray-500 tw-border tw-border-gray-500/30',
};

const healthIcons = {
  Healthy: <CheckCircle2 className="tw-h-3.5 tw-w-3.5" />,
  Degraded: <AlertTriangle className="tw-h-3.5 tw-w-3.5" />,
  Failed: <XCircle className="tw-h-3.5 tw-w-3.5" />,
  Unknown: <HelpCircle className="tw-h-3.5 tw-w-3.5" />,
};

const roleColors = {
  Leader: 'tw-bg-amber-400/15 tw-text-amber-300 tw-border tw-border-amber-300/40',
  Follower: 'tw-bg-blue-500/10 tw-text-blue-500 tw-border tw-border-blue-500/30',
  Remote: 'tw-bg-purple-500/10 tw-text-purple-500 tw-border tw-border-purple-500/30',
};

const roleIcons = {
  Leader: <Crown className="tw-h-3.5 tw-w-3.5" />,
  Follower: <Users className="tw-h-3.5 tw-w-3.5" />,
  Remote: <Users className="tw-h-3.5 tw-w-3.5" />,
};

const roleCompactLabel: Record<string, string> = {
  Leader: 'L',
  Follower: 'F',
  Remote: 'R',
};

export const Badge = React.forwardRef<HTMLSpanElement, BadgeProps>(
  ({ className, variant = 'default', size = 'md', healthStatus, role, icon, compact = false, children, ...props }, ref) => {
    let dynamicClasses = '';
    let dynamicIcon = icon;

    if (variant === 'health' && healthStatus) {
      dynamicClasses = healthColors[healthStatus];
      dynamicIcon = healthIcons[healthStatus];
    } else if (variant === 'role' && role) {
      dynamicClasses = roleColors[role];
      dynamicIcon = roleIcons[role];
    }

    return (
      <span
        ref={ref}
        className={cn(
          'tw-inline-flex tw-items-center tw-gap-1.5 tw-font-medium',
          variantClasses[variant],
          compact ? 'tw-px-1 tw-py-0.5 tw-text-xs tw-rounded' : sizeClasses[size],
          dynamicClasses,
          className
        )}
        {...props}
      >
        {dynamicIcon}
        {compact && variant === 'role' && role && roleCompactLabel[role]}
        {!compact && children}
      </span>
    );
  }
);

Badge.displayName = 'Badge';

// Convenience components for common use cases
export function HealthBadge({
  status,
  size = 'sm',
  compact = false,
}: {
  status: NodeHealth | GroupHealth | ReplicaState | 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';
  size?: BadgeSize;
  compact?: boolean;
}) {
  const normalizedStatus = toUiHealth(status.toString());
  return (
    <Badge variant="health" healthStatus={normalizedStatus} size={size} compact={compact} title={normalizedStatus}>
      {normalizedStatus}
    </Badge>
  );
}

export function RoleBadge({ role, size = 'sm', compact = false }: { role: ReplicaRole | 'Leader' | 'Follower' | 'Remote'; size?: BadgeSize; compact?: boolean }) {
  const normalizedRole = toUiRole(role.toString());
  return (
    <Badge variant="role" role={normalizedRole} size={size} compact={compact} title={normalizedRole}>
      {toDisplayState(normalizedRole)}
    </Badge>
  );
}
