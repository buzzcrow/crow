// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version.0.

import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { HwStatusBadge } from './Badge';
import { hwStatusLabel, hwStatusValue, HW_STATUS_NAMES, hwStatusToUiHealth } from '../../utils/entityDisplay';

describe('HwStatusBadge', () => {
  it('renders the correct label for each status', () => {
    for (let s = 0; s < HW_STATUS_NAMES.length; s++) {
      const { getByTitle } = render(<HwStatusBadge status={s} />);
      expect(getByTitle(HW_STATUS_NAMES[s])).toBeTruthy();
    }
  });

  it('uses green text for Up status', () => {
    const { container } = render(<HwStatusBadge status={hwStatusValue('Up')} />);
    const badge = container.querySelector('.tw-text-green-500');
    expect(badge).toBeTruthy();
  });

  it('uses yellow text for Maintenance status', () => {
    const { container } = render(<HwStatusBadge status={hwStatusValue('Maintenance')} />);
    const badge = container.querySelector('.tw-text-yellow-500');
    expect(badge).toBeTruthy();
  });

  it('uses white text for Init status', () => {
    const { container } = render(<HwStatusBadge status={hwStatusValue('Init')} />);
    const badge = container.querySelector('.tw-text-white');
    expect(badge).toBeTruthy();
  });

  it('uses red text for Bad status', () => {
    const { container } = render(<HwStatusBadge status={hwStatusValue('Bad')} />);
    const badge = container.querySelector('.tw-text-red-500');
    expect(badge).toBeTruthy();
  });

  it('uses red text for Offline status', () => {
    const { container } = render(<HwStatusBadge status={hwStatusValue('Offline')} />);
    const badge = container.querySelector('.tw-text-red-500');
    expect(badge).toBeTruthy();
  });

  it('uses yellow text for Suspect status', () => {
    const { container } = render(<HwStatusBadge status={hwStatusValue('Suspect')} />);
    const badge = container.querySelector('.tw-text-yellow-500');
    expect(badge).toBeTruthy();
  });

  it('uses red text for Missing status', () => {
    const { container } = render(<HwStatusBadge status={hwStatusValue('Missing')} />);
    const badge = container.querySelector('.tw-text-red-500');
    expect(badge).toBeTruthy();
  });
});

describe('hwStatusLabel / hwStatusValue round-trip', () => {
  it('round-trips all status values', () => {
    for (let s = 0; s < HW_STATUS_NAMES.length; s++) {
      expect(hwStatusValue(hwStatusLabel(s))).toBe(s);
    }
  });

  it('returns Init for out-of-range values', () => {
    expect(hwStatusLabel(99)).toBe('Init');
    expect(hwStatusLabel(-1)).toBe('Init');
  });
});

describe('hwStatusToUiHealth', () => {
  it('maps Up to Healthy', () => {
    expect(hwStatusToUiHealth(1)).toBe('Healthy');
  });

  it('maps Init to Unknown', () => {
    expect(hwStatusToUiHealth(0)).toBe('Unknown');
  });

  it('maps Maintenance and Suspect to Degraded', () => {
    expect(hwStatusToUiHealth(2)).toBe('Degraded');
    expect(hwStatusToUiHealth(3)).toBe('Degraded');
  });

  it('maps Missing, Bad, Offline to Failed', () => {
    expect(hwStatusToUiHealth(4)).toBe('Failed');
    expect(hwStatusToUiHealth(5)).toBe('Failed');
    expect(hwStatusToUiHealth(6)).toBe('Failed');
  });
});
