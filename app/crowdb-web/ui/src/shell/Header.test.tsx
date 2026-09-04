// Copyright 2026-present Gian <crow.db@outlook.com>.

import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent } from '@testing-library/react';
import { DomainProvider } from '../contexts/DomainContext';
import { Header, ClusterHealth } from './Header';
import { Domain } from '../types';

function renderHeader(overrides: Partial<{
  clusterHealth: ClusterHealth;
  onRefresh: () => void;
  refreshing: boolean;
  onShowTopology: () => void;
  onShowCapacity: () => void;
  onResetCluster: () => void;
  initialDomain: Domain;
}> = {}) {
  const onRefresh = overrides.onRefresh ?? vi.fn();
  const onShowTopology = overrides.onShowTopology ?? vi.fn();
  const onShowCapacity = overrides.onShowCapacity ?? vi.fn();
  const onResetCluster = overrides.onResetCluster ?? vi.fn();
  return render(
    <DomainProvider initialDomain={overrides.initialDomain ?? Domain.Cluster}>
      <Header
        clusterHealth={overrides.clusterHealth ?? 'Unknown'}
        onRefresh={onRefresh}
        refreshing={overrides.refreshing ?? false}
        onShowTopology={onShowTopology}
        onShowCapacity={onShowCapacity}
        onResetCluster={onResetCluster}
      />
    </DomainProvider>,
  );
}

describe('Header', () => {
  it('renders the brand title', () => {
    const { getByText } = renderHeader();
    expect(getByText(/CrowDB Storage Console/)).toBeTruthy();
  });

  it('renders the cluster health pill', () => {
    const { getByTitle } = renderHeader({ clusterHealth: 'Healthy' });
    expect(getByTitle('Cluster health: Healthy')).toBeTruthy();
  });

  it('renders Cluster, KV, and Capacity domain toggle buttons', () => {
    const { getByTestId } = renderHeader();
    expect(getByTestId('domain-cluster')).toHaveTextContent('Cluster');
    expect(getByTestId('domain-kv')).toHaveTextContent('KV');
    expect(getByTestId('domain-chunk')).toHaveTextContent('Capacity');
  });

  it('marks the active domain button with aria-pressed=true', () => {
    const { getByTestId } = renderHeader({ initialDomain: Domain.KV });
    expect(getByTestId('domain-kv').getAttribute('aria-pressed')).toBe('true');
    expect(getByTestId('domain-cluster').getAttribute('aria-pressed')).toBe('false');
  });

  it('clicking the KV domain button calls onShowTopology', () => {
    const onShowTopology = vi.fn();
    const { getByTestId } = renderHeader({ onShowTopology });
    fireEvent.click(getByTestId('domain-kv'));
    expect(onShowTopology).toHaveBeenCalledOnce();
  });

  it('clicking the Cluster domain button calls onShowTopology', () => {
    const onShowTopology = vi.fn();
    const { getByTestId } = renderHeader({
      onShowTopology,
      initialDomain: Domain.KV,
    });
    fireEvent.click(getByTestId('domain-cluster'));
    expect(onShowTopology).toHaveBeenCalledOnce();
  });

  it('clicking the Chunk domain button calls onShowCapacity', () => {
    const onShowCapacity = vi.fn();
    const { getByTestId } = renderHeader({ onShowCapacity });
    fireEvent.click(getByTestId('domain-chunk'));
    expect(onShowCapacity).toHaveBeenCalledOnce();
  });

  it('clicking refresh calls onRefresh', () => {
    const onRefresh = vi.fn();
    const { getByTitle } = renderHeader({ onRefresh });
    fireEvent.click(getByTitle(/Refresh/));
    expect(onRefresh).toHaveBeenCalledOnce();
  });

  it('renders the health glyph for each status', () => {
    const { getByText: getHealthy } = renderHeader({ clusterHealth: 'Healthy' });
    expect(getHealthy('✓')).toBeTruthy();
    const { getByText: getDegraded } = renderHeader({ clusterHealth: 'Degraded' });
    expect(getDegraded('!')).toBeTruthy();
    const { getByText: getFailed } = renderHeader({ clusterHealth: 'Failed' });
    expect(getFailed('✕')).toBeTruthy();
  });
});
