// Copyright 2026-present Gian <crow.db@outlook.com>.

import { describe, it, expect, vi } from 'vitest';
import { render, renderHook, act } from '@testing-library/react';
import { DomainProvider, useDomain } from './DomainContext';
import { Domain } from '../types';

describe('DomainProvider', () => {
  it('defaults to Cluster domain when no initialDomain provided', () => {
    const { result } = renderHook(() => useDomain(), {
      wrapper: ({ children }) => <DomainProvider>{children}</DomainProvider>,
    });
    expect(result.current.domain).toBe(Domain.Cluster);
  });

  it('uses the provided initialDomain', () => {
    const { result } = renderHook(() => useDomain(), {
      wrapper: ({ children }) => (
        <DomainProvider initialDomain={Domain.KV}>{children}</DomainProvider>
      ),
    });
    expect(result.current.domain).toBe(Domain.KV);
  });

  it('setDomain updates the domain', () => {
    const { result } = renderHook(() => useDomain(), {
      wrapper: ({ children }) => <DomainProvider>{children}</DomainProvider>,
    });
    act(() => result.current.setDomain(Domain.Chunk));
    expect(result.current.domain).toBe(Domain.Chunk);
  });

  it('throws when useDomain is used outside a DomainProvider', () => {
    // Suppress the expected error output.
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
    expect(() => renderHook(() => useDomain())).toThrow(
      'useDomain must be used within a DomainProvider',
    );
    spy.mockRestore();
  });

  it('renders children', () => {
    const { getByText } = render(
      <DomainProvider>
        <div>child-content</div>
      </DomainProvider>,
    );
    expect(getByText('child-content')).toBeTruthy();
  });
});
