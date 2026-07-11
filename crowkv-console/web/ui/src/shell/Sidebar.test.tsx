import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ReactNode } from 'react';
import { Sidebar } from './Sidebar';
import { ViewModeProvider } from '../contexts/ViewModeContext';
import { SelectionProvider } from '../contexts/SelectionContext';
import { ViewMode } from '../types';

const wrapper = ({ children }: { children: ReactNode }) => (
  <ViewModeProvider initialViewMode={ViewMode.Physical}>
    <SelectionProvider>{children}</SelectionProvider>
  </ViewModeProvider>
);

/**
 * Regression for `doc/todo_ui2.md` §5.6: at `recursive>=1` the backend
 * inflates `rack.nodes` from `NodeId[]` to `NodeView[]` (objects with
 * `id`, `host`, `has_server`, …). The Sidebar tree builder used to pass
 * the whole object as a `label`, which crashed React with error #31
 * ("Objects are not valid as a React child"). The fix in `Sidebar.tsx`
 * normalizes both shapes to the node-id string.
 */
describe('Sidebar tree builder', () => {
  it('renders rack nodes when rack.nodes is the legacy NodeId[] shape', () => {
    render(
      <Sidebar racks={[{ id: 'r1', name: 'Rack 1', nodes: ['n1'] as any }]} />,
      { wrapper },
    );
    expect(screen.getByText('Rack 1')).toBeInTheDocument();
    expect(screen.getByText('n1')).toBeInTheDocument();
  });

  it('exposes the unprefixed backend id on tree nodes via rawId', () => {
    let captured: any[] = [];
    render(
      <Sidebar
        racks={[{ id: 'r1', name: 'r1', nodes: ['n1'] as any }]}
        onNodeContextMenu={(n) => captured.push(n)}
      />,
      { wrapper },
    );
    // Simulate the context-menu callback path (the actual DOM event
    // path is exercised elsewhere; here we just check the data
    // contract). Find the rack and node entries via the in-tree IDs.
    const rackText = screen.getByText('r1');
    const nodeText = screen.getByText('n1');
    expect(rackText).toBeInTheDocument();
    expect(nodeText).toBeInTheDocument();

    // Build path: assert the data flowing into the tree carries rawId.
    // We do this by reading the rendered DOM data via test ids — but
    // since we don't render rawId directly, we fall back to invoking
    // the context menu programmatically.
    nodeText.parentElement?.dispatchEvent(
      new MouseEvent('contextmenu', { bubbles: true, cancelable: true }),
    );
    const node = captured.find((c) => c.type === 'Node');
    expect(node?.id).toBe('node-n1');
    expect(node?.rawId).toBe('n1');
  });

  it('renders rack nodes when rack.nodes is the NodeView[] envelope', () => {
    const rack = {
      id: 'r1',
      name: 'Rack 1',
      // The recursive>=1 shape returned by `http_list_racks`.
      nodes: [
        {
          id: 'n1',
          rack_id: 'r1',
          host: '127.0.0.1',
          ssh_user: '',
          ssh_port: 22,
          has_server: false,
          stores: [],
        },
      ] as any,
    };
    // If the inflated NodeView is passed as `label`, React throws
    // error #31 and the render fails.
    expect(() =>
      render(<Sidebar racks={[rack]} />, { wrapper }),
    ).not.toThrow();
    expect(screen.getByText('n1')).toBeInTheDocument();
  });
});
