import { describe, it, expect } from 'vitest';
import { Node, Edge } from 'reactflow';
import { applyLayout } from './layout';

function makeNode(id: string, layer?: number, groupKey?: string): Node {
  return {
    id,
    position: { x: 0, y: 0 },
    data: { layer, groupKey },
  };
}

describe('applyLayout', () => {
  it('hierarchical: groups nodes by layer onto separate rows', () => {
    const nodes: Node[] = [
      makeNode('r1', 0),
      makeNode('r2', 0),
      makeNode('n1', 1),
      makeNode('n2', 1),
      makeNode('n3', 1),
    ];
    const edges: Edge[] = [];
    const result = applyLayout(nodes, edges, { kind: 'hierarchical' });

    const ysByLayer = new Map<string, number>();
    for (const n of result.nodes) ysByLayer.set(n.id, n.position.y);
    expect(ysByLayer.get('r1')).toBe(ysByLayer.get('r2'));
    expect(ysByLayer.get('n1')).toBe(ysByLayer.get('n2'));
    expect(ysByLayer.get('n1')).toBe(ysByLayer.get('n3'));
    expect(ysByLayer.get('r1')).toBeLessThan(ysByLayer.get('n1')!);
  });

  it('grid: clusters nodes by groupKey', () => {
    const nodes: Node[] = [
      makeNode('a1', undefined, 'a'),
      makeNode('a2', undefined, 'a'),
      makeNode('b1', undefined, 'b'),
      makeNode('b2', undefined, 'b'),
    ];
    const result = applyLayout(nodes, [], { kind: 'grid' });

    const yById = new Map(result.nodes.map((n) => [n.id, n.position.y]));
    // Different groups should sit on different y-bands.
    const aYs = ['a1', 'a2'].map((id) => yById.get(id)!);
    const bYs = ['b1', 'b2'].map((id) => yById.get(id)!);
    expect(Math.min(...aYs)).not.toBe(Math.min(...bYs));
  });

  it('force: produces finite positions for every node', () => {
    const nodes: Node[] = ['a', 'b', 'c', 'd'].map((id) => makeNode(id));
    const edges: Edge[] = [
      { id: 'e1', source: 'a', target: 'b' },
      { id: 'e2', source: 'b', target: 'c' },
      { id: 'e3', source: 'c', target: 'd' },
    ];
    const result = applyLayout(nodes, edges, { kind: 'force' });
    expect(result.nodes).toHaveLength(4);
    for (const n of result.nodes) {
      expect(Number.isFinite(n.position.x)).toBe(true);
      expect(Number.isFinite(n.position.y)).toBe(true);
    }
  });
});
