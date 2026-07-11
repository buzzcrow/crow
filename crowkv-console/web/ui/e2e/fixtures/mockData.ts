/**
 * Realistic but small mock cluster used by Playwright + Lighthouse runs so
 * the SPA can fully hydrate without a live backend.
 */
export const mockRacks = [
  { id: 'rack-a', name: 'Rack A', nodes: ['node-a1', 'node-a2'] },
  { id: 'rack-b', name: 'Rack B', nodes: ['node-b1'] },
];

const ssh = { type: 'KeyDefault', user: 'crowkv' } as const;

export const mockNodes = [
  {
    id: 'node-a1',
    rack_id: 'rack-a',
    host: '10.0.0.11',
    ssh,
    server: {
      mgmt_url: 'http://10.0.0.11:9920',
      grpc_url: 'http://10.0.0.11:9921',
      pid: 1234,
      state: 'Running',
      health: 'Up',
      last_seen_ms: Date.now(),
    },
  },
  {
    id: 'node-a2',
    rack_id: 'rack-a',
    host: '10.0.0.12',
    ssh,
    server: {
      mgmt_url: 'http://10.0.0.12:9920',
      grpc_url: 'http://10.0.0.12:9921',
      pid: 1235,
      state: 'Running',
      health: 'Up',
      last_seen_ms: Date.now(),
    },
  },
  {
    id: 'node-b1',
    rack_id: 'rack-b',
    host: '10.0.0.21',
    ssh,
    server: {
      mgmt_url: 'http://10.0.0.21:9920',
      grpc_url: 'http://10.0.0.21:9921',
      pid: 1236,
      state: 'Running',
      health: 'Up',
      last_seen_ms: Date.now(),
    },
  },
];

export const mockStores = [
  {
    store_id: 'orders',
    name: 'Orders',
    nodes: ['node-a1', 'node-a2', 'node-b1'],
    groups: [
      { group_id: 'g0', leader: 'r0', health: 'Healthy', replica_count: 3 },
      { group_id: 'g1', leader: 'r1', health: 'Healthy', replica_count: 3 },
    ],
  },
  {
    store_id: 'users',
    name: 'Users',
    nodes: ['node-a1', 'node-b1'],
    groups: [{ group_id: 'g0', leader: 'r0', health: 'Healthy', replica_count: 2 }],
  },
];
