import { ReactNode } from 'react';
import { Rack, Node, StoreView, ViewMode } from '../types';
import { SelectedEntity } from '../contexts/SelectionContext';

export type CommandCategory = 'Entities' | 'Actions' | 'Views';

export interface CommandItem {
  id: string;
  category: CommandCategory;
  label: string;
  description?: string;
  /** Optional human-readable shortcut hint, e.g. "Cmd+R" */
  shortcut?: string;
  /** Optional icon name (caller renders). Lucide icons handled by the palette. */
  iconName?: string;
  /** Searchable keywords in addition to label/description. */
  keywords?: string[];
  /** Invoked when the user activates the command. */
  handler: (ctx: CommandContext) => void | Promise<void>;
}

export interface CommandContext {
  /** Currently active view mode. */
  viewMode: ViewMode;
  /** Toggle between physical and logical views. */
  toggleViewMode: () => void;
  /** Select an entity (drives the inspector). */
  selectEntity: (entity: SelectedEntity) => void;
  /** Manually refresh the cluster data. */
  refresh: () => void | Promise<void>;
}

export interface BuildCommandsInput {
  racks: Rack[];
  nodes: Node[];
  stores: StoreView[];
}

/**
 * Build the full command list from the current cluster data.
 *
 * The set is rebuilt on every palette open so it stays in sync with polling
 * data; the palette filters this list via fuzzySearch.
 */
export function buildCommands({ racks, nodes, stores }: BuildCommandsInput): CommandItem[] {
  const commands: CommandItem[] = [];

  // Entity navigation: racks
  for (const rack of racks) {
    commands.push({
      id: `nav-rack-${rack.id}`,
      category: 'Entities',
      label: rack.name || rack.id,
      description: `Rack · ${rack.nodes.length} node${rack.nodes.length === 1 ? '' : 's'}`,
      keywords: ['rack', rack.id],
      iconName: 'server',
      handler: ({ selectEntity, viewMode }) => {
        selectEntity({ type: 'Rack', id: rack.id, name: rack.name, viewMode });
      },
    });
  }

  // Entity navigation: nodes
  for (const node of nodes) {
    commands.push({
      id: `nav-node-${node.id}`,
      category: 'Entities',
      label: node.id,
      description: `Node · ${node.host} · rack ${node.rack_id}`,
      keywords: ['node', node.id, node.host, node.rack_id],
      iconName: 'hard-drive',
      handler: ({ selectEntity, viewMode }) => {
        selectEntity({
          type: 'Node',
          id: node.id,
          parentIds: { rackId: node.rack_id },
          viewMode,
        });
      },
    });
  }

  // Entity navigation: stores + groups
  for (const store of stores) {
    commands.push({
      id: `nav-store-${store.store_id}`,
      category: 'Entities',
      label: store.name || store.store_id,
      description: `Store · ${store.groups?.length ?? 0} group(s)`,
      keywords: ['store', store.store_id],
      iconName: 'database',
      handler: ({ selectEntity, viewMode }) => {
        selectEntity({ type: 'Store', id: store.store_id, name: store.name, viewMode });
      },
    });
    for (const group of store.groups || []) {
      commands.push({
        id: `nav-group-${store.store_id}-${group.group_id}`,
        category: 'Entities',
        label: `${store.store_id} / ${group.group_id}`,
        description: `Group · ${group.replica_count} replicas · health ${group.health}`,
        keywords: ['group', group.group_id, store.store_id],
        iconName: 'users',
        handler: ({ selectEntity, viewMode }) => {
          selectEntity({
            type: 'Group',
            id: group.group_id,
            parentIds: { storeId: store.store_id },
            viewMode,
          });
        },
      });
    }
  }

  // View commands
  commands.push({
    id: 'view-toggle',
    category: 'Views',
    label: 'Toggle view (Physical / Logical)',
    description: 'Switch between the infrastructure and cluster trees',
    shortcut: 'V',
    iconName: 'layout-dashboard',
    keywords: ['toggle', 'view', 'physical', 'logical', 'switch'],
    handler: ({ toggleViewMode }) => toggleViewMode(),
  });

  // Action commands
  commands.push({
    id: 'action-refresh',
    category: 'Actions',
    label: 'Refresh cluster data',
    description: 'Trigger an out-of-cycle refresh of all polled data',
    shortcut: 'R',
    iconName: 'refresh-cw',
    keywords: ['refresh', 'reload', 'update'],
    handler: ({ refresh }) => {
      void refresh();
    },
  });

  return commands;
}

/** Lookup table from iconName -> lucide-react component. The palette uses this. */
export type IconRenderer = (name: string | undefined) => ReactNode;
