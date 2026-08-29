// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

/**
 * Host-facing entrypoint for embedding the CrowDB Storage Console.
 *
 * Usage from a host React app:
 *   import { CrowdbConsole } from 'crowdb-console';
 *   <CrowdbConsole apiPrefix="/storage/crowdb-kv/api" readonly />
 */
export { default as CrowdbConsole, type CrowdbConsoleProps } from './App';
export { ViewMode } from './types';
export type { ActivityLogEntry } from './types';
export type { SelectedEntity } from './contexts/SelectionContext';
