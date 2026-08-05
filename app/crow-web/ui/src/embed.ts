// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

/**
 * Host-facing entrypoint for embedding the Crow Storage Console.
 *
 * Usage from a host React app:
 *   import { CrowConsole } from 'crow-console';
 *   <CrowConsole apiPrefix="/storage/crow-kv/api" readonly />
 */
export { default as CrowConsole, type CrowConsoleProps } from './App';
export { ViewMode } from './types';
export type { ActivityLogEntry } from './types';
export type { SelectedEntity } from './contexts/SelectionContext';
