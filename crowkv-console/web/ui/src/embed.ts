// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

/**
 * Host-facing entrypoint for embedding the CrowKV Console.
 *
 * Usage from a host React app:
 *   import { CrowkvConsole } from 'crowkv-console';
 *   <CrowkvConsole apiPrefix="/storage/crowkv/api" readonly />
 */
export { default as CrowkvConsole, type CrowkvConsoleProps } from './App';
export { ViewMode } from './types';
export type { ActivityLogEntry } from './types';
export type { SelectedEntity } from './contexts/SelectionContext';
