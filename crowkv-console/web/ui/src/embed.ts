/**
 * Host-facing entrypoint for embedding the CrowKV Console.
 *
 * Usage from a host React app:
 *   import { CrowkvConsole } from 'crowkv-console';
 *   <CrowkvConsole brandLogo={<MyLogo />} customActions={[...]} />
 */
export { default as CrowkvConsole, type CrowkvConsoleProps } from './App';
export type {
  CustomAction,
  CustomPanel,
  ViewMode,
  ThemeMode,
  ActivityLogEntry,
} from './types';
export type { SelectedEntity } from './contexts/SelectionContext';
