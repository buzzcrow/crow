type StorageKey =
  | 'themeMode'
  | 'viewMode'
  | 'favorites'
  | 'recentItems'
  | 'filterPresets'
  | 'selectedNodeId'
  | 'topologyLayout'
  | 'showEdgeLabels';

export const localStorage = {
  get<T>(key: StorageKey, defaultValue: T): T {
    try {
      const item = window.localStorage.getItem(key);
      return item ? JSON.parse(item) : defaultValue;
    } catch (error) {
      console.error(`Error reading ${key} from localStorage:`, error);
      return defaultValue;
    }
  },
  set<T>(key: StorageKey, value: T): void {
    try {
      window.localStorage.setItem(key, JSON.stringify(value));
    } catch (error) {
      console.error(`Error writing ${key} to localStorage:`, error);
    }
  },
  remove(key: StorageKey): void {
    try {
      window.localStorage.removeItem(key);
    } catch (error) {
      console.error(`Error removing ${key} from localStorage:`, error);
    }
  },
};
