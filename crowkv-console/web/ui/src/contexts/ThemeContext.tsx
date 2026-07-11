import { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { ThemeMode } from '../types';
import { localStorage } from '../utils/localStorage';

const STORAGE_KEY = 'themeMode' as const;

interface ThemeContextType {
  themeMode: ThemeMode;
  setThemeMode: (mode: ThemeMode) => void;
  isDarkMode: boolean;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

interface ThemeProviderProps {
  children: ReactNode;
  initialThemeMode?: ThemeMode;
  /** Custom theme tokens to override defaults */
  customTheme?: Record<string, string>;
}

export function ThemeProvider({ children, initialThemeMode, customTheme }: ThemeProviderProps) {
  const [themeMode, setThemeMode] = useState<ThemeMode>(() => {
    // Use initial prop if provided, otherwise load from storage or default to System
    if (initialThemeMode) return initialThemeMode;
    return localStorage.get<ThemeMode>(STORAGE_KEY, ThemeMode.System);
  });

  const [isDarkMode, setIsDarkMode] = useState<boolean>(false);

  // Apply theme to document
  useEffect(() => {
    const root = document.documentElement;
    const updateTheme = () => {
      let dark: boolean;
      if (themeMode === ThemeMode.System) {
        dark = window.matchMedia('(prefers-color-scheme: dark)').matches;
      } else {
        dark = themeMode === ThemeMode.Dark;
      }

      setIsDarkMode(dark);
      if (dark) {
        root.classList.add('tw-dark');
      } else {
        root.classList.remove('tw-dark');
      }

      // Apply custom theme tokens if provided
      if (customTheme) {
        Object.entries(customTheme).forEach(([key, value]) => {
          root.style.setProperty(`--${key}`, value);
        });
      }
    };

    updateTheme();

    // Listen for system theme changes when in System mode
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    const handleChange = () => {
      if (themeMode === ThemeMode.System) {
        updateTheme();
      }
    };

    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }, [themeMode, customTheme]);

  // Persist theme mode changes to localStorage
  useEffect(() => {
    localStorage.set(STORAGE_KEY, themeMode);
  }, [themeMode]);

  return (
    <ThemeContext.Provider value={{ themeMode, setThemeMode, isDarkMode }}>
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
}
