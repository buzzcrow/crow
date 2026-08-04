// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import '@testing-library/jest-dom/vitest';

// Polyfill matchMedia for components that read system theme.
if (!window.matchMedia) {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: (query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }),
  });
}

// Stub URL.createObjectURL used by export helpers.
if (!URL.createObjectURL) {
  URL.createObjectURL = () => 'blob:stub';
}
if (!URL.revokeObjectURL) {
  URL.revokeObjectURL = () => {};
}
