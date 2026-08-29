// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tiny monotonic id generator for client-only ephemeral keys (toasts,
//! activity entries). Avoids a uuid dependency; ids are unique per session.

let counter = 0;

/** Return a process-unique id string. */
export function nextId(prefix = 'id'): string {
  counter += 1;
  return `${prefix}-${Date.now().toString(36)}-${counter.toString(36)}`;
}
