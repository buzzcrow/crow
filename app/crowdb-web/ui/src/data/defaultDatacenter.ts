// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// UI-only fixed datacenter root. A cluster has one datacenter in
// practice, so this is a presentation constant — no backend entity,
// no API, no config. Multi-datacenter support is deferred to a later
// backend task; when it lands, the tree is refactored to fetch
// datacenters and the nested-grouping shape established here carries
// over unchanged.

export const DEFAULT_DC_ID = 'datacenter';
export const DEFAULT_DC_NAME = 'datacenter';
