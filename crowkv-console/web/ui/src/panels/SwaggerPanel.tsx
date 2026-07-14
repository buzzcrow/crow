// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { CrowKVServerView } from '../types';

interface SwaggerPanelProps {
  /** Node id whose crowkv-server OpenAPI doc to load. Empty = no node picked. */
  nodeId: string;
  /** API prefix (default "/api"). */
  apiPrefix?: string;
  /** Server views for resolving the selected node's management URL. */
  servers?: CrowKVServerView[];
}

/**
 * Embedded Swagger UI. Hosts an iframe pointing at the offline Swagger bundle
 * served by crowkv-web, targeting the selected node's proxied OpenAPI doc.
 * Loaded lazily by the inspector so initial page load is not blocked.
 *
 * A header bar above the iframe shows which node the API docs are loaded
 * from and its management URL, so the user always knows the target.
 */
export function SwaggerPanel({ nodeId, apiPrefix = '/api', servers = [] }: SwaggerPanelProps) {
  if (!nodeId) {
    return (
      <div className="tw-flex tw-items-center tw-justify-center tw-h-full tw-text-sm tw-text-muted tw-px-6 tw-text-center">
        No node is available to load an OpenAPI document.
      </div>
    );
  }

  const server = servers.find((s) => s.node_id === nodeId);
  const mgmtUrl = server?.process?.mgmt_url || '';
  const specUrl = new URL(`${apiPrefix}/nodes/${nodeId}/openapi.json`, window.location.origin).toString();
  const params = new URLSearchParams({ url: specUrl });
  if (mgmtUrl) params.set('serverUrl', mgmtUrl);
  const iframeUrl = `${apiPrefix}/swagger/index.html?${params.toString()}`;

  return (
    <iframe
      key={nodeId}
      title={`Swagger UI for ${nodeId}`}
      src={iframeUrl}
      className="tw-w-full tw-h-full tw-border-0 tw-bg-white"
    />
  );
}
