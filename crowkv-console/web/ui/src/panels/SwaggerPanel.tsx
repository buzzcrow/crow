interface SwaggerPanelProps {
  /** Node id whose crowkv-server OpenAPI doc to load. Empty = no node picked. */
  nodeId: string;
  /** API prefix (default "/api"). */
  apiPrefix?: string;
}

/**
 * Embedded Swagger UI. Hosts an iframe pointing at the offline Swagger bundle
 * served by crowkv-web, targeting the selected node's proxied OpenAPI doc.
 * Loaded lazily by the inspector so initial page load is not blocked.
 */
export function SwaggerPanel({ nodeId, apiPrefix = '/api' }: SwaggerPanelProps) {
  if (!nodeId) {
    return (
      <div className="tw-flex tw-items-center tw-justify-center tw-h-full tw-text-sm tw-text-muted tw-px-6 tw-text-center">
        No node is available to load an OpenAPI document.
      </div>
    );
  }

  const specUrl = new URL(`${apiPrefix}/nodes/${nodeId}/openapi.json`, window.location.origin).toString();
  const url = `${apiPrefix}/swagger/index.html?url=${encodeURIComponent(specUrl)}`;

  return (
    <iframe
      key={nodeId}
      title={`Swagger UI for ${nodeId}`}
      src={url}
      className="tw-w-full tw-h-full tw-border-0 tw-bg-white"
    />
  );
}
