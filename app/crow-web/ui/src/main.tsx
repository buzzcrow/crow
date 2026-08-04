// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import React from "react";
import ReactDOM from "react-dom/client";
import App, { type CrowConsoleProps } from "./App";
import { setApiBase } from "./api";
import { ViewMode } from "./types";
import "./index.css";

/**
 * Parse embedding props from the URL query string for the standalone mount.
 * This mirrors `CrowConsoleProps` so a reverse-proxied or embedded
 * deployment can be configured without a rebuild:
 *   ?apiPrefix=/proxy/api&readonly=1&disableModules=kv,swagger&view=Physical
 */
function propsFromQuery(search: string): CrowConsoleProps {
  const q = new URLSearchParams(search);
  const props: CrowConsoleProps = {};

  const apiPrefix = q.get("apiPrefix");
  if (apiPrefix) props.apiPrefix = apiPrefix;

  if (q.get("readonly") === "1" || q.get("readonly") === "true") props.readonly = true;

  const disabled = (q.get("disableModules") || "")
    .split(",")
    .map((m) => m.trim())
    .filter(Boolean);
  if (disabled.length > 0) {
    props.modules = Object.fromEntries(disabled.map((m) => [m, false]));
  }

  const view = q.get("view");
  if (view === "Physical") props.initialViewMode = ViewMode.Physical;
  else if (view === "Logical") props.initialViewMode = ViewMode.Logical;

  const nodeId = q.get("nodeId");
  if (nodeId) props.initialNodeId = nodeId;

  return props;
}

const props = propsFromQuery(window.location.search);
// Re-root all data-plane traffic before the first render/poll fires.
setApiBase(props.apiPrefix);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App {...props} />
  </React.StrictMode>,
);
