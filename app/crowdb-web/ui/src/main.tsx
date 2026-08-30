// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import React from "react";
import ReactDOM from "react-dom/client";
import App, { type CrowdbConsoleProps } from "./App";
import { setApiBase } from "./api";
import { Domain } from "./types";
import "./index.css";

/**
 * Parse embedding props from the URL query string for the standalone mount.
 * This mirrors `CrowdbConsoleProps` so a reverse-proxied or embedded
 * deployment can be configured without a rebuild:
 *   ?apiPrefix=/proxy/api&readonly=1&disableModules=kv&domain=Cluster
 */
function propsFromQuery(search: string): CrowdbConsoleProps {
  const q = new URLSearchParams(search);
  const props: CrowdbConsoleProps = {};

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

  const d = q.get("domain");
  if (d === "Cluster") props.initialDomain = Domain.Cluster;
  else if (d === "KV") props.initialDomain = Domain.KV;
  else if (d === "Chunk") props.initialDomain = Domain.Chunk;

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
