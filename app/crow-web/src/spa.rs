// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use crate::state::FRONTEND_DIST;
use axum::body::Body;
use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use std::path::{Path as StdPath, PathBuf};

/// SPA fallback handler. Maps the request URI to a file under
/// `FRONTEND_DIST`. Behavior:
///   1. If `dist/<path>` is a regular file, serve it (with a sniffed
///      `Content-Type` from the file extension).
///   2. Else, if `dist/index.html` exists, serve it (HTML5 history
///      routing fallback so deep links into the SPA still work).
///   3. Else, the build is missing — serve a static instructional page
///      explaining how to run `make ui-build`. This keeps
///      `cargo run` usable on machines without a Node toolchain.
pub async fn spa_fallback(uri: Uri) -> Response {
    let dist = StdPath::new(FRONTEND_DIST);

    // Sanitize the request path: strip leading slash, refuse `..`.
    let req_path = uri.path().trim_start_matches('/');
    if req_path.split('/').any(|seg| seg == "..") {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    }

    // 1. Direct file hit under dist/.
    if !req_path.is_empty() {
        let candidate: PathBuf = dist.join(req_path);
        if candidate.is_file() {
            return serve_file(&candidate).await;
        }
    }

    // 2. SPA routing fallback: serve dist/index.html if the build exists.
    let index = dist.join("index.html");
    if index.is_file() {
        return serve_file(&index).await;
    }

    // 3. Build missing. Return instructional fallback (200 so simple
    //    smoke tests can hit `/` without first installing Node).
    Html(FRONTEND_MISSING_HTML).into_response()
}

async fn serve_file(path: &StdPath) -> Response {
    match tokio::fs::read(path).await {
        Ok(bytes) => {
            let ct = guess_content_type(path);
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, ct)
                .body(Body::from(bytes))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

fn guess_content_type(path: &StdPath) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).unwrap_or("") {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ico" => "image/x-icon",
        _ => "application/octet-stream",
    }
}

/// Page shown when the React build output is missing. Embedded as a
/// `&'static str` so `cargo run` works without any filesystem assets.
const FRONTEND_MISSING_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Crow Storage Console &middot; UI not built</title>
    <style>
        body { font-family: ui-monospace, monospace; margin: 32px; max-width: 720px; background: #0b0d10; color: #d8dee9; }
        h1 { font-size: 18px; color: #88c0d0; }
        code, pre { background: #161a1f; border: 1px solid #2e3440; border-radius: 4px; padding: 2px 6px; }
        pre { padding: 10px; overflow-x: auto; }
        a { color: #88c0d0; }
    </style>
</head>
<body>
    <h1>Crow Storage Console &middot; UI not built</h1>
    <p>The React SPA bundle was not found at <code>crow-console/web/ui/dist/</code>.</p>
    <p>Run the build once and reload:</p>
    <pre>make ui-install
make ui-build</pre>
    <p>Or, for development with hot-reload, run the Vite dev server alongside this Axum backend:</p>
    <pre>make ui-dev    # serves http://127.0.0.1:5173 with /api proxy</pre>
    <p>The HTTP API is unaffected; you can still hit
        <a href="/healthz">/healthz</a>,
        <a href="/api/racks">/api/racks</a>,
        <a href="/api/stores">/api/stores</a>,
        <a href="/api/swagger/">/api/swagger/</a>.</p>
</body>
</html>"#;
