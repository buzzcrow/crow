// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

fn main() {
    tonic_build::configure()
        // Map `bytes` proto fields to `bytes::Bytes` so clones are O(1)
        // ref-count bumps instead of O(n) heap allocate + memcpy.
        // `AcceptedValue.payload` — Paxos Accept-fanout payload cloned
        // across N peers. KV request/response `bytes` fields — client
        // batch retry loop and server-side scan item construction.
        .bytes([
            "crowkv.rpc.AcceptedValue.payload",
            "crowkv.rpc.KvSetRequest.key",
            "crowkv.rpc.KvSetRequest.value",
            "crowkv.rpc.KvGetRequest.key",
            "crowkv.rpc.KvDeleteRequest.key",
            "crowkv.rpc.KvBatchItem.key",
            "crowkv.rpc.KvBatchItem.value",
            "crowkv.rpc.KvResponse.value",
            "crowkv.rpc.KvScanRequest.prefix",
            "crowkv.rpc.KvScanRequest.start_after",
            "crowkv.rpc.KvScanItem.key",
            "crowkv.rpc.KvScanItem.value",
        ])
        .type_attribute(".", "#[allow(clippy::must_use_candidate)]")
        .compile_protos(
            &["src/rpc/proto/pxos.proto", "src/rpc/proto/kv.proto"],
            &["src/rpc/proto"],
        )
        .expect("failed to compile proto files");

    // Emit rpath for the pixi/conda lib directory so that libspdlog and
    // friends are found at runtime when the C++ engine is built with
    // CROW_TREE_HAVE_SPDLOG.
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        let lib_dir = std::path::Path::new(&prefix).join("lib");
        if lib_dir.join("libspdlog.dylib").is_file() || lib_dir.join("libspdlog.so").is_file() {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        }
    }
    println!("cargo:rerun-if-env-changed=CONDA_PREFIX");
}
