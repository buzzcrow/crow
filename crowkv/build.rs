// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

fn main() {
    tonic_build::configure()
        // Map only `AcceptedValue.payload` to `bytes::Bytes` so the
        // Paxos Accept-fanout payload can be ref-count cloned across N
        // peers instead of being copied N times into owned `Vec<u8>`s.
        // Other `bytes` proto fields (KV `key` / `value` / `prefix`,
        // etc.) keep the default `Vec<u8>` mapping to avoid rippling
        // type changes through the KV API surface.
        .bytes(["crowkv.rpc.AcceptedValue.payload"])
        .type_attribute(".", "#[allow(clippy::must_use_candidate)]")
        .compile_protos(
            &["src/rpc/proto/pxos.proto", "src/rpc/proto/kv.proto"],
            &["src/rpc/proto"],
        )
        .expect("failed to compile proto files");

    // Emit rpath for the pixi/conda lib directory so that libspdlog and
    // friends are found at runtime when the C++ engine is built with
    // CROWTREE_HAVE_SPDLOG.
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        let lib_dir = std::path::Path::new(&prefix).join("lib");
        if lib_dir.join("libspdlog.dylib").is_file() || lib_dir.join("libspdlog.so").is_file() {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        }
    }
    println!("cargo:rerun-if-env-changed=CONDA_PREFIX");
}
