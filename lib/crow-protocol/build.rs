// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::process::Command;

#[allow(clippy::too_many_lines)]
fn main() {
    // ── Flatbuffer schemas (.fbs) → Rust via flatc ──
    // Common control-message schemas for the crow-rpc library (R104). The
    // `.fbs` files live under `src/fbs/` (separate from `src/proto/` which
    // holds protobuf `.proto` files). `common_msg.fbs` includes
    // `ret_code.fbs` and references FBRetCode in accessors, so it is
    // generated with `--gen-all` to inline ret_code into one self-contained
    // file (avoids flatc's cross-file `crate::` glob quirk). The other two
    // are standalone (no includes used).
    let fbs_files = [
        "src/fbs/ret_code.fbs",
        "src/fbs/msg_type.fbs",
        "src/fbs/common_type.fbs",
        "src/fbs/common_msg.fbs",
        "src/fbs/diskio.fbs",
        "src/fbs/diskdb.fbs",
        "src/fbs/kv_consensus.fbs",
        "src/fbs/kv_client.fbs",
        "src/fbs/chunkdb.fbs",
    ];
    for f in &fbs_files {
        println!("cargo:rerun-if-changed={f}");
    }
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR set by cargo"));
    let flatc = find_flatc();
    // msg_type and common_type: standalone, no cross-file references.
    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("-o")
        .arg(&out_dir)
        .arg("src/fbs/msg_type.fbs")
        .arg("src/fbs/common_type.fbs")
        .status()
        .unwrap_or_else(|e| panic!("failed to run flatc at {}: {e}", flatc.display()));
    assert!(
        status.success(),
        "flatc --rust failed for crow-protocol .fbs schemas"
    );
    // common_msg: --gen-all inlines ret_code.fbs so FBRetCode resolves.
    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("--gen-all")
        .arg("-o")
        .arg(&out_dir)
        .arg("src/fbs/common_msg.fbs")
        .status()
        .unwrap_or_else(|e| panic!("failed to run flatc at {}: {e}", flatc.display()));
    assert!(
        status.success(),
        "flatc --rust --gen-all failed for common_msg.fbs"
    );
    // diskio: --gen-all inlines common_type.fbs so FBInt128 resolves.
    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("--gen-all")
        .arg("-o")
        .arg(&out_dir)
        .arg("src/fbs/diskio.fbs")
        .status()
        .unwrap_or_else(|e| panic!("failed to run flatc at {}: {e}", flatc.display()));
    assert!(status.success(), "flatc --rust --gen-all failed for diskio.fbs");
    // diskdb: --gen-all inlines common_type.fbs so FBInt128 resolves.
    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("--gen-all")
        .arg("-o")
        .arg(&out_dir)
        .arg("src/fbs/diskdb.fbs")
        .status()
        .unwrap_or_else(|e| panic!("failed to run flatc at {}: {e}", flatc.display()));
    assert!(status.success(), "flatc --rust --gen-all failed for diskdb.fbs");
    // kv_consensus: --gen-all inlines common_type.fbs so FBInt128 resolves.
    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("--gen-all")
        .arg("-o")
        .arg(&out_dir)
        .arg("src/fbs/kv_consensus.fbs")
        .status()
        .unwrap_or_else(|e| panic!("failed to run flatc at {}: {e}", flatc.display()));
    assert!(
        status.success(),
        "flatc --rust --gen-all failed for kv_consensus.fbs"
    );
    // kv_client: --gen-all inlines common_type.fbs so FBInt128 resolves.
    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("--gen-all")
        .arg("-o")
        .arg(&out_dir)
        .arg("src/fbs/kv_client.fbs")
        .status()
        .unwrap_or_else(|e| panic!("failed to run flatc at {}: {e}", flatc.display()));
    assert!(
        status.success(),
        "flatc --rust --gen-all failed for kv_client.fbs"
    );
    // chunkdb: --gen-all inlines common_type.fbs + diskdb.fbs so
    // FBInt128 + FBSegment resolve.
    let status = Command::new(&flatc)
        .arg("--rust")
        .arg("--gen-all")
        .arg("-o")
        .arg(&out_dir)
        .arg("src/fbs/chunkdb.fbs")
        .status()
        .unwrap_or_else(|e| panic!("failed to run flatc at {}: {e}", flatc.display()));
    assert!(status.success(), "flatc --rust --gen-all failed for chunkdb.fbs");
}

/// Locate the `flatc` schema compiler. Pixi puts it in `$CONDA_PREFIX/bin`;
/// fall back to a pixi env dir relative to the manifest, then to `PATH`.
fn find_flatc() -> PathBuf {
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        let p = PathBuf::from(prefix).join("bin").join("flatc");
        if p.is_file() {
            return p;
        }
    }
    if let Ok(manifest) = std::env::var("CARGO_MANIFEST_DIR") {
        let pixi_env = PathBuf::from(manifest)
            .parent()
            .and_then(|p| p.parent())
            .map(|root| {
                root.join(".pixi")
                    .join("envs")
                    .join("default")
                    .join("bin")
                    .join("flatc")
            });
        if let Some(p) = pixi_env.filter(|p| p.is_file()) {
            return p;
        }
    }
    // Last resort: assume it is on PATH.
    PathBuf::from("flatc")
}
