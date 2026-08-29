// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Build script — link isa-l (erasure coding backend).
//!
//! isa-l is provided by the pixi environment (conda-forge). The library
//! and headers are under `.pixi/envs/default/{lib,include}/isa-l/`. We
//! rely on the pixi env being active (CARGO_MANIFEST_DIR-relative lookup
//! of `.pixi`) so no system pkg-config is needed.

fn main() {
    // The pixi env lives at the workspace root, three levels up from this
    // crate (lib/crowdb-common/rust → lib/crowdb-common → lib → workspace root).
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = std::path::Path::new(&manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .expect("workspace root");

    let pixi_lib = workspace_root.join(".pixi/envs/default/lib");
    let pixi_include = workspace_root.join(".pixi/envs/default/include");

    println!("cargo:rustc-link-search=native={}", pixi_lib.display());
    println!("cargo:rustc-link-lib=dylib=isal");
    println!("cargo:rerun-if-changed=build.rs");

    // Re-export the include path for any downstream FFI references.
    println!("cargo:include={}", pixi_include.display());
}
