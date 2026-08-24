// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

fn main() {
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
