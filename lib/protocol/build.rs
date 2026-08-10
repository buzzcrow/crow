// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[allow(clippy::must_use_candidate)]")
        .compile_protos(&["src/proto/diskdb.proto"], &["src/proto"])
        .expect("failed to compile diskdb proto");
}
