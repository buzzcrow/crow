// Compile the crowtree C++ engine (including the C ABI) into a static lib and
// link it into this crate. LZ4 is optional: set CROWTREE_LZ4_LIB=/path/to/dir
// containing liblz4 to enable on-disk compression; otherwise the codec degrades
// to identity (stored raw) and the build needs no system LZ4.
use std::fs;
use std::path::{Path, PathBuf};

fn collect_cc(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        // Engine sources use the .cpp extension (renamed from .cc in the STL
        // rename task); accept both so the crate builds regardless.
        match path.extension().and_then(|s| s.to_str()) {
            Some("cpp") | Some("cc") => out.push(path),
            _ => {}
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // crate dir = crowtree/ffi ; engine root = crowtree
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let engine = manifest
        .parent()
        .ok_or("CARGO_MANIFEST_DIR must have a parent crowtree dir")?
        .to_path_buf();
    let src = engine.join("src");
    let include = engine.join("include");

    let mut files = Vec::new();
    collect_cc(&src, &mut files)?;

    let mut build = cc::Build::new();
    build.cpp(true).std("c++20").include(&include).warnings(false);
    for f in &files {
        build.file(f);
        println!("cargo:rerun-if-changed={}", f.display());
    }

    let have_lz4 = std::env::var("CROWTREE_LZ4_LIB").ok();
    if have_lz4.is_some() {
        build.define("CROWTREE_HAVE_LZ4", "1");
    }
    build.compile("crowtree");

    if let Some(dir) = have_lz4 {
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=dylib=lz4");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
    println!("cargo:rerun-if-changed={}", include.display());
    println!("cargo:rerun-if-env-changed=CROWTREE_LZ4_LIB");
    Ok(())
}
