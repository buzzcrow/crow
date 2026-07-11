fn main() {
    tonic_build::configure()
        .compile_protos(&["src/rpc/proto/pxos.proto", "src/rpc/proto/kv.proto"], &["src/rpc/proto"])
        .expect("failed to compile proto files");
}
