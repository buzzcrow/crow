fn main() {
    tonic_build::configure()
        .compile_protos(&["src/rpc/proto/classic_paxos.proto"], &["src/rpc/proto"])
        .expect("failed to compile proto files");
}
