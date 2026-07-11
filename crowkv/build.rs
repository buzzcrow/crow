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
}
