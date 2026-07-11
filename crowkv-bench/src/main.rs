//! `crowkv-bench` — `CrowKV` benchmark and load test tool.
//!
//! Runs performance workloads against a `crowkv-server` instance or cluster.
//! Real content lands in P4 (G4 milestone) with benchmark/load-test wiring.
//! Key work: performance workloads, load generation, metrics collection.

fn main() {
    let _guards = crowkv::common::logging::init_file_logging("log", "crowkv-bench").expect("failed to initialize crowkv-bench logging");
    tracing::warn!("crowkv-bench is not yet implemented; next step: implement P4 benchmark/load-test wiring");
    std::process::exit(1);
}
