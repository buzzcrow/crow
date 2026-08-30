// Copyright 2026-present Gian <crow.db@outlook.com>

use std::process::ExitCode;
use std::time::Duration;

use crate::bench::handle::{ClusterHandle, DeployKind};
use crate::bench::target::kv::wait_for_re_election;

/// `bench clean` — wipe user data on every node of a deployed cluster,
/// keeping group0 sysdata + store/group/replica topology intact. The
/// cluster re-elects a leader after the wipe; this verb waits for
/// re-election + health before returning.
pub(crate) async fn bench_clean(args: super::CleanArgs, json: bool) -> ExitCode {
    let handle = match ClusterHandle::load(&args.target) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    if handle.kind != DeployKind::Kv {
        eprintln!(
            "error: clean is only applicable to kind=kv (got kind={})",
            handle.kind.label()
        );
        return ExitCode::from(2);
    }

    // Safety banner — the deliberately-non-trivial flow (per-node
    // endpoint + banner) makes accidental data loss hard.
    println!("wiping user data on cluster '{}', group0 preserved", handle.name);
    let _ = std::io::Write::flush(&mut std::io::stdout());

    // Cluster-busy probe: a test put against the old leader. If a
    // `bench run` is in flight, the put either times out (heavy load)
    // or the leader has stepped down (no leader). Either way, reject
    // before wiping. The probe key is deleted immediately after so it
    // does not contaminate a clean cluster.
    if let Err(e) = cluster_busy_probe(&handle).await {
        eprintln!("error: {e}");
        return ExitCode::from(2);
    }

    // Fan out wipe_user_data to every node's mgmt URL.
    let mut ok = 0u32;
    let mut no_op = 0u32;
    let mut failures: Vec<(String, String)> = Vec::new();
    for url in &handle.node_mgmt_urls {
        match wipe_one_node(url, handle.store_id, handle.group_id).await {
            Ok(r) => {
                if r.accepted {
                    ok += 1;
                } else {
                    no_op += 1;
                }
            }
            Err(e) => failures.push((url.clone(), e)),
        }
    }

    if !failures.is_empty() {
        eprintln!("partial wipe — {} node(s) failed:", failures.len());
        for (url, e) in &failures {
            eprintln!("  {url}: {e}");
        }
        eprintln!("teardown + redeploy required (no automatic rollback)");
        return ExitCode::from(2);
    }

    // Wait for re-election + health: poll every node's /topology
    // until all agree on a non-zero leader_id.
    let new_leader = match wait_for_re_election(
        &handle.node_mgmt_urls,
        handle.store_id,
        handle.group_id,
        Duration::from_secs(20),
    )
    .await
    {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: wipe succeeded but re-election wait failed: {e}");
            return ExitCode::from(2);
        }
    };

    if json {
        return crate::utils::print_json(&serde_json::json!({
            "target": handle.name,
            "wiped": ok,
            "no_op": no_op,
            "new_leader": new_leader,
        }));
    }
    println!("wiped {ok} node(s) (no-op on {no_op}), new leader {new_leader}, cluster healthy");
    ExitCode::SUCCESS
}

/// Wipe user data on one node via the mgmt HTTP endpoint.
async fn wipe_one_node(
    mgmt_url: &str,
    store_id: u64,
    group_id: u64,
) -> Result<crowdb_protocol::WipeResult, String> {
    use crowdb_kv_client::{ClientConfig, CrowdbKvClient, KVClusterAdmin, KVClusterMetaClient};
    // The wipe does not touch group0 meta, so the meta client is a
    // throwaway — KVClusterAdmin requires it by construction.
    let client = CrowdbKvClient::new(ClientConfig::new(Vec::new()));
    let meta = KVClusterMetaClient::new(client);
    let admin = KVClusterAdmin::new(meta, mgmt_url);
    admin
        .wipe_user_data(store_id, group_id)
        .await
        .map_err(|e| format!("{e}"))
}

/// Probe whether the cluster is busy serving a running bench. A test
/// put against the old leader with a short timeout: if it succeeds,
/// the cluster is idle (the put is deleted immediately); if it times
/// out or fails with no-leader, the cluster is busy or mid-election.
async fn cluster_busy_probe(handle: &ClusterHandle) -> Result<(), String> {
    use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
    let mut cfg = ClientConfig::new(Vec::new());
    cfg.retry.max_retries = 1;
    let client = CrowdbKvClient::new(cfg);
    client.seed_leader(handle.store_id, handle.group_id, handle.leader_endpoint.clone());
    let probe_key = b"__bench_clean_probe__";
    let put = tokio::time::timeout(
        Duration::from_secs(3),
        client.put(handle.store_id, handle.group_id, probe_key, b"ok", None),
    )
    .await;
    match put {
        Ok(Ok(_)) => {
            // Delete the probe key so it does not contaminate the
            // post-wipe key count. Best-effort — a failure here is
            // not fatal (the wipe will clear it anyway).
            let _ = client
                .delete(handle.store_id, handle.group_id, probe_key, None)
                .await;
            Ok(())
        }
        Ok(Err(crowdb_kv_client::Error::NotLeader { .. })) => {
            // No leader — could be a transient election. Allow the
            // wipe (the wipe steps down + re-elects anyway); this is
            // not a "busy" signal, just a no-leader state.
            Ok(())
        }
        Ok(Err(e)) => Err(format!(
            "cluster-busy probe failed (leader not serving): {e}; stop any running bench first"
        )),
        Err(_) => {
            Err("cluster busy — leader did not respond within 3s; stop the running bench first".to_string())
        }
    }
}
