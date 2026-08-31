// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench kv clean` — wipe user data on every node + wait for
//! re-election. Returns `{ new_leader, wiped_nodes }` so write-
//! regression sub-tests start from a data-empty cluster without a
//! full redeploy.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use crowdb_console_shared::clients::http::ServerClient;

use super::result::CleanResult;
use crate::commands::load_config;
use crate::Cli;

const STORE_ID: u64 = 0;
const GROUP_ID: u64 = 0;

pub async fn run(cli: &Cli) -> ExitCode {
    let config = match load_config(cli) {
        Ok(c) => c,
        Err(e) => return e,
    };

    let mut mgmt_urls: Vec<String> = config.servers.iter().map(|s| s.url.clone()).collect();
    mgmt_urls.sort();
    mgmt_urls.dedup();
    if mgmt_urls.is_empty() {
        eprintln!("bench kv clean: no servers in config");
        return ExitCode::from(2);
    }

    // Wipe user data on every node concurrently.
    let mut wiped = 0u64;
    let mut handles = Vec::with_capacity(mgmt_urls.len());
    for url in &mgmt_urls {
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            let sc = match ServerClient::new(&url) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("bench kv clean: {url}: client build failed: {e}");
                    return false;
                }
            };
            match sc.wipe_user_data(STORE_ID, GROUP_ID).await {
                Ok(r) => r.accepted,
                Err(e) => {
                    eprintln!("bench kv clean: {url}: wipe failed: {e}");
                    false
                }
            }
        }));
    }
    for h in handles {
        if let Ok(true) = h.await {
            wiped += 1;
        }
    }

    eprintln!("bench kv clean: wiped {wiped} nodes, waiting for re-election...");

    // Wait for re-election: poll topology until a leader is found.
    let leader = wait_for_leader(&mgmt_urls, Duration::from_secs(10)).await;
    match &leader {
        Some(l) => eprintln!("bench kv clean: new leader = {l}"),
        None => eprintln!("bench kv clean: WARNING: no leader elected after 10s"),
    }

    let result = CleanResult {
        new_leader: leader.clone().unwrap_or_default(),
        wiped_nodes: wiped,
    };
    crate::commands::print_json(cli, &result)
}

/// Poll `/topology` on every server until a leader for store 0 /
/// group 0 is elected (`leader_id` > 0), or `timeout` elapses.
/// Returns the mgmt URL of the server that reported the leader.
async fn wait_for_leader(mgmt_urls: &[String], timeout: Duration) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() >= deadline {
            return None;
        }
        for url in mgmt_urls {
            let Ok(sc) = ServerClient::new(url) else { continue };
            if let Ok(stores) = sc.topology().await {
                for store in &stores {
                    if store.store_id != STORE_ID {
                        continue;
                    }
                    for group in &store.groups {
                        if group.group_id == GROUP_ID && group.leader_id > 0 {
                            // The leader is this node if leader_id ==
                            // local_replica_id; otherwise find the
                            // remote's endpoint. For the bench purpose,
                            // returning the reporting URL when this node
                            // is the leader is sufficient.
                            if group.leader_id == group.local_replica_id {
                                return Some(url.clone());
                            }
                            for remote in &group.remotes {
                                if remote.id == group.leader_id {
                                    return Some(remote.endpoint.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
