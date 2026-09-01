// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `kv put/get/delete/scan/snapshot` command handlers — data-plane.

use std::process::ExitCode;

use clap::Subcommand;
use crowdb_kv_client::GetOutcome;

use crate::commands::{op_context, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum KvDataVerb {
    Put {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 'k', long)]
        key: String,
        #[arg(short = 'v', long)]
        value: String,
    },
    Get {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 'k', long)]
        key: String,
    },
    Delete {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 'k', long)]
        key: String,
    },
    Scan {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 'P', long, default_value = "")]
        prefix: String,
        #[arg(short = 'l', long, default_value_t = 100)]
        limit: u32,
    },
    #[command(subcommand)]
    Snapshot(SnapshotVerb),
}

#[derive(Subcommand, Debug)]
pub enum SnapshotVerb {
    Create {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
    },
    List {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
    },
    Release {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 'h', long)]
        handle: String,
    },
}

#[allow(clippy::too_many_lines)]
pub async fn run_kv_data_verb(cli: &Cli, verb: KvDataVerb) -> ExitCode {
    match verb {
        KvDataVerb::Put {
            store,
            group,
            key,
            value,
        } => {
            let (store_id, group_id) = match parse_store_group(&store, &group) {
                Ok(ids) => ids,
                Err(c) => return c,
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_data::put(
                &ctx,
                store_id,
                group_id,
                key.as_bytes(),
                value.as_bytes(),
                None,
            )
            .await
            {
                Ok(outcome) => {
                    if cli.json {
                        return print_json(
                            cli,
                            &serde_json::json!({"revision": outcome.revision, "request_id": outcome.request_id}),
                        );
                    }
                    println!("put ok (revision {})", outcome.revision);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: put: {e}");
                    ExitCode::from(2)
                }
            }
        }
        KvDataVerb::Get { store, group, key } => {
            let (store_id, group_id) = match parse_store_group(&store, &group) {
                Ok(ids) => ids,
                Err(c) => return c,
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_data::get(&ctx, store_id, group_id, key.as_bytes()).await {
                Ok(GetOutcome::Found { value, revision }) => {
                    if cli.json {
                        return print_json(
                            cli,
                            &serde_json::json!({
                                "found": true,
                                "value": String::from_utf8_lossy(&value),
                                "revision": revision,
                            }),
                        );
                    }
                    println!(
                        "found: {} (revision {})",
                        String::from_utf8_lossy(&value),
                        revision
                    );
                    ExitCode::SUCCESS
                }
                Ok(GetOutcome::NotFound) => {
                    if cli.json {
                        return print_json(cli, &serde_json::json!({"found": false}));
                    }
                    println!("not found");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: get: {e}");
                    ExitCode::from(2)
                }
            }
        }
        KvDataVerb::Delete { store, group, key } => {
            let (store_id, group_id) = match parse_store_group(&store, &group) {
                Ok(ids) => ids,
                Err(c) => return c,
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_data::delete(&ctx, store_id, group_id, key.as_bytes(), None)
                .await
            {
                Ok(_) => {
                    if !cli.json {
                        println!("deleted");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: delete: {e}");
                    ExitCode::from(2)
                }
            }
        }
        KvDataVerb::Scan {
            store,
            group,
            prefix,
            limit,
        } => {
            let (store_id, group_id) = match parse_store_group(&store, &group) {
                Ok(ids) => ids,
                Err(c) => return c,
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_data::scan(
                &ctx,
                store_id,
                group_id,
                prefix.as_bytes(),
                &[],
                limit,
            )
            .await
            {
                Ok(outcome) => {
                    if cli.json {
                        let items: Vec<_> = outcome
                            .items
                            .iter()
                            .map(|(k, v)| {
                                serde_json::json!({
                                    "key": String::from_utf8_lossy(k).to_string(),
                                    "value": String::from_utf8_lossy(v).to_string(),
                                })
                            })
                            .collect();
                        return print_json(
                            cli,
                            &serde_json::json!({"items": items, "truncated": outcome.truncated}),
                        );
                    }
                    for (k, v) in &outcome.items {
                        println!("{}  {}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: scan: {e}");
                    ExitCode::from(2)
                }
            }
        }
        KvDataVerb::Snapshot(sv) => run_snapshot_verb(cli, sv).await,
    }
}

async fn run_snapshot_verb(cli: &Cli, verb: SnapshotVerb) -> ExitCode {
    match verb {
        SnapshotVerb::Create { store, group } => {
            let (store_id, group_id) = match parse_store_group(&store, &group) {
                Ok(ids) => ids,
                Err(c) => return c,
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_data::create_snapshot(&ctx, store_id, group_id).await {
                Ok(resp) => {
                    if cli.json {
                        return print_json(
                            cli,
                            &serde_json::json!({"snapshot_handle": resp.snapshot_handle}),
                        );
                    }
                    println!("snapshot created: handle {}", resp.snapshot_handle);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: create snapshot: {e}");
                    ExitCode::from(2)
                }
            }
        }
        SnapshotVerb::List { store, group } => {
            let (store_id, group_id) = match parse_store_group(&store, &group) {
                Ok(ids) => ids,
                Err(c) => return c,
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_data::list_snapshots(&ctx, store_id, group_id).await {
                Ok(snapshots) => {
                    if cli.json {
                        return print_json(cli, &snapshots);
                    }
                    for s in &snapshots {
                        println!("handle {} slot {}", s.snapshot_handle, s.at_slot);
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: list snapshots: {e}");
                    ExitCode::from(2)
                }
            }
        }
        SnapshotVerb::Release { store, group, handle } => {
            let (store_id, group_id) = match parse_store_group(&store, &group) {
                Ok(ids) => ids,
                Err(c) => return c,
            };
            let snapshot_handle: u64 = match handle.parse() {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("error: invalid snapshot handle: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_data::release_snapshot(
                &ctx,
                store_id,
                group_id,
                snapshot_handle,
            )
            .await
            {
                Ok(_) => {
                    if !cli.json {
                        println!("released snapshot {snapshot_handle}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: release snapshot: {e}");
                    ExitCode::from(2)
                }
            }
        }
    }
}

fn parse_store_group(store: &str, group: &str) -> Result<(u64, u64), ExitCode> {
    let store_id: u64 = store.parse().map_err(|e| {
        eprintln!("error: invalid store id: {e}");
        ExitCode::from(1)
    })?;
    let group_id: u64 = group.parse().map_err(|e| {
        eprintln!("error: invalid group id: {e}");
        ExitCode::from(1)
    })?;
    Ok((store_id, group_id))
}
