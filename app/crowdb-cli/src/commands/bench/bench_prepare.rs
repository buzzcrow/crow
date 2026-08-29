// Copyright 2026-present Gian <crow.db@outlook.com>

use std::process::ExitCode;

use crate::bench::handle::{ClusterHandle, DeployKind};
use crate::bench::workload::{format_key, value_for};
use crowdb_kv_client::{ClientConfig, CrowdbClient};

/// `bench prepare` — pre-populate keys into a deployed cluster.
pub(crate) async fn bench_prepare(args: super::PrepareArgs, json: bool) -> ExitCode {
    let handle = match ClusterHandle::load(&args.target) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    if handle.kind != DeployKind::Kv {
        eprintln!(
            "error: prepare is only applicable to kind=kv (got kind={})",
            handle.kind.label()
        );
        return ExitCode::from(2);
    }
    if args.keys == 0 {
        if json {
            return crate::utils::print_json(&serde_json::json!({
                "target": handle.name,
                "keys": 0,
                "errors": 0,
                "ms": 0,
            }));
        }
        println!("nothing to prepare (keys=0)");
        return ExitCode::SUCCESS;
    }

    let mut client_cfg = ClientConfig::new(Vec::new());
    client_cfg.enable_nagle = handle.tunables.enable_nagle;
    client_cfg.quickack = handle.tunables.quickack;
    client_cfg.event_write = handle.tunables.event_write;
    client_cfg.send_queue_capacity = handle.tunables.send_queue_capacity;
    let client = CrowdbClient::new(client_cfg);
    client.seed_leader(handle.store_id, handle.group_id, handle.leader_endpoint.clone());

    // Optional value-size mix.
    let mix = if let Some(ref spec) = args.value_size_mix {
        match crate::bench::workload::ValueSizeMix::parse(spec) {
            Ok(m) => Some(m),
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::from(2);
            }
        }
    } else {
        None
    };

    println!("preparing {} keys (value_size={})...", args.keys, args.value_size);
    let _ = std::io::Write::flush(&mut std::io::stdout());

    let start = std::time::Instant::now();
    let mut errors: u64 = 0;
    for id in 0..args.keys {
        let key = format_key(id);
        let vsize = mix.as_ref().map_or(args.value_size, |m| m.size_for(id));
        let value = value_for(id, vsize);
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            match client
                .put(handle.store_id, handle.group_id, &key, &value, None)
                .await
            {
                Ok(_) => break,
                Err(crowdb_kv_client::Error::NotLeader { .. }) if attempts < 8 => {}
                Err(_) => {
                    errors += 1;
                    break;
                }
            }
        }
    }
    let ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

    if json {
        return crate::utils::print_json(&serde_json::json!({
            "target": handle.name,
            "keys": args.keys,
            "errors": errors,
            "ms": ms,
        }));
    }
    println!("prepared {} keys in {ms}ms ({errors} errors)", args.keys);
    ExitCode::SUCCESS
}
