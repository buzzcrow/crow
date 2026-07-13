use clap::Subcommand;
use std::path::PathBuf;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;
use crowkv_client::{ClientConfig, CrowkvClient, GetOutcome, ReadMode};

#[derive(Subcommand, Debug)]
pub enum KvVerb {
    /// Put a key/value. Bytes are taken as UTF-8 from the CLI; use
    /// `--value-file <path>` for binary payloads.
    Put {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        key: String,
        #[arg(long, conflicts_with = "value_file")]
        value: Option<String>,
        #[arg(long)]
        value_file: Option<PathBuf>,
        /// Optional client id for idempotency tracking. Defaults to 0.
        #[arg(long, default_value_t = 0)]
        client_id: u64,
        /// Optional client sequence for idempotency. Defaults to 0.
        #[arg(long, default_value_t = 0)]
        seq: u64,
    },
    /// Get a single key. Prints the value as UTF-8 (lossy) by default;
    /// use `--hex` to dump a hex-encoded payload.
    Get {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        key: String,
        #[arg(long)]
        hex: bool,
    },
    /// Delete a key. No-op (`not found`) is reported but not an error.
    Delete {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        key: String,
        #[arg(long, default_value_t = 0)]
        client_id: u64,
        #[arg(long, default_value_t = 0)]
        seq: u64,
    },
    /// Prefix scan. Prints up to `--limit` matching key/value rows; when
    /// more keys exist past the limit a `(truncated: ...)` note is written
    /// to stderr (raise `--limit` to see them).
    List {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Alias for `list` (same `--limit` truncation contract).
    Scan {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
}

pub async fn run_kv_verb(cli: &Cli, verb: KvVerb) -> ExitCode {
    match verb {
        KvVerb::Put {
            store_id,
            group_id,
            key,
            value,
            value_file,
            client_id,
            seq,
        } => {
            kv_put(
                cli,
                store_id,
                group_id,
                KvPutArgs {
                    key: &key,
                    value,
                    value_file,
                    client_id,
                    seq,
                },
            )
            .await
        }
        KvVerb::Get {
            store_id,
            group_id,
            key,
            hex,
        } => kv_get(cli, store_id, group_id, &key, hex).await,
        KvVerb::Delete {
            store_id,
            group_id,
            key,
            client_id,
            seq,
        } => kv_delete(cli, store_id, group_id, &key, client_id, seq).await,
        KvVerb::List {
            store_id,
            group_id,
            prefix,
            limit,
        }
        | KvVerb::Scan {
            store_id,
            group_id,
            prefix,
            limit,
        } => kv_scan(cli, store_id, group_id, &prefix, limit).await,
    }
}

struct KvPutArgs<'a> {
    key: &'a str,
    value: Option<String>,
    value_file: Option<PathBuf>,
    client_id: u64,
    seq: u64,
}

/// Resolve the group's current leader endpoint via the console (the CLI
/// never talks to a `crowkv-server` mgmt API directly -- see
/// `crate::utils::client::console_client`), then hand it to a fresh
/// [`CrowkvClient`] pre-seeded with that endpoint via `seed_leader`. This
/// keeps the CLI's existing console-routed discovery unchanged while
/// gaining `CrowkvClient`'s retry/backoff and connection pooling on the
/// actual RPC call (`doc/plan-client.md` §5 C1-C3). An empty mgmt-seed list
/// is fine here: the CLI is a one-shot process that already knows the
/// leader; `CrowkvClient` only falls back to polling `/topology` if that
/// seeded endpoint later returns `NotLeaderHint` or a transport error.
async fn resolve_kv_client(cli: &Cli, store_id: u64, group_id: u64) -> Result<CrowkvClient, ExitCode> {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return Err(c),
    };
    let endpoint = match client.resolve_endpoint(store_id, group_id).await {
        Ok(info) => info.grpc_url,
        Err(e) => {
            eprintln!("error: resolve endpoint for store {store_id} group {group_id}: {e}");
            return Err(ExitCode::from(2));
        }
    };
    let kv = CrowkvClient::new(ClientConfig::new(Vec::new()));
    kv.seed_leader(store_id, group_id, endpoint);
    Ok(kv)
}

async fn kv_put(cli: &Cli, store_id: u64, group_id: u64, args: KvPutArgs<'_>) -> ExitCode {
    let value_bytes = match (args.value, args.value_file) {
        (Some(v), None) => v.into_bytes(),
        (None, Some(p)) => match std::fs::read(&p) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("error: read --value-file {}: {e}", p.display());
                return ExitCode::from(1);
            }
        },
        (None, None) => {
            eprintln!("error: --value or --value-file is required");
            return ExitCode::from(1);
        }
        (Some(_), Some(_)) => unreachable!("clap conflicts_with"),
    };
    let client = match resolve_kv_client(cli, store_id, group_id).await {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client
        .put(
            store_id,
            group_id,
            args.key.as_bytes(),
            &value_bytes,
            Some((args.client_id, args.seq)),
        )
        .await
    {
        Ok(out) => {
            if cli.json {
                return print_json(&serde_json::json!({"ok": true, "revision": out.revision}));
            }
            // Pre-A12 output included `req=<request_id>`; the
            // console doesn't expose that field, so the printed
            // line drops it. Operators relying on the value should
            // switch to --json.
            println!("ok: rev={}", out.revision);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: put: {e}");
            ExitCode::from(2)
        }
    }
}

async fn kv_get(cli: &Cli, store_id: u64, group_id: u64, key: &str, hex_out: bool) -> ExitCode {
    let client = match resolve_kv_client(cli, store_id, group_id).await {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client
        .get(store_id, group_id, key.as_bytes(), ReadMode::Linearizable, None)
        .await
    {
        Ok(GetOutcome::NotFound) => {
            if cli.json {
                return print_json(&serde_json::json!({"found": false}));
            }
            println!("(not found)");
            ExitCode::from(3)
        }
        Ok(GetOutcome::Found { value, revision }) => {
            let hex_value = hex::encode(&value);
            if cli.json {
                return print_json(&serde_json::json!({
                    "found": true,
                    "revision": revision,
                    "value_hex": hex_value,
                    "value_utf8": String::from_utf8_lossy(&value).to_string(),
                }));
            }
            if hex_out {
                println!("{hex_value}");
            } else {
                use std::io::Write;
                let mut sink = std::io::stdout().lock();
                let _ = sink.write_all(&value);
                let _ = sink.write_all(b"\n");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: get: {e}");
            ExitCode::from(2)
        }
    }
}

async fn kv_delete(cli: &Cli, store_id: u64, group_id: u64, key: &str, client_id: u64, seq: u64) -> ExitCode {
    let client = match resolve_kv_client(cli, store_id, group_id).await {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client
        .delete(store_id, group_id, key.as_bytes(), Some((client_id, seq)))
        .await
    {
        Ok(out) => {
            if cli.json {
                return print_json(&serde_json::json!({"ok": true, "revision": out.revision}));
            }
            println!("ok: rev={}", out.revision);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: delete: {e}");
            ExitCode::from(2)
        }
    }
}

async fn kv_scan(cli: &Cli, store_id: u64, group_id: u64, prefix: &str, limit: u32) -> ExitCode {
    let client = match resolve_kv_client(cli, store_id, group_id).await {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client
        .scan(
            store_id,
            group_id,
            prefix.as_bytes(),
            limit,
            ReadMode::Linearizable,
        )
        .await
    {
        Ok(out) => {
            if cli.json {
                let items: Vec<serde_json::Value> = out
                    .items
                    .iter()
                    .map(|(key, value)| {
                        serde_json::json!({
                            "key_utf8": String::from_utf8_lossy(key).to_string(),
                            "value_utf8": String::from_utf8_lossy(value).to_string(),
                            "key_hex": hex::encode(key),
                            "value_hex": hex::encode(value),
                        })
                    })
                    .collect();
                return print_json(&serde_json::json!({
                    "items": items,
                    "truncated": out.truncated,
                }));
            }
            for (key, value) in &out.items {
                println!(
                    "{}\t{}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(value)
                );
            }
            if out.truncated {
                eprintln!("(truncated: more keys exist past --limit {limit})");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: scan/list: {e}");
            ExitCode::from(2)
        }
    }
}
