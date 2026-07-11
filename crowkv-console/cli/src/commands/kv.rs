use clap::Subcommand;
use std::path::PathBuf;
use std::process::ExitCode;

use crate::utils::{
    client::{console_client, resolve_single_target},
    print_json,
};
use crate::Cli;

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
    /// Prefix list. C6: server-side scan is not implemented yet — this
    /// verb returns an explanatory error.
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
    /// Alias for `list` with the same caveats.
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
        KvVerb::Get { store_id, group_id, key, hex } => kv_get(cli, store_id, group_id, &key, hex).await,
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
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.kv_put(store_id, group_id, args.key.as_bytes(), &value_bytes, args.client_id, args.seq).await {
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
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.kv_get(store_id, group_id, key.as_bytes()).await {
        Ok(out) if !out.found => {
            if cli.json {
                return print_json(&serde_json::json!({"found": false}));
            }
            println!("(not found)");
            ExitCode::from(3)
        }
        Ok(out) => {
            // The console populates value_hex unconditionally for
            // found responses; decoding back to bytes is the only way
            // to print a binary value verbatim.
            let hex_value = out.value_hex.clone().unwrap_or_default();
            if cli.json {
                return print_json(&serde_json::json!({
                    "found": true,
                    "revision": out.revision,
                    "value_hex": hex_value,
                    "value_utf8": out.value_utf8.clone().unwrap_or_default(),
                }));
            }
            if hex_out {
                println!("{hex_value}");
            } else {
                use std::io::Write;
                let bytes = hex::decode(&hex_value).unwrap_or_default();
                let mut sink = std::io::stdout().lock();
                let _ = sink.write_all(&bytes);
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
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.kv_delete(store_id, group_id, key.as_bytes(), client_id, seq).await {
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

/// Resolve a store's gRPC `host:port` from the legacy single-server
/// management API. The store's `listen_addr` is `0.0.0.0:N`; we
/// replace the host with the management URL's host so the operator
/// dialing remotely picks up the right interface.
///
/// **Migration note:** the four KV verbs no longer call this — they
/// route through `ConsoleClient` against `crowkv-web`. The bench
/// engine still talks gRPC directly to a single `crowkv-server` for
/// throughput reasons, so it keeps using this helper (and `--server`)
/// until a dedicated `ConsoleClient` bench path lands.
///
/// # Errors
/// Returns a non-zero exit code on transport or decode failure.
pub async fn resolve_kv_endpoint(cli: &Cli, store_id: u64) -> Result<String, ExitCode> {
    use crowkv_console_shared::clients::http::ServerClient;

    let mgmt_url = resolve_single_target(cli)?;
    let mgmt = ServerClient::new(mgmt_url.clone()).map_err(|e| {
        eprintln!("error: build client: {e}");
        ExitCode::from(2)
    })?;
    let detail = mgmt.get_store(store_id).await.map_err(|e| {
        eprintln!("error: lookup store {store_id}: {e}");
        ExitCode::from(2)
    })?;
    let listen = detail.listen_addr.ok_or_else(|| {
        eprintln!("error: store {store_id} has no listen_addr (server still starting?)");
        ExitCode::from(2)
    })?;
    let port = listen.rsplit(':').next().unwrap_or("");
    let host = mgmt_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split(':')
        .next()
        .unwrap_or("127.0.0.1");
    Ok(format!("{host}:{port}"))
}

async fn kv_scan(cli: &Cli, store_id: u64, group_id: u64, prefix: &str, limit: u32) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.kv_scan(store_id, group_id, prefix.as_bytes(), limit).await {
        Ok(out) => {
            if cli.json {
                return print_json(&serde_json::json!({
                    "items": out.items,
                    "truncated": out.truncated,
                }));
            }
            for item in &out.items {
                println!("{}\t{}", item.key_utf8, item.value_utf8);
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
