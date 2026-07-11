use std::process::ExitCode;

use crowkv_console_shared::snapshot::ClusterSnapshot;

use crate::Cli;

pub mod client;
pub mod config;

pub fn print_json<T: serde::Serialize>(v: &T) -> ExitCode {
    match serde_json::to_string_pretty(v) {
        Ok(s) => {
            println!("{s}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: json encode: {e}");
            ExitCode::from(2)
        }
    }
}

pub fn not_implemented(what: &str) -> ExitCode {
    eprintln!("crowkv: '{what}' is not implemented yet (C0/C1 skeleton).");
    ExitCode::from(1)
}

pub async fn fetch_snapshot(cli: &Cli) -> Result<ClusterSnapshot, ExitCode> {
    let targets = config::resolve_targets(cli)?;
    crowkv_console_shared::topology::aggregate(&targets).await.map_err(|e| {
        eprintln!("error: aggregate failed: {e}");
        ExitCode::from(2)
    })
}
