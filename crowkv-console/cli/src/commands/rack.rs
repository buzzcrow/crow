// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use clap::Subcommand;
use crowkv_console_shared::clients::console::AddRackBody;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum RackVerb {
    Add {
        #[arg(long)]
        id: String,
        #[arg(long, default_value = "")]
        name: String,
    },
    Remove {
        #[arg(long)]
        id: String,
    },
    List,
}

pub async fn run_rack_verb(cli: &Cli, verb: RackVerb) -> ExitCode {
    match verb {
        RackVerb::Add { id, name } => rack_add(cli, id, name).await,
        RackVerb::Remove { id } => rack_remove(cli, &id).await,
        RackVerb::List => rack_list(cli).await,
    }
}

async fn rack_add(cli: &Cli, id: String, name: String) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.add_rack(&AddRackBody { id: id.clone(), name }).await {
        Ok(r) => {
            if cli.json {
                return print_json(&r);
            }
            println!("added rack {}", r.id);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: add rack {id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn rack_remove(cli: &Cli, id: &str) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.remove_rack(id).await {
        Ok(()) => {
            println!("removed rack {id}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: remove rack {id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn rack_list(cli: &Cli) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let racks = match client.list_racks().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: list racks: {e}");
            return ExitCode::from(2);
        }
    };
    if cli.json {
        return print_json(&racks);
    }
    if racks.is_empty() {
        println!("(no racks)");
        return ExitCode::SUCCESS;
    }
    println!("{:<16}  NAME", "ID");
    for r in &racks {
        println!("{:<16}  {}", r.id, r.name);
    }
    ExitCode::SUCCESS
}
