use std::process::ExitCode;

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
