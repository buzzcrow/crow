#[cfg(feature = "swagger-ui")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = std::path::Path::new("target/openapi.json");
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = crowkv_server::management::openapi_json();
    std::fs::write(out, serde_json::to_string_pretty(&json)?)?;
    println!("{}", out.display());
    Ok(())
}

#[cfg(not(feature = "swagger-ui"))]
fn main() {
    eprintln!("openapi feature is required");
    std::process::exit(1);
}
