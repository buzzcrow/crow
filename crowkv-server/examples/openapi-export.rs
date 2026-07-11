//! Dump the in-process `OpenAPI` document to `target/openapi.json`. The
//! Swagger UI bundle that consumes it now lives in the console
//! (`crowkv-console/static/swagger-ui/`), so this binary is just a
//! convenience for offline tooling and CI verification.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = std::path::Path::new("target/openapi.json");
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = crowkv_server::mgmt_api::openapi_json();
    std::fs::write(out, serde_json::to_string_pretty(&json)?)?;
    println!("{}", out.display());
    Ok(())
}
