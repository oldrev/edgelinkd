use crate::defaults::create_default_flows_json;
use crate::Result;

/// Ensures the flows.json file exists, creating a default one if it doesn't
pub fn ensure_flows_file_exists(flows_path: &str) -> Result<()> {
    use std::fs;
    use std::path::Path;

    let path = Path::new(flows_path);

    // If file already exists, nothing to do
    if path.exists() {
        return Ok(());
    }

    // Create parent directory if it doesn't exist
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
            log::info!("Created directory: {}", parent.display());
        }
    }

    // Create default flows.json
    let default_flows = create_default_flows_json();
    let flows_content = serde_json::to_string_pretty(&default_flows)?;

    fs::write(flows_path, flows_content)?;
    log::info!("Created default flows.json at: {flows_path}");
    log::info!("Default flow includes an inject node that sends 'Hello, World!' every 5 seconds");

    Ok(())
}
