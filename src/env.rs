use crate::flows::ensure_flows_file_exists;
use config::Config;
use edgelink_core::EdgelinkError;
use once_cell::sync::OnceCell;
use std::path::Path;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct EdgelinkEnv {
    pub config: Config,
    pub exe_dir: String,
    pub ui_static_dir: OnceCell<String>,
}

impl EdgelinkEnv {
    pub fn new(config: Config) -> Self {
        let exe_dir = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.to_path_buf()))
            .and_then(|d| d.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "".to_string());
        let ui_static_dir = OnceCell::from(Self::calc_ui_static_dir(&exe_dir));
        Self { config, exe_dir, ui_static_dir }
    }

    /// Calculates the directory path for UI static files as a string, used for initialization.
    fn calc_ui_static_dir(exe_dir: &str) -> String {
        let mut static_dir = std::path::PathBuf::from("target").join("ui_static");
        // If not found in target/ui_static, try to find next to the executable
        if !static_dir.exists() {
            let exe_dir = if !exe_dir.is_empty() {
                Some(std::path::PathBuf::from(exe_dir))
            } else {
                std::env::current_exe().ok().and_then(|p| p.parent().map(|d| d.to_path_buf()))
            };
            if let Some(exe_dir) = exe_dir {
                let release_dir = exe_dir.join("ui_static");
                if release_dir.exists() {
                    static_dir = release_dir;
                }
            }
        }
        static_dir.to_string_lossy().to_string()
    }

    /// Prepare the runtime environment: ensure flows file exists or error if user-specified and missing
    pub fn prepare(&self) -> Result<(), EdgelinkError> {
        let flows_path =
            self.config.get_string("flows_path").expect("Config must provide flows_path after normalization");
        let is_default = self.config.get_bool("flows_path_is_default").unwrap_or(false);
        if is_default {
            // If using the default flows_path, create it automatically if missing
            ensure_flows_file_exists(&flows_path).map_err(|e| EdgelinkError::Other(e.into()))?;
        } else {
            // If user specified flows_path, it must exist
            if !Path::new(&flows_path).exists() {
                return Err(EdgelinkError::Other(
                    anyhow::anyhow!("The specified flows file does not exist: `{}`", flows_path).into(),
                ));
            }
        }
        Ok(())
    }
}
