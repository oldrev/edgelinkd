use crate::flows::ensure_flows_file_exists;
use config::Config;
use edgelink_core::EdgelinkError;
use std::path::Path;

pub struct EdgelinkEnv<'a> {
    pub config: &'a Config,
}

impl<'a> EdgelinkEnv<'a> {
    pub fn new(config: &'a Config) -> Self {
        Self { config }
    }

    /// Prepare the runtime environment: ensure flows file exists or error if user-specified and missing
    pub fn prepare(&self) -> Result<(), EdgelinkError> {
        let flows_path =
            self.config.get_string("flows_path").expect("Config must provide flows_path after normalization");
        let is_default = self.config.get_bool("flows_path_is_default").unwrap_or(false);
        if is_default {
            // 默认 flows_path，自动创建
            ensure_flows_file_exists(&flows_path).map_err(|e| EdgelinkError::Other(e.into()))?;
        } else {
            // 用户指定 flows_path，必须存在
            if !Path::new(&flows_path).exists() {
                return Err(EdgelinkError::Other(
                    anyhow::anyhow!("The specified flows file does not exist: `{}`", flows_path).into(),
                ));
            }
        }
        Ok(())
    }
}
