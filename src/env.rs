use crate::cliargs::CliArgs;
use crate::flows::ensure_flows_file_exists;
use edgelink_core::EdgelinkError;
use std::path::Path;

pub struct EdgelinkEnv<'a> {
    pub cli_args: &'a CliArgs,
}

impl<'a> EdgelinkEnv<'a> {
    pub fn new(cli_args: &'a CliArgs) -> Self {
        Self { cli_args }
    }

    /// Prepare the runtime environment: ensure flows file exists or error if user-specified and missing
    pub fn prepare(&self, flows_path: Option<String>) -> Result<(), EdgelinkError> {
        let flows_path = self.cli_args.get_flows_path(flows_path.clone());
        if !self.cli_args.is_flows_path_user(&flows_path.clone().into()) {
            ensure_flows_file_exists(&flows_path).map_err(|e| EdgelinkError::Other(e.into()))?;
        } else if !Path::new(&flows_path).exists() {
            return Err(EdgelinkError::Other(
                anyhow::anyhow!("The specified flows file does not exist: `{}`", flows_path).into(),
            ));
        }
        Ok(())
    }
}
