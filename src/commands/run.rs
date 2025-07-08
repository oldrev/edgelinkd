use anyhow::Result;
use std::sync::Arc;

use crate::cliargs::{CliArgs, Commands};
use crate::commands::Command;
use crate::runner::run_app_internal;

pub struct RunCommand;

#[async_trait::async_trait]
impl Command for RunCommand {
    fn name(&self) -> &'static str {
        "run"
    }

    fn description(&self) -> &'static str {
        "Run the EdgeLink workflow engine"
    }

    async fn execute(&self, args: Arc<CliArgs>) -> Result<()> {
        if let Some(Commands::Run { flows_path, headless, bind }) = &args.command {
            run_app_internal(args.clone(), flows_path.clone(), *headless, bind.clone()).await
        } else {
            anyhow::bail!("Invalid command arguments for run")
        }
    }
}
