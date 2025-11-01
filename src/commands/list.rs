use anyhow::Result;
use std::sync::Arc;

use crate::cliargs::CliArgs;
use crate::commands::Command;
use crate::registry::list_available_nodes;

#[allow(dead_code)]
pub struct ListCommand;

#[async_trait::async_trait]
impl Command for ListCommand {
    fn name(&self) -> &'static str {
        "list"
    }

    fn description(&self) -> &'static str {
        "List all available node types"
    }

    async fn execute(&self, _args: Arc<CliArgs>) -> Result<()> {
        list_available_nodes().await
    }
}
