use anyhow::Result;
use std::sync::Arc;

use crate::cliargs::CliArgs;

pub mod list;
pub mod run;

/// Trait for defining commands that can be executed by the CLI
#[async_trait::async_trait]
#[allow(dead_code)]
pub trait Command {
    /// The name of the command
    fn name(&self) -> &'static str;

    /// Description of the command
    fn description(&self) -> &'static str;

    /// Execute the command
    async fn execute(&self, args: Arc<CliArgs>) -> Result<()>;
}

/*
/// Registry for auto-discovered commands
pub struct CommandRegistry {
    commands: Vec<Box<dyn Command + Send + Sync>>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self { commands: Vec::new() }
    }

    pub fn register_command(&mut self, command: Box<dyn Command + Send + Sync>) {
        self.commands.push(command);
    }

    pub fn get_command(&self, name: &str) -> Option<&(dyn Command + Send + Sync)> {
        self.commands.iter().find(|cmd| cmd.name() == name).map(|cmd| cmd.as_ref())
    }

    pub fn list_commands(&self) -> &[Box<dyn Command + Send + Sync>] {
        &self.commands
    }
}

impl Default for CommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}
*/
