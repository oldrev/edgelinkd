use clap::{Parser, Subcommand};

use crate::consts;

const LONG_ABOUT: &str = r#"
EdgeLink Daemon Program

EdgeLink is a Node-RED compatible run-time engine implemented in Rust.

Copyright (C) 2023-TODAY Li Wei and contributors. All rights reserved.

For more information, visit the website: https://github.com/oldrev/edgelink
"#;

#[derive(Parser, Debug, Clone)]
#[command(
    version = concat!(env!("CARGO_PKG_VERSION"), " • #", env!("EDGELINK_BUILD_GIT_HASH"), " • built at ", env!("EDGELINK_BUILD_TIME")), 
    about,
    author,
    long_about=LONG_ABOUT,
    color=clap::ColorChoice::Always
)]
pub struct CliArgs {
    /// Use verbose output, '0' means quiet, no output printed to stdout.
    #[arg(short, long, default_value_t = 2, global = true)]
    pub verbose: usize,

    /// Home directory of EdgeLink, default is `~/.edgelink`
    #[arg(long, global = true)]
    pub home: Option<String>,

    /// Path of the log configuration file.
    #[arg(short, long, global = true)]
    pub log_path: Option<String>,

    /// Set the running environment in 'dev' or 'prod', default is `dev`
    #[arg(long, global = true)]
    pub env: Option<String>,

    /// Use specified user directory
    #[arg(short = 'u', long, global = true)]
    pub user_dir: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Run the EdgeLink workflow engine
    Run {
        /// Path of the 'flows.json' file.
        #[arg()]
        flows_path: Option<String>,

        /// Run in headless mode (do not start web server)
        #[arg(long, default_value_t = false)]
        headless: bool,

        /// Server bind address for web interface
        #[arg(long, default_value = "127.0.0.1:1888")]
        bind: String,
    },
    /// List all available node types
    List,
}

impl CliArgs {
    // （已移除 default_run_command，保持 CliArgs 纯数据对象）
    /// Get the actual flows path, considering user_dir if provided
    pub fn get_flows_path(&self, flows_path: Option<String>) -> String {
        if let Some(flows_path) = flows_path {
            flows_path
        } else {
            let base_dir = if let Some(ref user_dir) = self.user_dir {
                std::path::PathBuf::from(user_dir)
            } else {
                dirs_next::home_dir().expect("Can not found the $HOME dir!!!").join(consts::DEFAULT_HOME_DIR_NAME)
            };
            base_dir.join("flows.json").to_string_lossy().to_string()
        }
    }

    /// Returns true if flows_path is user-specified, false if default
    pub fn is_flows_path_user(&self, flows_path: &Option<String>) -> bool {
        flows_path.is_some()
    }
}
