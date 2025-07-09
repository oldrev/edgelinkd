use crate::cliargs::CliArgs;
use crate::consts;
use crate::defaults::create_default_config_file;

pub fn load_config(cli_args: &CliArgs) -> anyhow::Result<config::Config> {
    // Collect config file paths for logging
    let mut config_files = Vec::new();
    // Load configuration from default, development, and production files
    let home_dir = dirs_next::home_dir()
        .map(|x| x.join(consts::DEFAULT_HOME_DIR_NAME).to_string_lossy().to_string())
        .expect("Cannot get the `~/home` directory");

    // Priority order: --user-dir > --home > EDGELINK_HOME env var > default ~/.edgelinkd
    let edgelink_home_dir =
        cli_args.user_dir.clone().or(cli_args.home.clone()).or(std::env::var("EDGELINK_HOME").ok()).or(Some(home_dir));

    // Only set default flows_path if not specified by any source
    let mut builder = config::Config::builder();
    let mut home_dir_val = None;
    if let Some(ref hd) = edgelink_home_dir {
        home_dir_val = Some(hd.clone());
        builder = builder.set_override("home_dir", hd.clone())?;
        // Add config file paths for logging
        let main_cfg = std::path::Path::new(hd).join("edgelinkd.toml");
        let env_cfg = std::path::Path::new(hd).join(format!(
            "edgelinkd.{}.toml",
            cli_args.env.clone().or(std::env::var("EDGELINK_RUN_ENV").ok()).unwrap_or("dev".to_owned())
        ));
        config_files.push(main_cfg.display().to_string());
        config_files.push(env_cfg.display().to_string());
        // Actually add config files to builder
        builder = builder
            .add_source(config::File::with_name(&main_cfg.to_string_lossy()).required(false))
            .add_source(config::File::with_name(&env_cfg.to_string_lossy()).required(false));
    }
    // Merge CLI args into config builder (higher priority than default flows_path)
    builder = cli_args.merge_into_config_builder(builder)?;

    // Check if flows_path is specified by any source (CLI, config file, env)
    let flows_path = builder.clone().build()?.get_string("flows_path").ok();
    if flows_path.is_none() {
        if let Some(ref hd) = home_dir_val {
            use std::path::PathBuf;
            let default_flows = PathBuf::from(hd).join("flows.json").to_string_lossy().to_string();
            builder = builder.set_override("flows_path", default_flows)?;
            builder = builder.set_override("flows_path_is_default", true)?;
        }
    } else {
        builder = builder.set_override("flows_path_is_default", false)?;
    }

    let run_env = cli_args.env.clone().or(std::env::var("EDGELINK_RUN_ENV").ok()).unwrap_or("dev".to_owned());

    if cli_args.verbose > 0 {
        if let Some(ref x) = edgelink_home_dir {
            eprintln!("$EDGELINK_HOME={x}");
            eprintln!("Loading config files:");
            for f in &config_files {
                eprintln!("\t- `{f}`");
            }
        }
    }

    // Ensure the config directory exists and has default config
    if let Some(ref config_dir) = edgelink_home_dir {
        create_default_config_file(config_dir)?;
    }

    // Continue to set other config items
    builder = builder.set_override("run_env", run_env)?;
    builder = builder.set_override("node.msg_queue_capacity", 1)?;

    let config = builder.build()?;
    Ok(config)
}
