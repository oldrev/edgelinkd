use crate::cliargs::CliArgs;
use crate::consts;
use crate::defaults::create_default_config_file;

pub fn load_config(cli_args: &CliArgs) -> anyhow::Result<Option<config::Config>> {
    // Load configuration from default, development, and production files
    let home_dir = dirs_next::home_dir()
        .map(|x| x.join(consts::DEFAULT_HOME_DIR_NAME).to_string_lossy().to_string())
        .expect("Cannot get the `~/home` directory");

    // Priority order: --user-dir > --home > EDGELINK_HOME env var > default ~/.edgelink
    let edgelink_home_dir =
        cli_args.user_dir.clone().or(cli_args.home.clone()).or(std::env::var("EDGELINK_HOME").ok()).or(Some(home_dir));

    let run_env = cli_args.env.clone().or(std::env::var("EDGELINK_RUN_ENV").ok()).unwrap_or("dev".to_owned());

    if cli_args.verbose > 0 {
        if let Some(ref x) = edgelink_home_dir {
            eprintln!("$EDGELINK_HOME={x}");
        }
    }

    // Ensure the config directory exists and has default config
    if let Some(ref config_dir) = edgelink_home_dir {
        create_default_config_file(config_dir)?;
    }

    if let Some(md) = edgelink_home_dir.as_ref().and_then(|x| std::fs::metadata(x).ok()) {
        if md.is_dir() {
            let mut builder = config::Config::builder();

            builder = if let Some(hd) = edgelink_home_dir {
                builder
                    .add_source(config::File::with_name(&format!("{hd}/edgelinkd.toml")).required(false))
                    .add_source(config::File::with_name(&format!("{hd}/edgelinkd.{run_env}.toml")).required(false))
                    .set_override("home_dir", hd)?
            } else {
                builder
            };

            builder = builder
                .set_override("run_env", run_env)? // override run_env
                .set_override("node.msg_queue_capacity", 1)?;
            let config = builder.build()?;
            return Ok(Some(config));
        }
    }
    if cli_args.verbose > 0 {
        eprintln!("The `$EDGELINK_HOME` directory does not exist!");
    }
    Ok(None)
}
