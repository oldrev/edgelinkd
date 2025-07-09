use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use edgelink_web::server::WebServer;

use crate::app::App;
use crate::cliargs::{CliArgs, Commands};
use crate::config::load_config;
use crate::consts;
use crate::env::EdgelinkEnv;
use crate::logging;
use crate::registry::list_available_nodes;

/// Determine the static directory path at runtime with fallback strategies
fn determine_static_directory() -> PathBuf {
    if let Ok(cargo_target_dir) = std::env::var("CARGO_TARGET_DIR") {
        // If CARGO_TARGET_DIR is set, use it
        return PathBuf::from(cargo_target_dir).join("debug/ui_static");
    }

    if let Ok(out_dir) = std::env::var("OUT_DIR") {
        // If in build environment, use OUT_DIR
        return PathBuf::from(out_dir).join("ui_static");
    }

    // Runtime fallback: try to find build output directory
    let exe_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("./target/debug/edgelinkd.exe"));

    let target_debug_fallback = PathBuf::from("./target/debug");
    let target_dir = exe_path.parent().unwrap_or(&target_debug_fallback);

    let target_fallback = PathBuf::from("./target");
    let build_dir = target_dir.parent().unwrap_or(&target_fallback).join("build");

    if let Ok(entries) = std::fs::read_dir(&build_dir) {
        let mut latest_build: Option<PathBuf> = None;
        let mut latest_time = std::time::SystemTime::UNIX_EPOCH;

        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with("edgelink-app-") {
                if let Ok(metadata) = entry.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        if modified > latest_time {
                            latest_time = modified;
                            latest_build = Some(entry.path().join("out/ui_static"));
                        }
                    }
                }
            }
        }

        if let Some(path) = latest_build {
            if path.exists() {
                return path;
            }
        }
    }

    // Final fallback to project root static directory
    PathBuf::from("ui_static")
}

pub async fn run_app(cli_args: Arc<CliArgs>) -> anyhow::Result<()> {
    match &cli_args.command {
        Some(Commands::Run { flows_path: _, headless: _, bind: _ }) => run_app_internal(cli_args.clone()).await,
        Some(Commands::List) => list_available_nodes().await,
        // Some(Commands::Help) => crate::commands::help::HelpCommand.execute(cli_args).await,
        None => run_app_internal(cli_args.clone()).await,
    }
}

pub async fn run_app_internal(cli_args: Arc<CliArgs>) -> anyhow::Result<()> {
    if cli_args.verbose > 0 {
        eprintln!("EdgeLink v{} - #{}\n", consts::APP_VERSION, consts::GIT_HASH);
        eprintln!("Loading configuration...");
    }

    let cfg = load_config(&cli_args)?;

    logging::log_init(&cfg);
    if cfg.get_int("verbose").unwrap_or(2) > 0 {
        eprintln!("Logging sub-system initialized.\n");
    }

    log::info!("EdgeLink Version={}-#{}", consts::APP_VERSION, consts::GIT_HASH);
    log::info!("==========================================================\n");

    // Prepare the runtime environment (ensure flows file exists, etc.)
    let env = EdgelinkEnv::new(&cfg);
    env.prepare()?;

    // Create cancellation token for graceful shutdown
    let cancel = CancellationToken::new();

    let ctrl_c_token = cancel.clone();
    tokio::task::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to install CTRL+C signal handler");
        log::info!("CTRL+C pressed, cancelling tasks...");
        ctrl_c_token.cancel();
    });

    log::info!("Starting EdgeLink run-time engine...");
    log::info!("Press CTRL+C to terminate.");

    // Create the App first to get flows data
    let app = Arc::new(App::new(cli_args.clone(), cfg.clone(), None).await?);

    let headless = cfg.get_bool("headless").unwrap_or(false);
    let web_server_handle: Option<JoinHandle<()>> =
        if !headless { Some(start_web_server(app.clone(), &cfg, cancel.clone()).await?) } else { None };

    let app_result = app.run(cancel.child_token()).await;

    if let Some(handle) = web_server_handle {
        handle.abort();
    }

    tokio::time::timeout(tokio::time::Duration::from_secs(10), cancel.cancelled()).await?;
    log::info!("Bye!");

    app_result
}

/// Start the web server with the given configuration
async fn start_web_server(
    app: Arc<App>,
    cfg: &config::Config,
    cancel: CancellationToken,
) -> anyhow::Result<JoinHandle<()>> {
    // Determine static directory at runtime
    let static_dir = determine_static_directory();
    log::info!("Using static directory: {}", static_dir.display());

    // flows_path 只从 config 获取，已统一默认值
    let flows_path = cfg.get_string("flows_path").expect("Config must provide flows_path after normalization");
    let flows_path = PathBuf::from(flows_path);

    // Create restart callback
    let app_clone = app.clone();
    let restart_callback = Arc::new(move |_flows_path: PathBuf| {
        let app = app_clone.clone();
        tokio::spawn(async move {
            if let Err(e) = app.restart_engine().await {
                log::error!("Failed to restart flow engine: {e}");
            } else {
                log::info!("Flow engine restarted successfully");
            }
        })
    });

    let web_server = WebServer::new(static_dir, cancel.clone())
        .with_registry(app.registry().clone())
        .with_flows_file_path(flows_path)
        .with_restart_callback(restart_callback)
        .with_engine(app.engine().clone(), cancel.clone())
        .await;

    // Determine bind address: config > 默认
    let bind_addr = if let Ok(addr) = cfg.get_string("bind") {
        addr
    } else if let Ok(host) = cfg.get_string("ui-host.host") {
        let port = cfg.get_int("ui-host.port").unwrap_or(1888) as u16;
        format!("{host}:{port}")
    } else {
        "127.0.0.1:1888".to_string()
    };

    let addr: std::net::SocketAddr =
        bind_addr.parse().map_err(|_| anyhow::anyhow!("Invalid bind address: {}", bind_addr))?;

    log::info!("Starting web server at http://{addr}");
    log::info!("Admin API available at:");
    log::info!("  GET  http://{addr}/api/admin/flows");
    log::info!("  POST http://{addr}/api/admin/flows");
    log::info!("  GET  http://{addr}/api/admin/nodes");
    log::info!("  GET  http://{addr}/api/admin/settings");
    log::info!("Health check: http://{addr}/api/health");

    Ok(web_server.spawn(addr, cancel.clone()).await)
}
