use std::path::PathBuf;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use edgelink_core::runtime::paths;
use edgelink_web::server::WebServer;

use crate::app::App;
use crate::cliargs::{CliArgs, Commands};
use crate::config::load_config;
use crate::consts;
use crate::env::EdgelinkEnv;
use crate::logging;
use crate::registry::list_available_nodes;

pub async fn run_app(cli_args: Arc<CliArgs>) -> edgelink_core::Result<()> {
    match &cli_args.command {
        Some(Commands::Run { flows_path: _, headless: _, bind: _ }) => run_app_internal(cli_args.clone()).await,
        Some(Commands::List) => list_available_nodes().await,
        // Some(Commands::Help) => crate::commands::help::HelpCommand.execute(cli_args).await,
        None => run_app_internal(cli_args.clone()).await,
    }
}

pub async fn run_app_internal(cli_args: Arc<CliArgs>) -> edgelink_core::Result<()> {
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
    let env = Arc::new(EdgelinkEnv::new(cfg));
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
    let app = Arc::new(App::new(cli_args.clone(), env, None).await?);

    let headless = app.env().config.get_bool("headless").unwrap_or(false);
    use tokio::try_join;
    if headless {
        try_join!(app.run(cancel.child_token()), async { Ok(()) })?;
    } else {
        let handle = start_web_server(app.clone(), &app.env().config, cancel.clone()).await?;
        let app_fut = app.run(cancel.child_token());
        let web_fut = async {
            handle.await.map_err(|e| anyhow::anyhow!("Web server task failed: {e}"))?;
            Ok(())
        };
        try_join!(app_fut, web_fut)?;
    }

    // 等待 cancel token 完成
    tokio::time::timeout(tokio::time::Duration::from_secs(10), cancel.cancelled()).await?;
    log::info!("Bye!");

    Ok(())
}

/// Start the web server with the given configuration
async fn start_web_server(
    app: Arc<App>,
    cfg: &config::Config,
    cancel: CancellationToken,
) -> edgelink_core::Result<JoinHandle<()>> {
    // Determine static directory at runtime
    let static_dir = paths::ui_static_dir();
    log::info!("Using static directory: {}", static_dir.display());

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
        .await
        .with_flows_file_path(flows_path)
        .await
        .with_restart_callback(restart_callback)
        .await
        .with_engine(app.engine().clone(), cancel.clone())
        .await;

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

    web_server.spawn(addr, cancel.clone()).await
}
