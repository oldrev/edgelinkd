use axum::{Json, http::StatusCode, routing::get};
use edgelink_core::web::{StaticNodeLocale, StaticWebHandler};

async fn list_serialports() -> Result<Json<Vec<String>>, StatusCode> {
    let ports = tokio::task::spawn_blocking(tokio_serial::available_ports)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|port| port.port_name)
        .collect();
    Ok(Json(ports))
}

fn serialport_route() -> axum::routing::MethodRouter {
    get(list_serialports)
}

inventory::submit! {
    StaticWebHandler {
        type_: "/serialports",
        router: serialport_route,
    }
}

inventory::submit! {
    StaticNodeLocale {
        namespace: "serial",
        relative_path: "node-red-node-serialport/serial.json",
    }
}
