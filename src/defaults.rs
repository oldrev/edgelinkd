use crate::Result;

/// Creates a default flows.json with an inject->debug flow that sends "Hello, World" every 5 seconds
pub fn create_default_flows_json() -> serde_json::Value {
    serde_json::json!([
    {
        "id": "adf5c374d9ac0466",
        "type": "tab",
        "label": "Flow 1"
    },
    {
        "id": "ded1a8c84fec2323",
        "type": "inject",
        "z": "adf5c374d9ac0466",
        "name": "Inject Hello",
        "props": [
            {
                "p": "payload"
            },
            {
                "p": "topic",
                "vt": "str"
            }
        ],
        "repeat": "5",
        "crontab": "",
        "once": false,
        "onceDelay": 0.1,
        "topic": "",
        "payload": "Hello, EdgeLinkd!",
        "payloadType": "date",
        "x": 410,
        "y": 280,
        "wires": [
            [
                "dc18e4d63818b44b"
            ]
        ]
    },
    {
        "id": "dc18e4d63818b44b",
        "type": "debug",
        "z": "adf5c374d9ac0466",
        "name": "debug 1",
        "active": true,
        "tosidebar": true,
        "console": true,
        "tostatus": false,
        "complete": "payload",
        "targetType": "msg",
        "statusVal": "",
        "statusType": "auto",
        "x": 670,
        "y": 280,
        "wires": []
    }
    ])
}

/// Creates a default edgelinkd.toml configuration file
pub fn create_default_config_file(config_dir: &str) -> Result<()> {
    use std::fs;
    use std::path::Path;

    let config_path = Path::new(config_dir).join("edgelinkd.toml");

    // If config file already exists, nothing to do
    if config_path.exists() {
        return Ok(());
    }

    // Create directory if it doesn't exist
    if !Path::new(config_dir).exists() {
        fs::create_dir_all(config_dir)?;
        log::info!("Created config directory: {config_dir}");
    }

    // Create default config file content
    let default_config = r#"[runtime]

[runtime.engine]

[runtime.context]
default = "memory"

[runtime.context.stores]
memory = { provider = "memory" }

[runtime.flow]
node_msg_queue_capacity = 16

[ui-host]
host = "127.0.0.1"
port = 1888
"#;

    fs::write(&config_path, default_config)?;
    log::info!("Created default config file at: {}", config_path.display());

    Ok(())
}
