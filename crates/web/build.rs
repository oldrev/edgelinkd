use std::{env, fs, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let package_json = manifest_dir.join("../../3rd-party/node-red/package.json");
    println!("cargo:rerun-if-changed={}", package_json.display());

    let contents = fs::read_to_string(&package_json).expect("read Node-RED package.json");
    let version = contents
        .split_once("\"version\"")
        .and_then(|(_, rest)| rest.split_once(':'))
        .and_then(|(_, rest)| rest.split_once('"'))
        .and_then(|(_, rest)| rest.split_once('"'))
        .map(|(version, _)| version.trim())
        .filter(|version| !version.is_empty())
        .expect("Node-RED package.json version");

    println!("cargo:rustc-env=NODE_RED_VERSION={version}");
}
