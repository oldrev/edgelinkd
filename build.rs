use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let build_time = chrono::Utc::now().to_rfc3339();
    println!("cargo:rustc-env=EDGELINK_BUILD_TIME={build_time}");

    set_git_revision_hash();
    check_patch();
    gen_use_plugins_file();
    build_static_files();
}

fn gen_use_plugins_file() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("__use_node_plugins.rs");

    let plugins_dir = Path::new("node-plugins");
    let mut plugin_names = Vec::new();

    if plugins_dir.is_dir() {
        for entry in fs::read_dir(plugins_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.path().is_dir() {
                let plugin_name = entry.file_name().to_string_lossy().replace("-", "_");
                plugin_names.push(plugin_name);
            }
        }
    }

    let mut file_content = String::new();
    for plugin_name in plugin_names {
        file_content.push_str(&format!("extern crate {plugin_name};\n"));
    }

    fs::write(&dest_path, file_content).unwrap();

    println!("cargo:rerun-if-changed=node-plugins");
}

/// Make the current git hash available to the build as the environment
/// variable `EDGELINK_BUILD_GIT_HASH`.
fn set_git_revision_hash() {
    let args = &["rev-parse", "--short=10", "HEAD"];
    let Ok(output) = Command::new("git").args(args).output() else {
        return;
    };
    let rev = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if rev.is_empty() {
        return;
    }
    println!("cargo:rustc-env=EDGELINK_BUILD_GIT_HASH={rev}");
}

fn check_patch() {
    if env::consts::OS == "windows" {
        let output = Command::new("patch.exe")
            .arg("--version")
            .output()
            .expect("Failed to execute `patch.exe --version`, the GNU Patch program is required to build this project");

        if output.status.success() {
            let version_info = String::from_utf8_lossy(&output.stdout);
            let first_line = version_info.lines().next().unwrap_or("Unknown version");
            if !first_line.to_lowercase().contains("patch") {
                eprintln!("Error: The Patch program is required to build this project, but got: {first_line}");
                std::process::exit(1);
            }
        } else {
            let error_info = String::from_utf8_lossy(&output.stderr);
            eprintln!("Error: Failed to get patch.exe version: {error_info}");
            std::process::exit(1);
        }
    }
}

fn build_static_files() {
    use std::path::PathBuf;

    println!("cargo:rerun-if-changed=crates/web/public");
    println!("cargo:rerun-if-changed=3rd-party/node-red/packages");
    println!("cargo:rerun-if-changed=3rd-party/node-red/package.json");
    println!("cargo:rerun-if-changed=3rd-party/node-red/package-lock.json");

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let static_dir = Path::new(&out_dir).join("ui_static");
    let public_dir = PathBuf::from("crates/web/public");
    let node_red_dir = PathBuf::from("3rd-party/node-red/packages/node_modules/@node-red/editor-client/public");
    let node_red_nodes_dir = PathBuf::from("3rd-party/node-red/packages/node_modules/@node-red/nodes");
    let node_red_root = PathBuf::from("3rd-party/node-red");

    // Build Node-RED if needed
    if node_red_root.exists() {
        // Check if Node-RED needs to be built
        let package_json = node_red_root.join("package.json");
        let node_modules = node_red_root.join("node_modules");

        if package_json.exists() && (!node_modules.exists() || !node_red_dir.exists()) {
            println!("cargo:warning=Building Node-RED editor...");
            build_node_red(&node_red_root);
        }
    }

    // Only build if source directories exist
    if public_dir.exists() || node_red_dir.exists() || node_red_nodes_dir.exists() {
        println!("cargo:warning=Building static files directory...");

        // Create static directory if it doesn't exist
        std::fs::create_dir_all(&static_dir).expect("Failed to create static directory");

        // Incrementally copy from public directory
        if public_dir.exists() {
            copy_dir_contents_incremental(&public_dir, &static_dir).expect("Failed to copy public files");
        }

        // Incrementally copy from node-red directory
        if node_red_dir.exists() {
            copy_dir_contents_incremental(&node_red_dir, &static_dir).expect("Failed to copy node-red files");
        }

        // Copy Node-RED nodes directory to static/nodes
        if node_red_nodes_dir.exists() {
            let static_nodes_dir = static_dir.join("nodes");
            std::fs::create_dir_all(&static_nodes_dir).expect("Failed to create static nodes directory");
            copy_dir_contents_incremental(&node_red_nodes_dir, &static_nodes_dir)
                .expect("Failed to copy node-red nodes");
        }

        // Copy Node-RED core nodes lib directories to static/
        if node_red_nodes_dir.exists() {
            copy_node_lib_directories(&node_red_nodes_dir, &static_dir).expect("Failed to copy node lib directories");
        }

        // Copy Node-RED icon files to static/icons (for icon requests like /icons/node-red/file-in.svg)
        if node_red_nodes_dir.exists() {
            copy_node_red_icons(&node_red_nodes_dir, &static_dir).expect("Failed to copy node-red icons");
        }

        // Copy Node-RED locales for i18n support
        copy_node_red_locales(&static_dir).expect("Failed to copy node-red locales");

        println!("cargo:warning=Static files build complete!");
    }
}

/// Incrementally copy directory contents, only copying files that are newer or don't exist
fn copy_dir_contents_incremental(src: &Path, dst: &Path) -> std::io::Result<()> {
    use std::fs;

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            fs::create_dir_all(&dst_path)?;
            copy_dir_contents_incremental(&src_path, &dst_path)?;
        } else {
            let should_copy = if !dst_path.exists() {
                true
            } else {
                let src_metadata = fs::metadata(&src_path)?;
                let dst_metadata = fs::metadata(&dst_path)?;

                // Compare file size first (faster than time comparison)
                if src_metadata.len() != dst_metadata.len() {
                    true
                } else {
                    // Compare modification time if sizes are equal
                    let src_time = src_metadata.modified().ok();
                    let dst_time = dst_metadata.modified().ok();

                    match (src_time, dst_time) {
                        (Some(src_t), Some(dst_t)) => src_t > dst_t,
                        _ => true, // If we can't get timestamps, copy to be safe
                    }
                }
            };

            if should_copy {
                if let Some(parent) = dst_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(&src_path, &dst_path)?;
            }
        }
    }

    Ok(())
}

fn build_node_red(node_red_root: &Path) {
    use std::process::Command;

    // Check if npm is available
    let npm_cmd = if cfg!(target_os = "windows") { "npm.cmd" } else { "npm" };

    let npm_check = Command::new(npm_cmd).arg("--version").output();

    if npm_check.is_err() {
        println!("cargo:warning=npm not found, skipping Node-RED build");
        return;
    }

    // Install dependencies
    println!("cargo:warning=Installing Node-RED dependencies...");
    let install_result = Command::new(npm_cmd).arg("install").current_dir(node_red_root).status();

    if let Err(e) = install_result {
        panic!("Failed to install Node-RED dependencies: {e}");
    }

    if !install_result.unwrap().success() {
        panic!("npm ci failed for Node-RED");
    }

    // Build Node-RED
    println!("cargo:warning=Building Node-RED...");
    let build_result = Command::new(npm_cmd).arg("run").arg("build").current_dir(node_red_root).status();

    if let Err(e) = build_result {
        panic!("Failed to build Node-RED: {e}");
    }

    if !build_result.unwrap().success() {
        panic!("npm run build failed for Node-RED");
    }

    println!("cargo:warning=Node-RED build complete!");
}

/// Copy Node-RED icon files to static/icons directory for proper icon serving
fn copy_node_red_icons(node_red_nodes_dir: &Path, static_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let icons_src = node_red_nodes_dir.join("icons");
    if !icons_src.exists() {
        return Ok(());
    }

    let icons_dest = static_dir.join("icons/node-red");
    std::fs::create_dir_all(&icons_dest)?;

    // Copy all icon files from node-red/nodes/icons to static/icons/node-red/
    copy_dir_contents_incremental(&icons_src, &icons_dest)?;
    println!("cargo:warning=Copied Node-RED icons from {} to {}", icons_src.display(), icons_dest.display());

    Ok(())
}

fn copy_node_lib_directories(node_red_nodes_dir: &Path, static_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let core_dir = node_red_nodes_dir.join("core");
    if !core_dir.exists() {
        return Ok(());
    }

    // Copy core/*/lib/* to static/core/*/lib/*
    copy_core_lib_directories(&core_dir, static_dir)?;

    Ok(())
}

fn copy_core_lib_directories(core_dir: &Path, static_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if !core_dir.is_dir() {
        return Ok(());
    }

    // Iterate through core/* directories (common, function, network, etc.)
    for entry in std::fs::read_dir(core_dir)? {
        let entry = entry?;
        let category_path = entry.path();

        if category_path.is_dir() {
            let category_name = category_path.file_name().unwrap().to_str().unwrap();
            let lib_dir = category_path.join("lib");

            if lib_dir.exists() && lib_dir.is_dir() {
                // Create static/core/{category}/lib directory
                let dest_base = static_dir.join("core").join(category_name).join("lib");
                std::fs::create_dir_all(&dest_base)?;

                // Copy all lib contents to static/core/{category}/lib/
                copy_dir_contents_incremental(&lib_dir, &dest_base)?;
                println!("cargo:warning=Copied {} to {}", lib_dir.display(), dest_base.display());
            }
        }
    }

    Ok(())
}

/// Copy Node-RED locale files to static/locales for i18n support
fn copy_node_red_locales(static_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let editor_locales_dir = PathBuf::from("3rd-party/node-red/packages/node_modules/@node-red/editor-client/locales");
    let nodes_locales_dir = PathBuf::from("3rd-party/node-red/packages/node_modules/@node-red/nodes/locales");
    let dest_locales_dir = static_dir.join("locales");

    std::fs::create_dir_all(&dest_locales_dir)?;

    // Copy editor client locales
    if editor_locales_dir.exists() {
        for entry in std::fs::read_dir(&editor_locales_dir)? {
            let entry = entry?;
            let lang_dir = entry.path();

            if lang_dir.is_dir() {
                let lang_name = lang_dir.file_name().unwrap().to_str().unwrap();
                let dest_lang_dir = dest_locales_dir.join(lang_name);
                std::fs::create_dir_all(&dest_lang_dir)?;

                // Copy all JSON files from this language directory
                for lang_entry in std::fs::read_dir(&lang_dir)? {
                    let lang_entry = lang_entry?;
                    let src_file = lang_entry.path();

                    if src_file.is_file() && src_file.extension().is_some_and(|ext| ext == "json") {
                        let dest_file = dest_lang_dir.join(lang_entry.file_name());
                        std::fs::copy(&src_file, &dest_file)?;
                    }
                }
            }
        }
        println!(
            "cargo:warning=Copied editor locales from {} to {}",
            editor_locales_dir.display(),
            dest_locales_dir.display()
        );
    }

    // Copy nodes locales
    if nodes_locales_dir.exists() {
        for entry in std::fs::read_dir(&nodes_locales_dir)? {
            let entry = entry?;
            let lang_dir = entry.path();

            if lang_dir.is_dir() {
                let lang_name = lang_dir.file_name().unwrap().to_str().unwrap();
                let dest_lang_dir = dest_locales_dir.join(lang_name);
                std::fs::create_dir_all(&dest_lang_dir)?;

                // Copy messages.json if it exists
                let messages_file = lang_dir.join("messages.json");
                if messages_file.exists() {
                    let dest_messages = dest_lang_dir.join("messages.json");
                    std::fs::copy(&messages_file, &dest_messages)?;
                }

                // Copy all node category directories (common, function, network, etc.)
                for lang_entry in std::fs::read_dir(&lang_dir)? {
                    let lang_entry = lang_entry?;
                    let category_path = lang_entry.path();

                    if category_path.is_dir() {
                        let category_name = category_path.file_name().unwrap().to_str().unwrap();
                        let dest_category_dir = dest_lang_dir.join(category_name);
                        std::fs::create_dir_all(&dest_category_dir)?;

                        // Copy all JSON files in this category
                        for category_entry in std::fs::read_dir(&category_path)? {
                            let category_entry = category_entry?;
                            let src_file = category_entry.path();

                            if src_file.is_file() && src_file.extension().is_some_and(|ext| ext == "json") {
                                let dest_file = dest_category_dir.join(category_entry.file_name());
                                std::fs::copy(&src_file, &dest_file)?;
                            }
                        }
                    }
                }
            }
        }
        println!(
            "cargo:warning=Copied nodes locales from {} to {}",
            nodes_locales_dir.display(),
            dest_locales_dir.display()
        );
    }

    Ok(())
}
