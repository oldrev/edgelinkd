use std::path::PathBuf;

pub fn ui_static_dir() -> PathBuf {
    let mut static_dir = std::path::PathBuf::from("target").join("ui_static");
    if !static_dir.exists() {
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                let release_dir = exe_dir.join("ui_static");
                if release_dir.exists() {
                    static_dir = release_dir;
                }
            }
        }
    }
    static_dir
}
