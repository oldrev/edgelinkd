use axum::response::IntoResponse;

const ABOUT_HEADER: &str = r#"# EdgeLinkd

EdgeLinkd is a Rust-based Node-RED compatible runtime.

Copyright (c) 2023-present Wei Li and other contributors.

- Website: https://oldrev.github.io/edgelinkd
- Contact: oldrev@gmail.com

Licensed under the Apache License, Version 2.0.

## Included software

This distribution includes software developed by the OpenJS Foundation and other
contributors. Node-RED is developed by the OpenJS Foundation and is licensed
under the Apache License, Version 2.0.

- EdgeLinkd: https://github.com/oldrev/edgelinkd
- Node-RED: https://nodered.org/
- OpenJS Foundation: https://openjsf.org/
- Apache License 2.0: https://www.apache.org/licenses/LICENSE-2.0

## Node-RED changelog

"#;

/// Markdown shown by Node-RED's built-in About menu.
pub fn about_markdown() -> String {
    format!("{ABOUT_HEADER}{}", include_str!("../../../../3rd-party/node-red/CHANGELOG.md"))
}

/// Serve the About Markdown without changing the bundled Node-RED editor.
pub async fn get_about() -> impl IntoResponse {
    ([(axum::http::header::CONTENT_TYPE, "text/markdown; charset=utf-8")], about_markdown())
}
