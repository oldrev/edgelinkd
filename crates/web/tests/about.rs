use axum::response::IntoResponse;
use edgelink_web::handlers::about::{about_markdown, get_about};

#[test]
fn about_markdown_identifies_edgelinkd_and_preserves_attributions() {
    let about = about_markdown();

    for required in [
        "EdgeLinkd",
        "Wei Li",
        "oldrev.github.io/edgelinkd",
        "oldrev@gmail.com",
        "OpenJS Foundation",
        "Apache License, Version 2.0",
        "Node-RED changelog",
    ] {
        assert!(about.contains(required), "about text is missing {required:?}");
    }
}

#[tokio::test]
async fn about_handler_serves_markdown_content_type() {
    let response = get_about().await.into_response();

    assert_eq!(
        response.headers().get("content-type").and_then(|value| value.to_str().ok()),
        Some("text/markdown; charset=utf-8")
    );
}
