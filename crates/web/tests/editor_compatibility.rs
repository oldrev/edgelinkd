use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use edgelink_web::{WebServer, models::RedSystemSettings};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

fn server(editor: &str) -> WebServer {
    let mut settings = serde_json::to_value(RedSystemSettings::default()).unwrap();
    settings["editorTheme"]["codeEditor"]["lib"] = editor.into();
    let cfg = config::Config::try_from(&json!({"web": settings})).unwrap();
    RedSystemSettings::load(&cfg).expect("valid editor settings");
    WebServer::new(concat!(env!("CARGO_MANIFEST_DIR"), "/public"), CancellationToken::new(), &cfg)
}

#[tokio::test]
async fn code_exchange_explicitly_rejects_unsupported_login() {
    let response = server("monaco")
        .router()
        .oneshot(
            Request::post("/auth/token")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("code=expired-or-unknown"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(body["error"], "not_supported");
    assert!(body.get("accessToken").is_none());
}

#[tokio::test]
async fn admin_api_does_not_grant_cross_origin_access_by_default() {
    let router = server("monaco").router();
    for method in ["GET", "OPTIONS"] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri("/settings")
                    .header("origin", "https://unrelated.example")
                    .header("access-control-request-method", "GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!response.headers().contains_key("access-control-allow-origin"));
        if method == "GET" {
            assert_eq!(response.status(), StatusCode::OK);
        }
    }
}

#[tokio::test]
async fn editor_bootstrap_matches_advertised_library() {
    for (requested, effective) in [("monaco", "monaco"), ("ace", "ace"), ("basic", "basic"), ("unknown", "basic")] {
        let router = server(requested).router();
        let response = router.clone().oneshot(Request::get("/settings").body(Body::empty()).unwrap()).await.unwrap();
        let settings: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16384).await.unwrap()).unwrap();
        assert_eq!(settings["editorTheme"]["codeEditor"]["lib"], effective);
        assert_eq!(settings["editorTheme"]["schemes"], json!(["light", "dark"]));

        let response = router.clone().oneshot(Request::get("/theme").body(Body::empty()).unwrap()).await.unwrap();
        let theme: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16384).await.unwrap()).unwrap();
        assert_eq!(theme["asset"]["vendorMonaco"].as_str().unwrap().is_empty(), effective != "monaco");
        assert_eq!(theme["asset"]["vendorAce"].as_str().unwrap().is_empty(), effective != "ace");

        for path in ["/", "/index.html"] {
            let response = router.clone().oneshot(Request::get(path).body(Body::empty()).unwrap()).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let html = String::from_utf8(to_bytes(response.into_body(), 16384).await.unwrap().to_vec()).unwrap();
            assert_eq!(html.contains("vendor/monaco/monaco-bootstrap.js"), effective == "monaco");
            assert_eq!(html.contains("vendor/ace/ace-bootstrap.js"), effective == "ace");
        }
    }
}
