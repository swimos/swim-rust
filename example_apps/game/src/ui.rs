// Copyright 2015-2023 Swim Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use axum::http::{header, HeaderValue};
use axum::{
    body::StreamBody, http::StatusCode, response::IntoResponse, routing::get,
    Router,
};
use futures::TryFutureExt;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

#[derive(Clone, Copy, Debug)]
pub struct UiConfig {
    pub port: u16,
}

impl UiConfig {
    pub fn new(port: u16) -> Self {
        UiConfig { port }
    }
}

pub fn ui_server_router(port: u16) -> Router {
    Router::new()
        .route("/index.html", get(index_html))
        .route("/lib/game.min.js", get(game_min_js))
        .route("/lib/game.min.js.map", get(game_min_js_map))
        .route("/lib/swim-core.min.js", get(swim_core_min_js))
        .route("/lib/swim-core.min.js.map", get(swim_core_min_js_map))
        .route("/lib/swim-host.min.js", get(swim_host_min_js))
        .route("/lib/swim-host.min.js.map", get(swim_host_min_js_map))
        .route("/lib/swim-ui.min.js", get(swim_ui_min_js))
        .route("/lib/swim-ui.min.js.map", get(swim_ui_min_js_map))
        .route("/lib/swim-ux.min.js", get(swim_ux_min_js))
        .route("/lib/swim-ux.min.js.map", get(swim_ux_min_js_map))
        .route("/lib/swim-vis.min.js", get(swim_vis_min_js))
        .route("/lib/swim-vis.min.js.map", get(swim_vis_min_js_map))
        .route("/assets/favicon.ico", get(favicon))
        .route("/assets/swim-logo.png", get(swim_logo))

        .with_state(UiConfig { port })
}

static HTML: HeaderValue = HeaderValue::from_static("text/html; charset=utf-8");
static JS: HeaderValue = HeaderValue::from_static("application/json");
static ICON: HeaderValue = HeaderValue::from_static("image/x-icon");
static PNG: HeaderValue = HeaderValue::from_static("image/png");

async fn load_file(path: &str) -> impl IntoResponse {
    let target = format!("ui/dist/{}", path);

    if let Ok((file, len)) = File::open(&target)
        .and_then(|f| async move {
            let meta = f.metadata().await?;
            Ok((f, meta.len()))
        })
        .await
    {
        let headers = [(header::CONTENT_LENGTH, HeaderValue::from(len))];
        Ok((headers, StreamBody::new(ReaderStream::new(file))))
    } else {
        Err((StatusCode::NOT_FOUND, format!("File not found: {}", target)))
    }
}

async fn index_html() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, HTML.clone())];
    (headers, load_file("index.html").await)
}

async fn game_min_js() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/game.min.js").await)
}

async fn game_min_js_map() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/game.min.js.map").await)
}

async fn swim_core_min_js() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-core.min.js").await)
}

async fn swim_core_min_js_map() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-core.min.js.map").await)
}

async fn swim_host_min_js() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-host.min.js").await)
}

async fn swim_host_min_js_map() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-host.min.js.map").await)
}

async fn swim_ui_min_js() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-ui.min.js").await)
}

async fn swim_ui_min_js_map() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-ui.min.js.map").await)
}

async fn swim_ux_min_js() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-ux.min.js").await)
}

async fn swim_ux_min_js_map() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-ux.min.js.map").await)
}

async fn swim_vis_min_js() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-vis.min.js").await)
}

async fn swim_vis_min_js_map() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("lib/swim-vis.min.js.map").await)
}

async fn favicon() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, ICON.clone())];
    (headers, load_file("assets/favicon.ico").await)
}

async fn swim_logo() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, PNG.clone())];
    (headers, load_file("assets/swim-logo.png").await)
}