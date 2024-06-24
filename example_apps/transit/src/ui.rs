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

use axum::body::Body;
use axum::http::{header, HeaderValue};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use futures::{TryFutureExt, TryStreamExt};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;
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

/// Trivial web server to present the app UI.
///
/// # Arguments
/// * `port` - The port that the swimos server is listening on.
pub fn ui_server_router(port: u16) -> Router {
    Router::new()
        .route("/index.html", get(index_html))
        .route("/dist/main/swimos-transit.js", get(transit_js))
        .route("/dist/main/swimos-transit.js.map", get(transit_js_map))
        .with_state(UiConfig { port })
}

const INDEX: &str = "ui/index.html";

async fn index_html(State(UiConfig { port }): State<UiConfig>) -> impl IntoResponse {
    if let Ok(file) = File::open(INDEX).await {
        let lines = LinesStream::new(BufReader::new(file).lines()).map_ok(move |mut line| {
            if line == "const portParam = baseUri.port();" {
                format!("const portParam = {};\n", port)
            } else {
                line.push('\n');
                line
            }
        });
        let body_result = lines
            .try_fold(String::new(), |mut acc, line| async move {
                acc.push_str(&line);
                Ok(acc)
            })
            .await;
        if let Ok(body) = body_result {
            let headers = [
                (header::CONTENT_TYPE, HTML.clone()),
                (header::CONTENT_LENGTH, HeaderValue::from(body.len())),
            ];
            Ok((headers, body))
        } else {
            Err((StatusCode::NOT_FOUND, format!("File not found: {}", INDEX)))
        }
    } else {
        Err((StatusCode::NOT_FOUND, format!("File not found: {}", INDEX)))
    }
}

async fn transit_js() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("dist/main/swimos-transit.js").await)
}

static HTML: HeaderValue = HeaderValue::from_static("text/html; charset=utf-8");
static JS: HeaderValue = HeaderValue::from_static("application/json");

async fn transit_js_map() -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, JS.clone())];
    (headers, load_file("dist/main/swimos-transit.js.map").await)
}

async fn load_file(path: &str) -> impl IntoResponse {
    let target = format!("ui/{}", path);

    if let Ok((file, len)) = File::open(&target)
        .and_then(|f| async move {
            let meta = f.metadata().await?;
            Ok((f, meta.len()))
        })
        .await
    {
        let headers = [(header::CONTENT_LENGTH, HeaderValue::from(len))];
        Ok((headers, Body::from_stream(ReaderStream::new(file))))
    } else {
        Err((StatusCode::NOT_FOUND, format!("File not found: {}", target)))
    }
}
