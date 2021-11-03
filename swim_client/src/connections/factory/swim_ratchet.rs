// Copyright 2015-2021 SWIM.AI inc.
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

use crate::connections::factory::stream::{build_stream, get_stream_type};
use crate::connections::factory::{async_factory, HostConfig};
use futures::future::BoxFuture;
use futures::FutureExt;
use http::uri::InvalidUri;
use http::Request;
use http::Uri;
use ratchet::deflate::Deflate;
use ratchet::{ProtocolRegistry, WebSocketConfig};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use swim_runtime::error::{
    ConnectionError, HttpError, HttpErrorKind, InvalidUriError, InvalidUriErrorKind,
};
use swim_runtime::ws::utils::maybe_resolve_scheme;
use swim_runtime::ws::{
    CompressionSwitcherProvider, Protocol, StreamDef, StreamSwitcher, WebSocketDef,
    WebsocketFactory,
};
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;
use url::Url;

const WARP0_PROTO: &str = "warp0";

pub struct RatchetWebSocketFactory {
    inner: async_factory::AsyncFactory<StreamDef, Deflate>,
    host_configurations: HashMap<Url, HostConfig>,
}

impl RatchetWebSocketFactory {
    pub async fn with(
        buffer_size: usize,
        host_configurations: HashMap<Url, HostConfig>,
    ) -> RatchetWebSocketFactory {
        let inner = async_factory::AsyncFactory::new(buffer_size, connect).await;

        RatchetWebSocketFactory {
            inner,
            host_configurations,
        }
    }

    pub async fn new(buffer_size: usize) -> RatchetWebSocketFactory {
        RatchetWebSocketFactory::with(buffer_size, Default::default()).await
    }
}

impl WebsocketFactory for RatchetWebSocketFactory {
    type Sock = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;
    type Ext = Deflate;

    fn connect(&mut self, url: Url) -> BoxFuture<Result<WebSocketDef<Deflate>, ConnectionError>> {
        let config = match self.host_configurations.entry(url.clone()) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => v
                .insert(HostConfig {
                    protocol: Protocol::PlainText,
                    compression_level: CompressionSwitcherProvider::Off,
                })
                .clone(),
        };

        self.inner.connect_using(url, config).boxed()
    }
}

async fn connect(url: Url, config: HostConfig) -> Result<WebSocketDef<Deflate>, ConnectionError> {
    let HostConfig {
        protocol,
        compression_level,
    } = config;

    let url = url.as_str();
    let uri: Uri = url.parse().map_err(|e: InvalidUri| {
        ConnectionError::Http(HttpError::new(
            HttpErrorKind::InvalidUri(InvalidUriError::new(
                InvalidUriErrorKind::Malformatted,
                Some(url.to_string()),
            )),
            Some(e.to_string()),
        ))
    })?;
    let request = Request::get(uri)
        .body(())
        .map_err(|e| ConnectionError::Http(HttpError::from(e)))?;

    let request = maybe_resolve_scheme(request)?;
    let stream_type = get_stream_type(&request, &protocol)?;

    let port = request
        .uri()
        .port_u16()
        .unwrap_or_else(|| match request.uri().scheme_str() {
            Some("wss") => 443,
            Some("ws") => 80,
            // resolved by `maybe_resolve_scheme`
            _ => unreachable!(),
        });

    let domain = match request.uri().host() {
        Some(d) => d.to_string(),
        None => {
            return Err(ConnectionError::Http(HttpError::invalid_url(
                request.uri().to_string(),
                Some("Malformatted URI. Missing host".into()),
            )));
        }
    };

    let host = format!("{}:{}", domain, port);
    let stream = build_stream(&host, stream_type).await?;

    match ratchet::subscribe_with(
        WebSocketConfig::default(),
        stream,
        request,
        compression_level,
        ProtocolRegistry::new(vec![WARP0_PROTO])
            .expect("Failed to build WebSocket protocol registry"),
    )
    .await
    {
        Ok(sock) => Ok(sock.into_websocket()),
        Err(e) => Err(e.into()),
    }
}
