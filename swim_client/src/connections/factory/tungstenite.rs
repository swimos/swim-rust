// Copyright 2015-2020 SWIM.AI inc.
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

use std::io::ErrorKind;
use std::ops::Deref;

use futures::future::ErrInto as FutErrInto;
use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use http::{HeaderValue, Request, Response, Uri};
use tokio::net::TcpStream;
use tokio_tls::TlsStream;
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::*;
use tokio_tungstenite::{client_async_with_config, WebSocketStream};
use url::Url;

use super::async_factory;
use crate::connections::factory::stream::{
    build_stream, get_stream_type, MaybeCompressed, SinkTransformer, StreamTransformer,
};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use swim_common::request::request_future::SendAndAwait;
use swim_common::ws::error::{ConnectionError, WebSocketError};
use swim_common::ws::{maybe_resolve_scheme, Protocol, WebsocketFactory};
use tokio_tungstenite::tungstenite::extensions::deflate::DeflateConfig;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use utilities::errors::FlattenErrors;
use utilities::future::{TransformedSink, TransformedStream};

type TungSink = TransformedSink<SplitSink<WsConnection, Message>, SinkTransformer>;
type TungStream = TransformedStream<SplitStream<WsConnection>, StreamTransformer>;
type ConnectionFuture = SendAndAwait<ConnReq, Result<(TungSink, TungStream), ConnectionError>>;

pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;
pub type WsConnection = WebSocketStream<MaybeTlsStream<TcpStream>, MaybeCompressed>;
pub type ConnReq = async_factory::ConnReq<TungSink, TungStream>;

async fn connect(
    url: Url,
    config: &mut HostConfig,
) -> Result<
    (
        WebSocketStream<MaybeTlsStream<TcpStream>, MaybeCompressed>,
        Response<()>,
    ),
    ConnectionError,
> {
    let uri: Uri = url
        .as_str()
        .parse()
        .map_err(|e| ConnectionError::SocketError(WebSocketError::from(e)))?;
    let mut request = Request::get(uri).body(())?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("warp0"));

    let request = maybe_resolve_scheme(request)?;
    let stream_type = get_stream_type(&request, &config.protocol)?;

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
            return Err(WebSocketError::Url(String::from("Malformatted URI. Missing host")).into());
        }
    };

    let host = format!("{}:{}", domain, port);
    let stream = build_stream(&host, domain, stream_type).await?;

    match client_async_with_config(
        request,
        stream,
        Some(WebSocketConfig::default_with_encoder(
            MaybeCompressed::new_from_config(config.compression_config.clone()),
        )),
    )
    .await
    .map_err(TungsteniteError)
    {
        Ok((stream, response)) => Ok((stream, response)),
        Err(e) => Err(e.into()),
    }
}

#[derive(Clone)]
pub enum CompressionConfig {
    Uncompressed,
    Deflate(DeflateConfig),
}

#[derive(Clone)]
pub struct HostConfig {
    pub protocol: Protocol,
    pub compression_config: CompressionConfig,
}

impl TungsteniteWsFactory {
    /// Create a tungstenite-tokio connection factory where the internal task uses the provided
    /// buffer size.
    pub async fn new_with_configs(
        buffer_size: usize,
        host_configurations: HashMap<Url, HostConfig>,
    ) -> TungsteniteWsFactory {
        let inner = async_factory::AsyncFactory::new(buffer_size, open_conn).await;

        TungsteniteWsFactory {
            inner,
            host_configurations,
        }
    }

    pub async fn new(buffer_size: usize) -> TungsteniteWsFactory {
        TungsteniteWsFactory::new_with_configs(buffer_size, Default::default()).await
    }
}

impl WebsocketFactory for TungsteniteWsFactory {
    type WsStream = TungStream;
    type WsSink = TungSink;
    type ConnectFut = FlattenErrors<FutErrInto<ConnectionFuture, ConnectionError>>;

    fn connect(&mut self, url: Url) -> Self::ConnectFut {
        let config = match self.host_configurations.entry(url.clone()) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => v
                .insert(HostConfig {
                    protocol: Protocol::PlainText,
                    compression_config: CompressionConfig::Uncompressed,
                })
                .clone(),
        };

        self.inner.connect_using(url, config)
    }
}

/// Specialized [`AsyncFactory`] that creates tungstenite-tokio connections.
pub struct TungsteniteWsFactory {
    inner: async_factory::AsyncFactory<TungSink, TungStream>,
    host_configurations: HashMap<Url, HostConfig>,
}

async fn open_conn(
    url: url::Url,
    mut config: HostConfig,
) -> Result<(TungSink, TungStream), ConnectionError> {
    tracing::info!("Connecting to URL {:?}", &url);

    match connect(url, &mut config).await {
        Ok((ws_str, _)) => {
            let (tx, rx) = ws_str.split();
            let transformed_sink = TransformedSink::new(tx, SinkTransformer);
            let transformed_stream = TransformedStream::new(rx, StreamTransformer);

            Ok((transformed_sink, transformed_stream))
        }
        Err(e) => {
            tracing::error!(cause = %e, "Failed to connect to URL");
            Err(e)
        }
    }
}

pub type TError = tungstenite::error::Error;

#[derive(Debug)]
struct TungsteniteError(tungstenite::error::Error);

impl Deref for TungsteniteError {
    type Target = TError;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<TungsteniteError> for ConnectionError {
    fn from(e: TungsteniteError) -> Self {
        match e.deref() {
            TError::ConnectionClosed => ConnectionError::Closed,
            TError::Url(url) => ConnectionError::SocketError(WebSocketError::Url(url.to_string())),
            TError::HttpFormat(_) | TError::Http(_) => {
                ConnectionError::SocketError(WebSocketError::Protocol)
            }
            TError::Io(e) if e.kind() == ErrorKind::ConnectionRefused => {
                ConnectionError::ConnectionRefused
            }
            _ => ConnectionError::ConnectError,
        }
    }
}

#[cfg(test)]
mod tests {
    use swim_common::ws::error::ConnectionError;

    use crate::configuration::router::ConnectionPoolParams;
    use crate::connections::factory::tungstenite::TungsteniteWsFactory;
    use crate::connections::{ConnectionPool, SwimConnPool};

    #[tokio::test]
    async fn invalid_protocol() {
        let buffer_size = 5;
        let mut connection_pool = SwimConnPool::new(
            ConnectionPoolParams::default(),
            TungsteniteWsFactory::new(buffer_size).await,
        );

        let url = url::Url::parse("xyz://swim.ai").unwrap();
        let rx = connection_pool
            .request_connection(url, false)
            .await
            .unwrap();

        assert!(matches!(rx.err().unwrap(), ConnectionError::SocketError(_)));
    }
}
