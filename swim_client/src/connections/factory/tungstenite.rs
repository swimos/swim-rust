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

use futures::stream::{SplitSink, SplitStream};
use futures::{FutureExt, StreamExt};
use http::{HeaderValue, Request, Response, Uri};
use tokio::net::TcpStream;
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::{client_async_with_config, WebSocketStream};
use url::Url;

use super::async_factory;
use crate::connections::factory::stream::{
    build_stream, get_stream_type, SinkTransformer, StreamTransformer,
};
use http::header::SEC_WEBSOCKET_PROTOCOL;
use http::uri::InvalidUri;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use swim_runtime::error::{
    ConnectionError, HttpError, HttpErrorKind, InvalidUriError, InvalidUriErrorKind,
    TungsteniteError,
};
use swim_runtime::ws::maybe_resolve_scheme;
use swim_runtime::ws::Protocol;
use swim_runtime::ws::{ConnFuture, WebsocketFactory};
use swim_utilities::future::{TransformedSink, TransformedStream};
use tokio_native_tls::TlsStream;
use tokio_tungstenite::tungstenite::extensions::compression::WsCompression;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;

type TungSink = TransformedSink<SplitSink<WsConnection, Message>, SinkTransformer>;
type TungStream = TransformedStream<SplitStream<WsConnection>, StreamTransformer>;

pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;
pub type WsConnection = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type ConnReq = async_factory::ConnReq<TungSink, TungStream>;

const WARP0_PROTO: &str = "warp0";
const MAX_MESSAGE_SIZE: usize = 64 << 20;

async fn connect(
    url: Url,
    config: &mut HostConfig,
) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response<()>), ConnectionError> {
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
    let mut request = Request::get(uri)
        .body(())
        .map_err(|e| ConnectionError::Http(HttpError::from(e)))?;

    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(WARP0_PROTO),
    );

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
            return Err(ConnectionError::Http(HttpError::invalid_url(
                request.uri().to_string(),
                Some("Malformatted URI. Missing host".into()),
            )));
        }
    };

    let host = format!("{}:{}", domain, port);
    let stream = build_stream(&host, stream_type).await?;

    match client_async_with_config(
        request,
        stream,
        Some(WebSocketConfig {
            compression: config.compression_level,
            ..Default::default()
        }),
    )
    .await
    .map_err(TungsteniteError)
    {
        Ok((stream, response)) => Ok((stream, response)),
        Err(e) => Err(e.into()),
    }
}

#[derive(Clone)]
pub struct HostConfig {
    pub protocol: Protocol,
    pub compression_level: WsCompression,
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

    fn connect(&mut self, url: Url) -> ConnFuture<Self::WsSink, Self::WsStream> {
        let config = match self.host_configurations.entry(url.clone()) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => v
                .insert(HostConfig {
                    protocol: Protocol::PlainText,
                    compression_level: WsCompression::None(Some(MAX_MESSAGE_SIZE)),
                })
                .clone(),
        };

        self.inner.connect_using(url, config).boxed()
    }
}

/// Specialized [`crate::connections::factory::async_factory::AsyncFactory`] that creates
/// tungstenite-tokio connections.
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

#[cfg(test)]
mod tests {
    use crate::configuration::router::ConnectionPoolParams;
    use crate::connections::factory::tungstenite::TungsteniteWsFactory;
    use crate::connections::{ConnectionPool, SwimConnPool};

    use swim_runtime::error::{
        ConnectionError, HttpError, HttpErrorKind, InvalidUriError, InvalidUriErrorKind,
    };

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

        let expected = ConnectionError::Http(HttpError::new(
            HttpErrorKind::InvalidUri(InvalidUriError::new(
                InvalidUriErrorKind::UnsupportedScheme,
                Some("xyz".into()),
            )),
            None,
        ));
        assert_eq!(rx.err().unwrap(), expected);
    }
}
