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
use native_tls::TlsConnector;
use tokio::net::TcpStream;
use tokio_tls::{TlsConnector as TokioTlsConnector, TlsStream};
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::*;
use tokio_tungstenite::{client_async_with_config, WebSocketStream};
use url::Url;

use swim_common::request::request_future::SendAndAwait;
use swim_common::ws::error::{ConnectionError, WebSocketError};
use swim_common::ws::{maybe_resolve_scheme, Protocol, WebsocketFactory, WsMessage};
use utilities::errors::FlattenErrors;

use super::async_factory;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

type TungSink = TransformedSink<SplitSink<WsConnection, Message>, SinkTransformer>;
type TungStream = TransformedStream<SplitStream<WsConnection>, StreamTransformer>;
type ConnectionFuture = SendAndAwait<ConnReq, Result<(TungSink, TungStream), ConnectionError>>;

pub type WsConnection = WebSocketStream<MaybeTlsStream<TcpStream>, MaybeCompressed>;

pub type ConnReq = async_factory::ConnReq<TungSink, TungStream>;

#[derive(Clone)]
pub enum MaybeCompressed {
    Compressed(DeflateExtension),
    Uncompressed(UncompressedExt),
}

impl Default for MaybeCompressed {
    fn default() -> Self {
        MaybeCompressed::Uncompressed(UncompressedExt::default())
    }
}

#[derive(Debug)]
pub struct CompressionError(String);

impl From<CompressionError> for TError {
    fn from(_: CompressionError) -> Self {
        unimplemented!()
    }
}

impl Display for CompressionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionError")
            .field("error", &self.0)
            .finish()
    }
}

impl WebSocketExtension for MaybeCompressed {
    type Error = CompressionError;

    fn enabled(&self) -> bool {
        match self {
            MaybeCompressed::Uncompressed(ext) => ext.enabled(),
            MaybeCompressed::Compressed(ext) => ext.enabled(),
        }
    }

    fn rsv1(&self) -> bool {
        match self {
            MaybeCompressed::Uncompressed(ext) => ext.rsv1(),
            MaybeCompressed::Compressed(ext) => ext.rsv1(),
        }
    }

    fn on_request<T>(&mut self, request: Request<T>) -> Request<T> {
        match self {
            MaybeCompressed::Uncompressed(ext) => ext.on_request(request),
            MaybeCompressed::Compressed(ext) => ext.on_request(request),
        }
    }

    fn on_response<T>(&mut self, response: &Response<T>) {
        match self {
            MaybeCompressed::Uncompressed(ext) => ext.on_response(response),
            MaybeCompressed::Compressed(ext) => ext.on_response(response),
        }
    }

    fn on_send_frame(&mut self, frame: Frame) -> Result<Frame, Self::Error> {
        match self {
            MaybeCompressed::Uncompressed(ext) => ext
                .on_send_frame(frame)
                .map_err(|e| CompressionError(e.to_string())),
            MaybeCompressed::Compressed(ext) => ext
                .on_send_frame(frame)
                .map_err(|e| CompressionError(e.to_string())),
        }
    }

    fn on_receive_frame(&mut self, frame: Frame) -> Result<Option<Message>, Self::Error> {
        match self {
            MaybeCompressed::Uncompressed(ext) => ext
                .on_receive_frame(frame)
                .map_err(|e| CompressionError(e.to_string())),
            MaybeCompressed::Compressed(ext) => ext
                .on_receive_frame(frame)
                .map_err(|e| CompressionError(e.to_string())),
        }
    }
}

pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;

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

    match client_async_with_config(request, stream, None)
        .await
        .map_err(TungsteniteError)
    {
        Ok((stream, response)) => Ok((stream, response)),
        Err(e) => Err(e.into()),
    }
}

fn get_stream_type<T>(
    request: &Request<T>,
    protocol: &Protocol,
) -> Result<Protocol, WebSocketError> {
    match request.uri().scheme_str() {
        Some("ws") => Ok(Protocol::PlainText),
        Some("wss") => match protocol {
            Protocol::PlainText => Err(WebSocketError::BadConfiguration(
                "Attempted to connect to a secure WebSocket without a TLS configuration".into(),
            )),
            tls => Ok(tls.clone()),
        },
        Some(s) => Err(WebSocketError::unsupported_scheme(s)),
        None => Err(WebSocketError::missing_scheme()),
    }
}

async fn build_stream(
    host: &str,
    domain: String,
    stream_type: Protocol,
) -> Result<MaybeTlsStream<TcpStream>, WebSocketError> {
    let socket = TcpStream::connect(host)
        .await
        .map_err(|e| WebSocketError::Message(e.to_string()))?;

    match stream_type {
        Protocol::PlainText => Ok(StreamSwitcher::Plain(socket)),
        Protocol::Tls(certificate) => {
            let mut tls_conn_builder = TlsConnector::builder();
            tls_conn_builder.add_root_certificate(certificate);

            let connector = tls_conn_builder.build()?;
            let stream = TokioTlsConnector::from(connector);
            let connected = stream.connect(&domain, socket).await;

            match connected {
                Ok(s) => Ok(StreamSwitcher::Tls(s)),
                Err(e) => Err(WebSocketError::Tls(e.to_string())),
            }
        }
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
    pub async fn new(
        buffer_size: usize,
        host_configurations: HashMap<Url, HostConfig>,
    ) -> TungsteniteWsFactory {
        let inner = async_factory::AsyncFactory::new(buffer_size, open_conn).await;

        TungsteniteWsFactory {
            inner,
            host_configurations,
        }
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

use futures_util::core_reexport::fmt::Formatter;
use std::fmt::Display;
use tokio_tungstenite::tungstenite::ext::deflate::{DeflateConfig, DeflateExtension};
use tokio_tungstenite::tungstenite::ext::uncompressed::UncompressedExt;
use tokio_tungstenite::tungstenite::ext::WebSocketExtension;
use tokio_tungstenite::tungstenite::protocol::frame::Frame;
use tokio_tungstenite::tungstenite::Message;
use utilities::future::{TransformMut, TransformedSink, TransformedStream};

/// Specialized [`AsyncFactory`] that creates tungstenite-tokio connections.
pub struct TungsteniteWsFactory {
    inner: async_factory::AsyncFactory<TungSink, TungStream>,
    host_configurations: HashMap<Url, HostConfig>,
}

pub struct SinkTransformer;
impl TransformMut<WsMessage> for SinkTransformer {
    type Out = Message;

    fn transform(&mut self, input: WsMessage) -> Self::Out {
        match input {
            WsMessage::Text(s) => Message::Text(s),
            WsMessage::Binary(v) => Message::Binary(v),
        }
    }
}

pub struct StreamTransformer;
impl TransformMut<Result<Message, TError>> for StreamTransformer {
    type Out = Result<WsMessage, ConnectionError>;

    fn transform(&mut self, input: Result<Message, TError>) -> Self::Out {
        match input {
            Ok(i) => match i {
                Message::Text(s) => Ok(WsMessage::Text(s)),
                Message::Binary(v) => Ok(WsMessage::Binary(v)),
                _ => Err(ConnectionError::ReceiveMessageError),
            },
            Err(_) => Err(ConnectionError::ConnectError),
        }
    }
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

type TError = tungstenite::error::Error;

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
            TungsteniteWsFactory::new(buffer_size, Default::default()).await,
        );

        let url = url::Url::parse("xyz://swim.ai").unwrap();
        let rx = connection_pool
            .request_connection(url, false)
            .await
            .unwrap();

        assert!(matches!(rx.err().unwrap(), ConnectionError::SocketError(_)));
    }
}
