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

use std::fs::File;
use std::io::ErrorKind;
use std::io::{BufReader, Read};
use std::ops::Deref;

use futures::future::ErrInto as FutErrInto;
use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use http::{Request, Response, Uri};
use native_tls::Certificate;
use native_tls::TlsConnector;
use tokio::net::TcpStream;
use tokio_tls::{TlsConnector as TokioTlsConnector, TlsStream};
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::*;
use tokio_tungstenite::{client_async_with_config, WebSocketStream};
use url::Url;

use swim_common::request::request_future::SendAndAwait;
use swim_common::ws::error::{ConnectionError, WebSocketError};
use swim_common::ws::{
    maybe_resolve_scheme, StreamType, WebSocketConfig, WebsocketFactory, WsMessage,
};
use utilities::errors::FlattenErrors;
use utilities::future::{TransformMut, TransformedSink, TransformedStream};

use super::async_factory;
use std::env;

type TungSink = TransformedSink<SplitSink<WsConnection, Message>, SinkTransformer>;
type TungStream = TransformedStream<SplitStream<WsConnection>, StreamTransformer>;
type ConnectionFuture = SendAndAwait<ConnReq, Result<(TungSink, TungStream), ConnectionError>>;

pub type WsConnection = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type ConnReq = async_factory::ConnReq<TungSink, TungStream>;

pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;

async fn connect(
    url: Url,
    config: WebSocketConfig,
) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response<()>), ConnectionError> {
    let uri: Uri = url.as_str().parse()?;
    let request = Request::get(uri).body(())?;

    let request = maybe_resolve_scheme(request)?;
    let stream_type = get_stream_type(&request, &config)?;

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
        Ok(r) => Ok(r),
        Err(e) => Err(e.into()),
    }
}

fn get_stream_type<T>(
    request: &Request<T>,
    config: &WebSocketConfig,
) -> Result<StreamType, WebSocketError> {
    match request.uri().scheme_str() {
        Some("ws") => Ok(StreamType::Plain),
        Some("wss") => match &config.stream_type {
            StreamType::Plain => Ok(StreamType::Tls(None)),
            s => Ok(s.clone()),
        },
        Some(s) => Err(WebSocketError::unsupported_scheme(s)),
        None => Err(WebSocketError::missing_scheme()),
    }
}

async fn build_stream(
    host: &str,
    domain: String,
    stream_type: StreamType,
) -> Result<MaybeTlsStream<TcpStream>, WebSocketError> {
    let socket = TcpStream::connect(host)
        .await
        .map_err(|e| WebSocketError::Message(e.to_string()))?;

    match stream_type {
        StreamType::Plain => Ok(StreamSwitcher::Plain(socket)),
        StreamType::Tls(tls_config) => {
            println!("{:?}", env::current_dir());

            let mut tls_conn_builder = TlsConnector::builder();

            // todo: make config obj
            if let Some(path) = tls_config {
                let mut reader = BufReader::new(File::open(path).map_err(|e| {
                    println!("{}", e.to_string());
                    WebSocketError::Tls(e.to_string())
                })?);

                let mut buf = vec![];
                reader
                    .read_to_end(&mut buf)
                    .map_err(|e| WebSocketError::Tls(e.to_string()))?;

                match Certificate::from_pem(&buf) {
                    Ok(cert) => {
                        tls_conn_builder.add_root_certificate(cert);
                    }
                    Err(e) => return Err(WebSocketError::Tls(e.to_string())),
                }
            }

            let connector = tls_conn_builder
                .build()
                .map_err(|e| WebSocketError::Tls(e.to_string()))?;
            let stream = TokioTlsConnector::from(connector);
            let connected = stream.connect(&domain, socket).await;

            match connected {
                Ok(s) => Ok(StreamSwitcher::Tls(s)),
                Err(e) => {
                    println!("Connect err: {:?}", e);
                    Err(WebSocketError::Tls(e.to_string()))
                }
            }
        }
    }
}

impl TungsteniteWsFactory {
    /// Create a tungstenite-tokio connection factory where the internal task uses the provided
    /// buffer size.
    pub async fn new(buffer_size: usize) -> TungsteniteWsFactory {
        let inner = async_factory::AsyncFactory::new(buffer_size, open_conn).await;
        TungsteniteWsFactory { inner }
    }
}

impl WebsocketFactory for TungsteniteWsFactory {
    type WsStream = TungStream;
    type WsSink = TungSink;
    type ConnectFut = FlattenErrors<FutErrInto<ConnectionFuture, ConnectionError>>;

    fn connect(&mut self, url: Url) -> Self::ConnectFut {
        self.inner.connect(url)
    }
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
                m => todo!("{}", m),
            },
            Err(_) => Err(ConnectionError::ConnectError),
        }
    }
}

/// Specialized [`AsyncFactory`] that creates tungstenite-tokio connections.
pub struct TungsteniteWsFactory {
    inner: async_factory::AsyncFactory<TungSink, TungStream>,
}

async fn open_conn(url: url::Url) -> Result<(TungSink, TungStream), ConnectionError> {
    tracing::info!("Connecting to URL {:?}", &url);

    let config = WebSocketConfig {
        stream_type: StreamType::Tls(Some("../certificate.cert".into())),
        // extensions: vec![],
    };

    match connect(url, config).await {
        Ok((ws_str, _)) => {
            let (tx, rx) = ws_str.split();
            let transformed_sink = TransformedSink::new(tx, SinkTransformer);
            let transformed_stream = TransformedStream::new(rx, StreamTransformer);

            Ok((transformed_sink, transformed_stream))
        }
        Err(e) => {
            // Error::Url(m) => {
            //     // Malformatted URL, permanent error
            //     tracing::error!(cause = %m, "Failed to connect to the host due to an invalid URL");
            // }
            // Error::Io(io_err) => {
            //     // todo: This should be considered a fatal error. How should it be handled?
            //     tracing::error!(cause = %io_err, "IO error when attempting to connect to host");
            // }
            // Error::Tls(tls_err) => {
            //     // Apart from any WouldBock, SSL session closed, or retry errors, these seem to be unrecoverable errors
            //     tracing::error!(cause = %tls_err, "IO error when attempting to connect to host");
            // }
            // Error::Protocol(m) => {
            //     tracing::error!(cause = %m, "A protocol error occured when connecting to host");
            // }
            // Error::Http(code) => {
            //     // todo: This should be expanded and determined if it is possibly a transient error
            //     // but for now it will suffice
            //     tracing::error!(status_code = %code, "HTTP error when connecting to host");
            // }
            // Error::HttpFormat(http_err) => {
            //     // This should be expanded and determined if it is possibly a transient error
            //     // but for now it will suffice
            //     tracing::error!(cause = %http_err, "HTTP error when connecting to host");
            // }
            // e => {
            //     // Transient or unreachable errors
            //     tracing::error!(cause = %e, "Failed to connect to URL");
            // }

            println!("{}", e.to_string());

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
