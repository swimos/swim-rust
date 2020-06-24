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

use futures::future::ErrInto as FutErrInto;
use futures::stream::{SplitSink, SplitStream, StreamExt};
use tokio::net::TcpStream;
use tokio_tls::TlsStream;
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::Error;
use tokio_tungstenite::*;
use url::Url;

use common::request::request_future::SendAndAwait;

use super::async_factory;
use common::connections::error::{ConnectionError, WebSocketError};
use common::connections::{WebsocketFactory, WsMessage};
use std::io::ErrorKind;
use std::ops::Deref;
use utilities::errors::FlattenErrors;
use utilities::future::{TransformMut, TransformedSink, TransformedStream};

type TungSink = TransformedSink<SplitSink<WsConnection, Message>, SinkTransformer>;
type TungStream = TransformedStream<SplitStream<WsConnection>, StreamTransformer>;
type ConnectionFuture = SendAndAwait<ConnReq, Result<(TungSink, TungStream), ConnectionError>>;

pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;
pub type WsConnection = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type ConnReq = async_factory::ConnReq<TungSink, TungStream>;

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
                _ => Err(ConnectionError::ReceiveMessageError),
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

    match connect_async(url).await.map_err(TungsteniteError) {
        Ok((ws_str, _)) => {
            let (tx, rx) = ws_str.split();
            let transformed_sink = TransformedSink::new(tx, SinkTransformer);
            let transformed_stream = TransformedStream::new(rx, StreamTransformer);

            Ok((transformed_sink, transformed_stream))
        }
        Err(e) => {
            match &*e {
                Error::Url(m) => {
                    // Malformatted URL, permanent error
                    tracing::error!(cause = %m, "Failed to connect to the host due to an invalid URL");
                    Err(e.into())
                }
                Error::Io(io_err) => {
                    // todo: This should be considered a fatal error. How should it be handled?
                    tracing::error!(cause = %io_err, "IO error when attempting to connect to host");
                    Err(e.into())
                }
                Error::Tls(tls_err) => {
                    // Apart from any WouldBock, SSL session closed, or retry errors, these seem to be unrecoverable errors
                    tracing::error!(cause = %tls_err, "IO error when attempting to connect to host");
                    Err(e.into())
                }
                Error::Protocol(m) => {
                    tracing::error!(cause = %m, "A protocol error occured when connecting to host");
                    Err(e.into())
                }
                Error::Http(code) => {
                    // todo: This should be expanded and determined if it is possibly a transient error
                    // but for now it will suffice
                    tracing::error!(status_code = %code, "HTTP error when connecting to host");
                    Err(e.into())
                }
                Error::HttpFormat(http_err) => {
                    // This should be expanded and determined if it is possibly a transient error
                    // but for now it will suffice
                    tracing::error!(cause = %http_err, "HTTP error when connecting to host");
                    Err(e.into())
                }
                e => {
                    // Transient or unreachable errors
                    tracing::error!(cause = %e, "Failed to connect to URL");
                    // Err(e.into())
                    unimplemented!()
                }
            }
        }
    }
}

type TError = tungstenite::error::Error;
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
                ConnectionError::SendMessageError
            }
            _ => ConnectionError::ConnectError,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::configuration::router::ConnectionPoolParams;
    use crate::connections::factory::tungstenite::TungsteniteWsFactory;
    use crate::connections::{ConnectionPool, SwimConnPool};

    use common::connections::error::ConnectionError;

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
