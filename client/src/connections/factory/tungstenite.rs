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
use futures::stream::{ErrInto as StrErrInto, SplitSink, SplitStream, StreamExt, TryStreamExt};
use tokio::net::TcpStream;
use tokio_tls::TlsStream;
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::Error;
use tokio_tungstenite::*;
use url::Url;

use common::request::request_future::SendAndAwait;

use crate::connections::factory::errors::FlattenErrors;
use crate::connections::factory::WebsocketFactory;
use crate::connections::{ConnectionError, ConnectionErrorKind};

use super::async_factory;
use futures::task::{Context, Poll};
use futures::{Sink, Stream};
use pin_project::pin_project;
use std::pin::Pin;

pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;
pub type WsConnection = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type ConnReq = async_factory::ConnReq<TungWsSink, TungWsStream>;

#[pin_project]
pub struct TungWsSink {
    #[pin]
    inner: SplitSink<WsConnection, Message>,
}

impl Sink<String> for TungWsSink {
    type Error = ConnectionError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_ready(cx)
            .map_err(|_| ConnectionError::new(ConnectionErrorKind::ConnectError))
    }

    fn start_send(self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        self.project()
            .inner
            .start_send(Message::Text(item))
            .map_err(|_| ConnectionError::new(ConnectionErrorKind::ConnectError))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_flush(cx)
            .map_err(|_| ConnectionError::new(ConnectionErrorKind::ConnectError))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_close(cx)
            .map_err(|_| ConnectionError::new(ConnectionErrorKind::ConnectError))
    }
}

#[pin_project]
pub struct TungWsStream {
    #[pin]
    inner: StrErrInto<SplitStream<WsConnection>, ConnectionError>,
}

impl Stream for TungWsStream {
    type Item = Result<String, ConnectionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(m))) => Poll::Ready(Some(Ok(m.to_string()))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
        }
    }
}

/// Specialized [`AsyncFactory`] that creates tungstenite-tokio connections.
pub struct TungsteniteWsFactory {
    inner: async_factory::AsyncFactory<TungWsSink, TungWsStream>,
}

async fn open_conn(url: url::Url) -> Result<(TungWsSink, TungWsStream), ConnectionError> {
    tracing::info!("Connecting to URL {:?}", &url);

    match connect_async(url).await {
        Ok((ws_str, _)) => {
            let (tx, rx) = ws_str.split();
            Ok((
                TungWsSink { inner: tx },
                TungWsStream {
                    inner: rx.err_into::<ConnectionError>(),
                },
            ))
        }
        Err(e) => {
            match &e {
                Error::Url(m) => {
                    // Malformatted URL, permanent error
                    tracing::error!(cause = %m, "Failed to connect to the host due to an invalid URL");
                    Err(e.into())
                }
                Error::Io(io_err) => {
                    // This should be considered a fatal error. How should it be handled?
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
                    // This should be expanded and determined if it is possibly a transient error
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
                    Err(ConnectionError::new(ConnectionErrorKind::ConnectError))
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

type ConnectionFuture = SendAndAwait<ConnReq, Result<(TungWsSink, TungWsStream), ConnectionError>>;

impl WebsocketFactory for TungsteniteWsFactory {
    type WsStream = TungWsStream;
    type WsSink = TungWsSink;
    type ConnectFut = FlattenErrors<FutErrInto<ConnectionFuture, ConnectionError>>;

    fn connect(&mut self, url: Url) -> Self::ConnectFut {
        self.inner.connect(url)
    }
}
