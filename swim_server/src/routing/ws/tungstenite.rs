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

use futures::FutureExt;
use futures_util::future::BoxFuture;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::WebSocketStream;

use crate::routing::error::ConnectionError;
use crate::routing::ws::{CloseReason, JoinedStreamSink, TransformedStreamSink, WsConnections};
use swim_common::ws::error::WebSocketError;
use swim_common::ws::protocol::WsMessage;
use tokio::io::{AsyncRead, AsyncWrite};

type TError = tokio_tungstenite::tungstenite::Error;
type TransformedWsStream<S> =
    TransformedStreamSink<WebSocketStream<S>, Message, WsMessage, TError, ConnectionError>;

const DEFAULT_CLOSE_MSG: &str = "Closing connection";

pub struct TungsteniteWsConnections {
    // external: TokioNetworking,
    config: WebSocketConfig,
}

impl<S> JoinedStreamSink<Message, TError> for WebSocketStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    type CloseFut = BoxFuture<'static, Result<(), TError>>;

    fn close(mut self, reason: Option<CloseReason>) -> Self::CloseFut {
        async move {
            let (code, reason) = match reason {
                Some(CloseReason::GoingAway) => (CloseCode::Away, None),
                Some(CloseReason::ProtocolError(e)) => (CloseCode::Protocol, Some(e.into())),
                None => (CloseCode::Abnormal, None),
            };

            let close_frame = CloseFrame {
                code,
                reason: reason.unwrap_or_else(|| DEFAULT_CLOSE_MSG.into()),
            };

            WebSocketStream::close(&mut self, Some(close_frame)).await
        }
        .boxed()
    }
}

impl From<TError> for ConnectionError {
    fn from(e: TError) -> Self {
        match e {
            TError::AlreadyClosed | TError::ConnectionClosed => ConnectionError::Closed,
            TError::Url(url) => ConnectionError::Websocket(WebSocketError::Url(url.to_string())),
            TError::HttpFormat(err) => ConnectionError::Warp(err.to_string()),
            TError::Http(response) => ConnectionError::Http(response.status()),
            TError::Io(e) => ConnectionError::Socket(e.kind()),
            Error::Tls(e) => ConnectionError::Websocket(WebSocketError::Tls(e.to_string())),
            Error::Protocol(_) => ConnectionError::Websocket(WebSocketError::Protocol),
            Error::SendQueueFull(_) | Error::Capacity(_) => {
                ConnectionError::Websocket(WebSocketError::Capacity)
            }
            Error::Utf8 => ConnectionError::Websocket(WebSocketError::Protocol),
            Error::ExtensionError(e) => {
                ConnectionError::Websocket(WebSocketError::Extension(e.to_string()))
            }
        }
    }
}

impl<S> WsConnections<S> for TungsteniteWsConnections
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    type StreamSink = TransformedWsStream<S>;
    type Fut = BoxFuture<'static, Result<Self::StreamSink, ConnectionError>>;

    fn open_connection(&self, stream: S, addr: String) -> Self::Fut {
        let TungsteniteWsConnections { config, .. } = self;
        let config = *config;

        async move {
            let connect_result =
                tokio_tungstenite::client_async_with_config(addr, stream, Some(config)).await;
            match connect_result {
                Ok((stream, response)) => {
                    if response.status().is_success() {
                        Ok(TransformedStreamSink::new(stream))
                    } else {
                        Err(ConnectionError::Http(response.status()))
                    }
                }

                Err(e) => Err(e.into()),
            }
        }
        .boxed()
    }

    fn accept_connection(&self, stream: S) -> Self::Fut {
        let TungsteniteWsConnections { config, .. } = self;
        let config = *config;

        async move {
            let accept_result =
                tokio_tungstenite::accept_async_with_config(stream, Some(config)).await;
            match accept_result {
                Ok(stream) => Ok(TransformedStreamSink::new(stream)),
                Err(e) => Err(e.into()),
            }
        }
        .boxed()
    }
}
