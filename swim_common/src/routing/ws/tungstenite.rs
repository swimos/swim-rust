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

use futures::FutureExt;
use futures_util::future::BoxFuture;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::routing::error::{ConnectionError, TError};
use crate::routing::ws::WsMessage;
use crate::routing::ws::{
    CloseCode, CloseReason, JoinedStreamSink, TransformedStreamSink, WsConnections,
};
use crate::routing::{HttpError, HttpErrorKind, TungsteniteError};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode as TungCloseCode;

pub type TransformedWsStream<S> =
    TransformedStreamSink<WebSocketStream<S>, Message, WsMessage, TError, ConnectionError>;

const DEFAULT_CLOSE_MSG: &str = "Closing connection";

pub struct TungsteniteWsConnections {
    // external: TokioNetworking,
    pub config: WebSocketConfig,
}

impl<S> JoinedStreamSink<Message, TError> for WebSocketStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    type CloseFut = BoxFuture<'static, Result<(), TError>>;

    fn close(mut self, reason: Option<CloseReason>) -> Self::CloseFut {
        async move {
            let (code, reason) = match reason {
                Some(CloseReason {
                    code: CloseCode::GoingAway,
                    ..
                }) => (TungCloseCode::Away, None),
                Some(CloseReason {
                    code: CloseCode::ProtocolError,
                    reason,
                }) => (TungCloseCode::Protocol, Some(reason.into())),
                _ => (TungCloseCode::Abnormal, None),
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
        ConnectionError::from(TungsteniteError(e))
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
                    if response.status().is_success() || response.status().is_informational() {
                        Ok(TransformedStreamSink::new(stream))
                    } else {
                        Err(ConnectionError::Http(HttpError::new(
                            HttpErrorKind::StatusCode(Some(response.status())),
                            None,
                        )))
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
