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
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::extensions::uncompressed::UncompressedExt;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use url::Url;

use swim_common::ws::WsMessage;

use crate::routing::error::ConnectionError;
use crate::routing::ws::{CloseReason, JoinedStreamSink, TransformedStreamSink, WsConnections};
use swim_common::ws::error::WebSocketError;

type TError = tokio_tungstenite::tungstenite::Error;
type TransformedWsStream<S, E> =
    TransformedStreamSink<WebSocketStream<S, E>, Message, WsMessage, TError, ConnectionError>;

const DEFAULT_CLOSE_MSG: &str = "Closing connection";

pub struct TungsteniteWsConnections {
    config: WebSocketConfig,
}

impl JoinedStreamSink<Message, TError> for WebSocketStream<TcpStream, UncompressedExt> {
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
            TError::ConnectionClosed => ConnectionError::Closed,
            TError::Url(url) => ConnectionError::Websocket(WebSocketError::Url(url.to_string())),
            TError::HttpFormat(err) => ConnectionError::Warp(err.to_string()),
            TError::Http(code) => ConnectionError::Http(code),
            TError::Io(e) => ConnectionError::Socket(e.kind()),
            e => ConnectionError::Message(e.to_string()),
        }
    }
}

/*
   todo: once the new tungstenite deflate API has been merged, provide the config to Tungstenite
    and remove the extension type parameters.

   todo: URL specific configurations
*/
impl WsConnections<TcpStream> for TungsteniteWsConnections {
    type StreamSink = TransformedWsStream<TcpStream, UncompressedExt>;
    type Fut = BoxFuture<'static, Result<Self::StreamSink, ConnectionError>>;

    fn open_connection(&self, socket: TcpStream, url: Url) -> Self::Fut {
        let TungsteniteWsConnections { config: _config } = self;
        async {
            let connect_result =
                tokio_tungstenite::client_async_with_config(url, socket, None).await;
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

    fn accept_connection(&self, socket: TcpStream) -> Self::Fut {
        let TungsteniteWsConnections { config: _config } = self;
        async {
            let accept_result = tokio_tungstenite::accept_async_with_config(socket, None).await;
            match accept_result {
                Ok(stream) => Ok(TransformedStreamSink::new(stream)),
                Err(e) => Err(e.into()),
            }
        }
        .boxed()
    }
}
