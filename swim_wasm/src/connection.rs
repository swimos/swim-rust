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
use futures::{FutureExt, StreamExt, TryFutureExt};
use std::io::ErrorKind;
use swim_common::request::Request;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use wasm_bindgen_futures::spawn_local;
use ws_stream_wasm::{WsErr, WsMessage as WasmMessage, WsMeta, WsStream};

use std::ops::Deref;
use swim_common::routing::error::{
    CloseError, CloseErrorKind, ConnectionError, EncodingError, EncodingErrorKind, HttpError,
    HttpErrorKind, InvalidUriError, InvalidUriErrorKind, IoError, StatusCode,
};
use swim_common::routing::ws::{ConnFuture, WebsocketFactory, WsMessage};
use utilities::future::{TransformMut, TransformedSink, TransformedStream};

/// A transformer that converts from a [`swim_common::routing::ws::WsMessage`] to
/// [`ws_stream_wasm::WsMessage`].
pub struct SinkTransformer;
impl TransformMut<WsMessage> for SinkTransformer {
    type Out = WasmMessage;

    fn transform(&mut self, input: WsMessage) -> Self::Out {
        match input {
            WsMessage::Binary(v) => WasmMessage::Binary(v),
            WsMessage::Text(v) => WasmMessage::Text(v),
            m => {
                // todo: Wasm-stream-sink doesn't provide the functionality to handle ping, pong,
                //  and close frames. Reliance on this crate needs to be removed and rewritten.
                panic!("Unable to handle {:?} messages", m)
            }
        }
    }
}

/// A transformer that converts from a [`ws_stream_wasm::WsMessage`] to
/// [`swim_common::routing::ws::WsMessage`].
pub struct StreamTransformer;
impl TransformMut<WasmMessage> for StreamTransformer {
    type Out = Result<WsMessage, ConnectionError>;

    fn transform(&mut self, input: WasmMessage) -> Self::Out {
        match input {
            WasmMessage::Text(s) => Ok(WsMessage::Text(s)),
            WasmMessage::Binary(data) => Ok(WsMessage::Binary(data)),
        }
    }
}

/// A WASM WebSocket connection factory implementation. Each connection request returns a result where
/// the successful variant is a sink and stream for the provided URL, or an appropriate
/// [`ConnectionError`].
pub struct WasmWsFactory {
    pub sender: mpsc::Sender<ConnReq>,
}

pub struct ConnReq {
    request: Request<Result<(WasmWsSink, WasmWsStream), ConnectionError>>,
    url: url::Url,
}

impl WasmWsFactory {
    /// Creates a new WASM WebSocket connection factory using the provided `buffer_size` for message
    /// requests.
    pub fn new(buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size);
        spawn_local(Self::factory_task(rx));

        WasmWsFactory { sender: tx }
    }

    async fn factory_task(mut receiver: mpsc::Receiver<ConnReq>) {
        while let Some(ConnReq { request, url }) = receiver.recv().await {
            let connect_result = WsMeta::connect(url, None).await;

            match connect_result {
                Ok((_ws_meta, ws_stream)) => {
                    let (sink, stream) = ws_stream.split();
                    let transformed_sink = TransformedSink::new(sink, SinkTransformer);
                    let transformed_stream = TransformedStream::new(stream, StreamTransformer);

                    let res = (transformed_sink, transformed_stream);

                    let _ = request.send(Ok(res));
                }
                Err(e) => {
                    let _ = request.send(Err(WsError(e).into()));
                }
            }
        }
    }
}

type WasmWsSink = TransformedSink<SplitSink<WsStream, WasmMessage>, SinkTransformer>;
type WasmWsStream = TransformedStream<SplitStream<WsStream>, StreamTransformer>;

impl WebsocketFactory for WasmWsFactory {
    type WsStream = WasmWsStream;
    type WsSink = WasmWsSink;

    fn connect(&mut self, url: Url) -> ConnFuture<'_, Self::WsSink, Self::WsStream> {
        async move {
            let (tx, rx) = oneshot::channel();
            let req = ConnReq {
                request: Request::new(tx),
                url,
            };

            self.sender.send(req).await.map_err(|_| {
                ConnectionError::Closed(CloseError::new(
                    CloseErrorKind::Unexpected,
                    Some("WebSocket factory closed".into()),
                ))
            })?;
            rx.map_err(|_| {
                ConnectionError::Closed(CloseError::new(CloseErrorKind::Unexpected, None))
            })
            .await?
        }
        .boxed()
    }
}

// A wrapper around the [`ws_stream_wasm::WsErr`] used to implement [`From`] for converting to
// [`ConnectionError`]s.
struct WsError(WsErr);

impl Deref for WsError {
    type Target = WsErr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<WsError> for ConnectionError {
    fn from(e: WsError) -> Self {
        match e.0 {
            WsErr::InvalidUrl { supplied } => ConnectionError::Http(HttpError::new(
                HttpErrorKind::InvalidUri(InvalidUriError::new(
                    InvalidUriErrorKind::Malformatted,
                    Some(supplied),
                )),
                None,
            )),
            WsErr::ConnectionFailed { event: _ } => {
                todo!()
            }
            WsErr::InvalidCloseCode { supplied: _ } => todo!(),
            WsErr::ForbiddenPort => ConnectionError::Http(HttpError::new(
                HttpErrorKind::StatusCode(Some(StatusCode::FORBIDDEN)),
                None,
            )),
            WsErr::ConnectionNotOpen => ConnectionError::Closed(CloseError::new(
                CloseErrorKind::Unexpected,
                Some("No open connection".into()),
            )),
            WsErr::InvalidWsState { supplied } => ConnectionError::Io(IoError::new(
                ErrorKind::InvalidInput,
                Some(format!(
                    "Invalid WebSocket state. Supplied state: {}",
                    supplied
                )),
            )),
            WsErr::ReasonStringToLong => ConnectionError::Io(IoError::new(
                ErrorKind::InvalidInput,
                Some("Supplied close reason was too long".into()),
            )),
            WsErr::InvalidEncoding => {
                ConnectionError::Encoding(EncodingError::new(EncodingErrorKind::Invalid, None))
            }
            WsErr::CantDecodeBlob => {
                ConnectionError::Encoding(EncodingError::new(EncodingErrorKind::Invalid, None))
            }
            WsErr::UnknownDataType => {
                ConnectionError::Encoding(EncodingError::new(EncodingErrorKind::Unsupported, None))
            }
            e => {
                // WsErr is marked with #[non_exhaustive]
                ConnectionError::Io(IoError::new(ErrorKind::Other, Some(format!("{:?}", e))))
            }
        }
    }
}
