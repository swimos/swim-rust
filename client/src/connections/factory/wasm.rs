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
use futures::sink::SinkErrInto;
use futures::stream::{SplitSink, SplitStream};
use futures::task::{Context, Poll};
use futures::{Sink, Stream, StreamExt, TryFutureExt};
use futures_util::SinkExt;
use pin_project::*;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use wasm_bindgen::__rt::core::pin::Pin;
use wasm_bindgen_futures::spawn_local;
use ws_stream_wasm::*;

use common::request::request_future::{RequestFuture, SendAndAwait, Sequenced};
use common::request::Request;

use crate::connections::factory::errors::FlattenErrors;
use crate::connections::factory::WebsocketFactory;
use crate::connections::{ConnectionError, ConnectionErrorKind};

#[pin_project]
pub struct WasmWsSink {
    #[pin]
    inner: SinkErrInto<SplitSink<WsStream, WsMessage>, WsMessage, ConnectionError>,
}

impl Sink<String> for WasmWsSink {
    type Error = ConnectionError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        self.project().inner.start_send(WsMessage::Text(item))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

#[pin_project]
pub struct WasmWsStream {
    #[pin]
    inner: SplitStream<WsStream>,
}

impl Stream for WasmWsStream {
    type Item = Result<String, ConnectionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(WsMessage::Text(s))) => Poll::Ready(Some(Ok(s))),
            Poll::Ready(Some(WsMessage::Binary(_))) => unimplemented!(),
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

pub struct WasmWsFactory {
    pub(in crate::connections::factory) sender: mpsc::Sender<ConnReq>,
}

pub struct ConnReq {
    request: Request<Result<(WasmWsSink, WasmWsStream), ConnectionError>>,
    url: url::Url,
}

impl From<WsErr> for ConnectionError {
    fn from(_: WsErr) -> Self {
        ConnectionError::new(ConnectionErrorKind::ConnectError)
    }
}

impl WasmWsFactory {
    pub fn new(buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size);
        let _task = spawn_local(Self::factory_task(rx));

        WasmWsFactory { sender: tx }
    }

    async fn factory_task(mut receiver: mpsc::Receiver<ConnReq>) {
        while let Some(ConnReq { request, url }) = receiver.next().await {
            let (_ws, wsio) = WsMeta::connect(url, None)
                .await
                .map_err(|_| ConnectionError::new(ConnectionErrorKind::ConnectError))
                .unwrap();

            let (sink, stream) = wsio.split();
            let res = (
                WasmWsSink {
                    inner: sink.sink_err_into(),
                },
                WasmWsStream { inner: stream },
            );

            let _ = request.send(Ok(res));
        }
    }
}

pub type ConnectionFuture =
    SendAndAwait<ConnReq, Result<(WasmWsSink, WasmWsStream), ConnectionError>>;

impl WebsocketFactory for WasmWsFactory {
    type WsStream = WasmWsStream;
    type WsSink = WasmWsSink;
    type ConnectFut = FlattenErrors<FutErrInto<ConnectionFuture, ConnectionError>>;

    fn connect(&mut self, url: Url) -> Self::ConnectFut {
        let (tx, rx) = oneshot::channel();
        let req = ConnReq {
            request: Request::new(tx),
            url,
        };

        let req_fut = RequestFuture::new(self.sender.clone(), req);

        FlattenErrors::new(TryFutureExt::err_into::<ConnectionError>(Sequenced::new(
            req_fut, rx,
        )))
    }
}
