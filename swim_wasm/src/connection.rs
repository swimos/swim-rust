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
use futures::stream::{SplitSink, SplitStream};
use futures::{StreamExt, TryFutureExt};
use tokio::sync::{mpsc, oneshot};
use url::Url;
use wasm_bindgen_futures::spawn_local;
use ws_stream_wasm::*;

use common::request::request_future::{RequestFuture, SendAndAwait, Sequenced};
use common::request::Request;

use common::connections::error::{ConnectionError, ConnectionErrorKind};
use common::connections::WebsocketFactory;
use utilities::errors::FlattenErrors;
use utilities::future::{TransformMut, TransformedSink, TransformedStream};

type WasmWsSink = TransformedSink<SplitSink<WsStream, WsMessage>, SinkTransformer>;
type WasmWsStream = TransformedStream<SplitStream<WsStream>, StreamTransformer>;

pub struct SinkTransformer;
impl TransformMut<String> for SinkTransformer {
    type Out = WsMessage;

    fn transform(&mut self, input: String) -> Self::Out {
        WsMessage::Text(input)
    }
}

pub struct StreamTransformer;
impl TransformMut<WsMessage> for StreamTransformer {
    type Out = Result<String, ConnectionError>;

    fn transform(&mut self, input: WsMessage) -> Self::Out {
        match input {
            WsMessage::Text(s) => Ok(s),
            WsMessage::Binary(_) => panic!("Unsupported message type"),
        }
    }
}

pub struct WasmWsFactory {
    pub sender: mpsc::Sender<ConnReq>,
}

pub struct ConnReq {
    request: Request<Result<(WasmWsSink, WasmWsStream), ConnectionError>>,
    url: url::Url,
}

impl WasmWsFactory {
    pub fn new(buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size);
        spawn_local(Self::factory_task(rx));

        WasmWsFactory { sender: tx }
    }

    async fn factory_task(mut receiver: mpsc::Receiver<ConnReq>) {
        while let Some(ConnReq { request, url }) = receiver.next().await {
            let (_ws, wsio) = WsMeta::connect(url, None)
                .await
                .map_err(|_| ConnectionError::new(ConnectionErrorKind::ConnectError))
                .unwrap();

            let (sink, stream) = wsio.split();
            let transformed_sink = TransformedSink::new(sink, SinkTransformer);
            let transformed_stream = TransformedStream::new(stream, StreamTransformer);

            let res = (transformed_sink, transformed_stream);

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
