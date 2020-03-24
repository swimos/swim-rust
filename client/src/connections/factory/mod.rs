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

use super::ConnectionError;
use futures::{Future, Sink, Stream};
use tokio_tungstenite::tungstenite::protocol::Message;

pub trait WebsocketFactory {
    type WsStream: Stream<Item = Result<Message, ConnectionError>> + Send + 'static;
    type WsSink: Sink<Message> + Send + 'static;

    type ConnectFut: Future<Output = Result<(Self::WsSink, Self::WsStream), ConnectionError>>
        + Send
        + 'static;

    fn connect(&mut self, url: url::Url) -> Self::ConnectFut;
}

pub mod tungstenite {
    use common::request::Request;
    use tokio::net::TcpStream;
    use tokio::sync::{mpsc, oneshot};
    use tokio_tls::TlsStream;
    use tokio_tungstenite::*;

    use crate::connections::factory::WebsocketFactory;
    use crate::connections::ConnectionError;
    use common::request::request_future::{RequestFuture, SendAndAwait, Sequenced};
    use futures::future::ErrInto as FutErrInto;
    use futures::stream::{ErrInto as StrErrInto, SplitSink, SplitStream, StreamExt, TryStreamExt};
    use futures::TryFutureExt;
    use tokio::task::JoinHandle;
    use tokio_tungstenite::stream::Stream as StreamSwitcher;
    use tokio_tungstenite::tungstenite::protocol::Message;
    use url::Url;

    pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;

    pub type WsConnection = WebSocketStream<MaybeTlsStream<TcpStream>>;

    type TungWsStream = StrErrInto<SplitStream<WsConnection>, ConnectionError>;
    type TungWsSink = SplitSink<WsConnection, Message>;

    pub struct ConnReq(Request<(TungWsSink, TungWsStream)>, url::Url);

    pub struct TungsteniteWsFactory {
        sender: mpsc::Sender<ConnReq>,
        _task: JoinHandle<()>,
    }

    pub async fn new(buffer_size: usize) -> TungsteniteWsFactory {
        let (tx, rx) = mpsc::channel(buffer_size);
        let task = tokio::task::spawn(factory_task(rx));
        TungsteniteWsFactory {
            sender: tx,
            _task: task,
        }
    }

    async fn factory_task(receiver: mpsc::Receiver<ConnReq>) {
        receiver
            .for_each(|req| async {
                let ConnReq(request, url) = req;
                if let Ok((ws_str, _)) = connect_async(url).await {
                    let (tx, rx) = ws_str.split();
                    let _ = request.send((tx, rx.err_into()));
                }
            })
            .await
    }

    impl WebsocketFactory for TungsteniteWsFactory {
        type WsStream = TungWsStream;
        type WsSink = TungWsSink;
        type ConnectFut =
            FutErrInto<SendAndAwait<ConnReq, (TungWsSink, TungWsStream)>, ConnectionError>;

        fn connect(&mut self, url: Url) -> Self::ConnectFut {
            let (tx, rx) = oneshot::channel();
            let req = ConnReq(Request::new(tx), url);
            let req_fut = RequestFuture::new(self.sender.clone(), req);
            TryFutureExt::err_into::<ConnectionError>(Sequenced::new(req_fut, rx))
        }
    }
}
