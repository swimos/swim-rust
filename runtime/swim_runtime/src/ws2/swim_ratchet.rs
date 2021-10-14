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

use crate::ws2::sender::{boxed, BoxWebSocketSender, FnMutSender, WebSocketSender};
use bytes::BytesMut;
use futures::stream::{unfold, BoxStream};
use futures::{Future, Stream, StreamExt, TryFutureExt};
use ratchet::{
    CloseReason, Error, ExtensionDecoder, ExtensionEncoder, Message, NoExtProvider,
    ProtocolRegistry, WebSocketConfig, WebSocketStream,
};
use tokio::io::{AsyncRead, AsyncWrite};

type BoxedSender = BoxWebSocketSender<Message, CloseReason, Error>;
type ConnectResult = Result<(BoxedSender, BoxStream<'static, Result<Message, Error>>), Error>;

pub fn for_ratchet_sender<S, E>(
    tx: ratchet::Sender<S, E>,
) -> impl WebSocketSender<Message, CloseReason, Error>
where
    S: WebSocketStream + Send + 'static,
    E: ExtensionEncoder + Send + 'static,
{
    FnMutSender::new(tx, ratchet_send_op, ratchet_close_op)
}

fn ratchet_send_op<'a, S, E>(
    tx: &'a mut ratchet::Sender<S, E>,
    _t: Message,
) -> impl Future<Output = Result<(), Error>> + Send + 'a
where
    S: WebSocketStream + Send + 'static,
    E: ExtensionEncoder + Send + 'static,
{
    tx.write_text("test").map_err(Into::into)
}

fn ratchet_close_op<'a, S, E>(
    tx: ratchet::Sender<S, E>,
    _reason: CloseReason,
) -> impl Future<Output = Result<(), Error>> + Send + 'a
where
    S: WebSocketStream + Send + 'static,
    E: ExtensionEncoder + Send + 'static,
{
    tx.close(None).map_err(Into::into)
}

pub async fn open_connection<S>(config: WebSocketConfig, stream: S, addr: String) -> ConnectResult
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let (tx, rx) = ratchet::subscribe_with(
        config,
        stream,
        addr,
        NoExtProvider,
        ProtocolRegistry::default(),
    )
    .await?
    .into_websocket()
    .split()?;

    Ok((boxed(for_ratchet_sender(tx)), into_stream(rx).boxed()))
}

pub async fn accept_connection<S>(config: WebSocketConfig, stream: S) -> ConnectResult
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let (tx, rx) = ratchet::accept_with(stream, config, NoExtProvider, ProtocolRegistry::default())
        .await?
        .upgrade()
        .await?
        .into_websocket()
        .split()?;

    Ok((boxed(for_ratchet_sender(tx)), into_stream(rx).boxed()))
}

fn into_stream<S, E>(rx: ratchet::Receiver<S, E>) -> impl Stream<Item = Result<Message, Error>>
where
    S: WebSocketStream,
    E: ExtensionDecoder,
{
    unfold(ReceiverStream::new(rx), |mut rx| async move {
        match rx.read().await {
            Ok(item) => Some((Ok(item), rx)),
            Err(_) => {
                unimplemented!()
            }
        }
    })
}

pub struct ReceiverStream<S, E> {
    _inner: ratchet::Receiver<S, E>,
    _buf: BytesMut,
}

impl<S, E> ReceiverStream<S, E>
where
    S: WebSocketStream,
    E: ExtensionDecoder,
{
    pub fn new(inner: ratchet::Receiver<S, E>) -> Self {
        ReceiverStream {
            _inner: inner,
            _buf: BytesMut::default(),
        }
    }

    async fn read(&mut self) -> Result<Message, Error> {
        unimplemented!()
    }
}
