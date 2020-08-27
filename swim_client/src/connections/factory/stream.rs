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

use futures::task::{Context, Poll};
use futures::{ready, Sink, Stream};
use pin_project::*;
use std::pin::Pin;
use swim_common::ws::error::ConnectionError;
use swim_common::ws::{WebSocketHandler, WsMessage};
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::Message;

type TError = tungstenite::error::Error;

#[pin_project(project = TungStreamProj)]
#[derive(Clone)]
pub struct TungStreamHandler<I, H> {
    #[pin]
    inner: I,
    handler: H,
}

impl<I, H> TungStreamHandler<I, H> {
    pub fn wrap(inner: I, handler: H) -> TungStreamHandler<I, H> {
        TungStreamHandler { inner, handler }
    }
}

impl<I, H> Stream for TungStreamHandler<I, H>
where
    I: Stream<Item = Result<Message, TError>>,
    H: WebSocketHandler,
{
    type Item = Result<WsMessage, ConnectionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.inner.poll_next(cx)) {
            Some(Ok(msg)) => {
                let mut ws_message = match msg {
                    Message::Text(s) => WsMessage::Text(s),
                    Message::Binary(v) => WsMessage::Binary(v),
                    _ => unimplemented!(),
                };

                this.handler.on_receive(&mut ws_message)?;

                Poll::Ready(Some(Ok(ws_message)))
            }
            Some(Err(_)) => unimplemented!(),
            _ => Poll::Ready(None),
        }
    }
}

impl<I, H> Sink<WsMessage> for TungStreamHandler<I, H>
where
    I: Sink<Message>,
    H: WebSocketHandler,
{
    type Error = ConnectionError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_ready(cx)
            .map_err(|_| ConnectionError::ConnectError)
    }

    fn start_send(self: Pin<&mut Self>, mut message: WsMessage) -> Result<(), Self::Error> {
        let this = self.project();
        this.handler.on_send(&mut message)?;

        let message = match message {
            WsMessage::Text(s) => Message::Text(s),
            WsMessage::Binary(v) => Message::Binary(v),
        };

        this.inner
            .start_send(message)
            .map_err(|_| ConnectionError::ConnectError)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_flush(cx)
            .map_err(|_| ConnectionError::ConnectError)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        let r = this
            .inner
            .poll_close(cx)
            .map_err(|_| ConnectionError::ConnectError);

        this.handler.on_close();
        r
    }
}
