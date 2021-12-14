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

use crate::byte_routing::message::read_raw_header;
use crate::byte_routing::routing::router::ServerRouter;
use crate::byte_routing::routing::{DispatchError, Dispatcher, RawRoute};
use crate::routing::RoutingAddr;
use crate::ws::{into_stream, WsMessage};
use futures::future::Either;
use futures::Stream;
use futures_util::future::select;
use futures_util::StreamExt;
use pin_utils::pin_mut;
use ratchet::{ExtensionDecoder, WebSocketStream};
use swim_model::path::RelativePath;
use swim_utilities::future::retryable::RetryStrategy;
use swim_utilities::trigger;
use thiserror::Error;

enum ReadEvent {
    Dispatch(WsMessage),
    Register(RelativePath, RawRoute),
    Stop,
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("Dispatch error: `{0}`")]
    Dispatch(#[from] DispatchError),
    #[error("Transport error: `{0}`")]
    WebSocket(#[from] ratchet::Error),
}

pub async fn task<S, D, E>(
    id: RoutingAddr,
    router: ServerRouter,
    receiver: ratchet::Receiver<S, E>,
    mut downlink_rx: D,
    stop_on: trigger::Receiver,
) -> Result<(), ReadError>
where
    S: WebSocketStream,
    D: Stream<Item = (RelativePath, RawRoute)> + Unpin,
    E: ExtensionDecoder,
{
    let mut dispatcher = Dispatcher::new(id, RetryStrategy::default(), router);
    let stream = into_stream(receiver).take_until(stop_on);

    pin_mut!(stream);

    loop {
        match select_next(&mut stream, &mut downlink_rx).await {
            Ok(ReadEvent::Dispatch(msg)) => match msg {
                WsMessage::Text(value) => dispatch_text_message(&mut dispatcher, value).await?,
                WsMessage::Binary(_) => panic_binary(),
                WsMessage::Ping | WsMessage::Pong => {}
                WsMessage::Close(_) => break,
            },
            Ok(ReadEvent::Register(addr, route)) => {
                dispatcher.register_downlink(addr, route);
            }
            Ok(ReadEvent::Stop) => return Ok(()),
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

#[cold]
#[inline(never)]
fn panic_binary() {
    panic!("Received an unsupported binary websocket message")
}

async fn select_next<S, D>(ws_rx: &mut S, downlink_rx: &mut D) -> Result<ReadEvent, ReadError>
where
    S: Stream<Item = Result<WsMessage, ratchet::Error>> + Unpin,
    D: Stream<Item = (RelativePath, RawRoute)> + Unpin,
{
    match select(ws_rx.next(), downlink_rx.next()).await {
        Either::Left((Some(Ok(msg)), _)) => {
            // note: ratchet could be changed to yield continuation frames as opposed to only
            //  yielding the entire message and then the segments could be decoded as they are
            //  delivered. This should only require ratchet_core exporting more and for a wrapper
            //  to be written that still handles control frames and upholds the websocket protocol.
            Ok(ReadEvent::Dispatch(msg))
        }
        Either::Left((Some(Err(e)), _)) => Err(e.into()),
        Either::Right((Some((addr, writer)), _)) => Ok(ReadEvent::Register(addr, writer)),
        Either::Left((None, _)) | Either::Right((None, _)) => Ok(ReadEvent::Stop),
    }
}

async fn dispatch_text_message(
    dispatcher: &mut Dispatcher,
    value: String,
) -> Result<(), DispatchError> {
    let message = read_raw_header(value.as_str())?;
    dispatcher.dispatch(message).await
}
