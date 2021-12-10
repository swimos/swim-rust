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

use crate::byte_routing::remote::transport::{AgentChannel, DownlinkChannel};
use crate::byte_routing::selector::Selector;
use crate::compat::{
    AgentMessageDecoder, MessageDecodeError, Notification, Operation, RawResponseMessageDecoder,
    RequestMessage, ResponseMessage,
};
use futures_util::StreamExt;
use ratchet::{ExtensionEncoder, MessageType, WebSocketStream};
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use swim_form::structural::read::from_model::ValueMaterializer;
use swim_model::Value;
use swim_utilities::io::byte_channel::ByteReader;
use swim_utilities::trigger;
use thiserror::Error;
use tokio::select;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Decoder, FramedRead};

enum Message {
    Request(RequestMessage<swim_model::Value>),
    Response(ResponseMessage<bytes::Bytes>),
}

impl From<RequestMessage<Value>> for Message {
    fn from(req: RequestMessage<Value>) -> Self {
        Message::Request(req)
    }
}

impl From<ResponseMessage<bytes::Bytes>> for Message {
    fn from(req: ResponseMessage<bytes::Bytes>) -> Self {
        Message::Response(req)
    }
}

enum WriteEvent {
    AttachDownlink(FramedRead<ByteReader, RawResponseMessageDecoder>),
    AttachAgent(FramedRead<ByteReader, AgentMessageDecoder<Value, ValueMaterializer>>),
    Message(Result<Message, MessageDecodeError>),
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("Transport error: `{0}`")]
    WebSocket(#[from] ratchet::Error),
}

pub async fn task<S, E>(
    mut sender: ratchet::Sender<S, E>,
    chunk_after: NonZeroUsize,
    downlink_channel: DownlinkChannel,
    agent_channel: AgentChannel,
    stop_on: trigger::Receiver,
) -> Result<(), WriteError>
where
    S: WebSocketStream,
    E: ExtensionEncoder,
{
    let chunk_after = chunk_after.get();
    let mut downlink_requests = ReceiverStream::new(downlink_channel).take_until(stop_on.clone());
    let mut agent_requests = ReceiverStream::new(agent_channel).take_until(stop_on.clone());
    let mut downlink_selector = Selector::new(map_reader);
    let mut agent_selector = Selector::new(map_reader);

    loop {
        let action: Option<WriteEvent> = select! {
            ev = downlink_selector.read() => ev.map(|msg| WriteEvent::Message(msg.map(Into::into))),
            ev = agent_selector.read() => ev.map(|msg| WriteEvent::Message(msg.map(Into::into))),
            req = downlink_requests.next() => req.map(WriteEvent::AttachDownlink),
            req = agent_requests.next() => req.map(WriteEvent::AttachAgent),
        };

        match action {
            Some(WriteEvent::Message(result)) => match result {
                Ok(payload) => {
                    write_message(&mut sender, payload, chunk_after).await?;
                }
                Err(e) => {
                    match e {
                        MessageDecodeError::Io(err) if err.kind() == ErrorKind::BrokenPipe => {
                            // The producer has been dropped and will have been removed by the
                            // selector.
                        }
                        e => {
                            // Any error at this point indicates a bug as the events have been
                            // produced locally.
                            panic!("WebSocket write task decode error: {:?}", e);
                        }
                    }
                }
            },
            Some(WriteEvent::AttachAgent(framed)) => agent_selector.attach(framed),
            Some(WriteEvent::AttachDownlink(framed)) => downlink_selector.attach(framed),
            None => break Ok(()),
        }
    }
}

async fn write_message<S, E>(
    sender: &mut ratchet::Sender<S, E>,
    message: Message,
    chunk_after: usize,
) -> Result<(), ratchet::Error>
where
    S: WebSocketStream,
    E: ExtensionEncoder,
{
    let (path, envelope, body) = match message {
        Message::Request(request) => {
            let RequestMessage { path, envelope, .. } = request;
            match envelope {
                Operation::Link => (path, "link", None),
                Operation::Sync => (path, "sync", None),
                Operation::Unlink => (path, "unlink", None),
                Operation::Command(body) => (path, "command", Some(body.to_string())),
            }
        }
        Message::Response(response) => {
            let ResponseMessage { path, envelope, .. } = response;
            match envelope {
                Notification::Linked => (path, "linked", None),
                Notification::Synced => (path, "synced", None),
                Notification::Unlinked => (path, "unlinked", None),
                Notification::Event(body) => (
                    path,
                    "event",
                    Some(String::from_utf8(body.to_vec()).unwrap()),
                ),
            }
        }
    };

    let payload = format!(
        "@{}(node:{}, lane:{}){}",
        envelope,
        path.node,
        path.lane,
        body.unwrap_or_default()
    );

    sender
        .write_fragmented(payload, MessageType::Text, chunk_after)
        .await
}

async fn map_reader<D>(
    mut framed: FramedRead<ByteReader, D>,
) -> Option<Result<(FramedRead<ByteReader, D>, D::Item), D::Error>>
where
    D: Decoder,
{
    framed
        .next()
        .await
        .map(|res| res.map(|item| (framed, item)))
}
