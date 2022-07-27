// Copyright 2015-2021 Swim Inc.
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

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use either::Either;
use futures::{
    future::{join, join_all},
    pin_mut, Future, Sink, SinkExt, Stream, StreamExt,
};
use smallvec::SmallVec;
use swim_messages::compat::{
    Path, RawRequestMessageEncoder, RawResponseMessageEncoder, RequestMessage, ResponseMessage,
};
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    trigger,
};
use swim_warp::envelope::{peel_envelope_header_str, RawEnvelope};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::FramedWrite;
use uuid::Uuid;

use crate::error::LaneNotFound;

pub enum AttachClient {
    OneWay {
        receiver: ByteReader,
    },
    AttachDownlink {
        node: Text,
        lane: Text,
        sender: ByteWriter,
        receiver: ByteReader,
    },
}

pub struct FindNode {
    node: Text,
    provider: oneshot::Sender<Result<(ByteWriter, ByteReader), LaneNotFound>>,
}

pub struct RemoteTask<In, Out> {
    id: Uuid,
    stop_signal: trigger::Receiver,
    input: In,
    output: Out,
    attach_rx: mpsc::Receiver<AttachClient>,
    find_tx: mpsc::Sender<FindNode>,
}

impl<In, Out> RemoteTask<In, Out> {
    pub fn new(
        id: Uuid,
        stop_signal: trigger::Receiver,
        input: In,
        output: Out,
        attach_rx: mpsc::Receiver<AttachClient>,
        find_tx: mpsc::Sender<FindNode>,
    ) -> Self {
        RemoteTask {
            id,
            stop_signal,
            input,
            output,
            attach_rx,
            find_tx,
        }
    }
}

enum IncomingEvent<M> {
    Register(RegisterIncoming),
    Message(Result<M, std::io::Error>),
}

enum OutgoingEvent {
    Message(OutgoingTaskMessage),
}

impl<In, Out, T> RemoteTask<In, Out>
where
    In: Stream<Item = Result<T, std::io::Error>>,
    T: AsRef<str>,
    Out: Sink<String, Error = std::io::Error>,
{
    pub async fn run(self) {
        let RemoteTask {
            id,
            mut stop_signal,
            input,
            mut output,
            attach_rx,
            find_tx,
        } = self;

        let (incoming_tx, incoming_rx) = mpsc::channel(1);
        let (outgoing_tx, outgoing_rx) = mpsc::channel(1);

        let reg = registration_task(
            attach_rx,
            incoming_tx,
            outgoing_tx.clone(),
            stop_signal.clone(),
        );
        let in_task = incoming_task(
            id,
            stop_signal.clone(),
            input,
            incoming_rx,
            find_tx,
            outgoing_tx,
        );
        todo!()
    }
}

struct RegisterIncoming {
    node: Text,
    lane: Text,
    sender: ByteWriter,
}

enum OutgoingTaskMessage {
    RegisterOutgoing {
        receiver: ByteReader,
    },
    NotFound {
        error: LaneNotFound,
    }
}

async fn registration_task<F>(
    rx: mpsc::Receiver<AttachClient>,
    incoming_tx: mpsc::Sender<RegisterIncoming>,
    outgoing_tx: mpsc::Sender<OutgoingTaskMessage>,
    combined_stop: F,
) where
    F: Future + Unpin,
{
    let mut requests = ReceiverStream::new(rx).take_until(combined_stop);
    while let Some(request) = requests.next().await {
        match request {
            AttachClient::AttachDownlink {
                node,
                lane,
                sender,
                receiver,
            } => {
                if let (Ok(in_res), Ok(out_res)) =
                    join(incoming_tx.reserve(), outgoing_tx.reserve()).await
                {
                    in_res.send(RegisterIncoming { node, lane, sender });
                    out_res.send(OutgoingTaskMessage::RegisterOutgoing { receiver });
                } else {
                    break;
                }
            }
            AttachClient::OneWay { receiver } => {
                if outgoing_tx
                    .send(OutgoingTaskMessage::RegisterOutgoing { receiver })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

const DL_SOFT_CAP: usize = 1;

type ResponseWriters = SmallVec<[ResponseWriter; DL_SOFT_CAP]>;



async fn outgoing_task<Out>(
    mut stop_signal: trigger::Receiver,
    mut output: Out,
    mut messages_rx: mpsc::Receiver<OutgoingTaskMessage>,
)
where
    Out: Sink<String, Error = std::io::Error>,
{
    loop {
        let event = tokio::select! {
            _ = &mut stop_signal => break,
            Some(message) = messages_rx.recv() => OutgoingEvent::Message(message),
            else => break,
        };
        match event {
            OutgoingEvent::Message(OutgoingTaskMessage::RegisterOutgoing { receiver }) => {
                todo!()
            },
            OutgoingEvent::Message(OutgoingTaskMessage::NotFound { error }) => {
                todo!()
            },
        }
    }
    
}

async fn incoming_task<In, T>(
    id: Uuid,
    mut stop_signal: trigger::Receiver,
    input: In,
    mut attach_rx: mpsc::Receiver<RegisterIncoming>,
    find_tx: mpsc::Sender<FindNode>,
    outgoing_tx: mpsc::Sender<OutgoingTaskMessage>,
) where
    In: Stream<Item = Result<T, std::io::Error>>,
    T: AsRef<str>,
{
    pin_mut!(input);

    let mut client_subscriptions: HashMap<Text, HashMap<Text, ResponseWriters>> = HashMap::new();
    let mut agent_routes: HashMap<Text, RequestWriter> = HashMap::new();

    loop {
        let event: IncomingEvent<T> = tokio::select! {
            biased;
            _ = &mut stop_signal => break,
            Some(request) = attach_rx.recv() => IncomingEvent::Register(request),
            Some(result) = input.next() => IncomingEvent::Message(result),
            else => break,
        };

        match event {
            IncomingEvent::Register(RegisterIncoming { node, lane, sender }) => {
                client_subscriptions
                    .entry(node)
                    .or_default()
                    .entry(lane)
                    .or_default()
                    .push(FramedWrite::new(sender, Default::default()));
            }
            IncomingEvent::Message(Ok(frame)) => {
                let body = frame.as_ref();
                if let Ok(envelope) = peel_envelope_header_str(body) {
                    match interpret_envelope(id, envelope) {
                        Some(Either::Left(request)) => {
                            let node = &request.path.node;
                            if let Some(writer) = if let Some(writer) = agent_routes.get_mut(node.as_ref()) {
                                Some(writer)
                            } else {
                                match connect_agent_route(Text::new(node.as_ref()), &find_tx, &outgoing_tx).await {
                                    Ok(Some(writer)) => {
                                        let writer = agent_routes.entry(Text::new(node.as_ref())).or_insert_with_key(move |_| writer);
                                        Some(writer)
                                    },
                                    Err(_) => break,
                                    _ => None,
                                }
                            } {
                                if writer.send(request).await.is_err() {
                                    todo!("Log failed agent. Retry?");
                                }
                            }
                        }
                        Some(Either::Right(response)) => {
                            let Path { node, lane } = response.path.clone();
                            if let Some(node_map) = client_subscriptions.get_mut(node.as_ref()) {
                                if let Some(senders) = node_map.get_mut(lane.as_ref()) {
                                    if !send_response(senders, response).await {
                                        node_map.remove(lane.as_ref());
                                        if node_map.is_empty() {
                                            client_subscriptions.remove(node.as_ref());
                                        }
                                    }
                                } else {
                                    todo!("Log unexpected envelope.");
                                };
                            } else {
                                todo!("Log unexpected envelope.");
                            }
                        }
                        _ => {
                            //TODO Auth and Deauth not implemented.
                        }
                    }
                } else {
                    todo!("Log error.");
                }
            }
            IncomingEvent::Message(Err(_)) => {
                // todo!("Log error.");
                break;
            }
        }
    }
}

async fn connect_agent_route(
    node: Text,
    find_tx: &mpsc::Sender<FindNode>,
    outgoing_tx: &mpsc::Sender<OutgoingTaskMessage>,
) -> Result<Option<RequestWriter>, ()> {
    let (tx, rx) = oneshot::channel();
    let find = FindNode { node, provider: tx };
    find_tx.send(find).await.map_err(|_| ())?;
    match rx.await {
        Ok(Ok((writer, reader))) => {
            outgoing_tx.send(OutgoingTaskMessage::RegisterOutgoing { receiver: reader }).await
                .map(move |_| Some(RequestWriter::new(writer, Default::default())))
                .map_err(|_| ())
        }
        Ok(Err(error)) => {
            outgoing_tx.send(OutgoingTaskMessage::NotFound { error }).await
                .map(move |_| None)
                .map_err(|_| ())
        }
        _ => Err(()),
    }
}

async fn send_response(senders: &mut ResponseWriters, message: RawResponse<'_>) -> bool {
    let messages_clones = std::iter::repeat(message);
    let failed = join_all(senders.iter_mut().zip(messages_clones).enumerate().map(
        |(i, (sender, msg))| async move {
            if sender.send(msg).await.is_err() {
                Some(i)
            } else {
                None
            }
        },
    ))
    .await
    .into_iter()
    .filter_map(std::convert::identity)
    .collect::<HashSet<_>>();
    if failed.is_empty() {
        true
    } else {
        let filtered = std::mem::take(senders)
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !failed.contains(i))
            .map(|(_, tx)| tx);
        senders.extend(filtered);
        !senders.is_empty()
    }
}

type RawRequest<'a> = RequestMessage<Cow<'a, str>, &'a str>;
type RawResponse<'a> = ResponseMessage<Cow<'a, str>, &'a str, &'a str>;

fn interpret_envelope(
    id: Uuid,
    envelope: RawEnvelope<'_>,
) -> Option<Either<RawRequest<'_>, RawResponse<'_>>> {
    match envelope {
        RawEnvelope::Link {
            node_uri, lane_uri, ..
        } => Some(Either::Left(RequestMessage::link(
            id,
            Path::new(node_uri, lane_uri),
        ))),
        RawEnvelope::Sync {
            node_uri, lane_uri, ..
        } => Some(Either::Left(RequestMessage::sync(
            id,
            Path::new(node_uri, lane_uri),
        ))),
        RawEnvelope::Unlink {
            node_uri, lane_uri, ..
        } => Some(Either::Left(RequestMessage::unlink(
            id,
            Path::new(node_uri, lane_uri),
        ))),
        RawEnvelope::Command {
            node_uri,
            lane_uri,
            body,
        } => Some(Either::Left(RequestMessage::command(
            id,
            Path::new(node_uri, lane_uri),
            *body,
        ))),
        RawEnvelope::Linked {
            node_uri, lane_uri, ..
        } => Some(Either::Right(ResponseMessage::linked(
            id,
            Path::new(node_uri, lane_uri),
        ))),
        RawEnvelope::Synced {
            node_uri, lane_uri, ..
        } => Some(Either::Right(ResponseMessage::synced(
            id,
            Path::new(node_uri, lane_uri),
        ))),
        RawEnvelope::Unlinked {
            node_uri,
            lane_uri,
            body,
        } => {
            let unlinked_body = if body.is_empty() { Some(*body) } else { None };
            Some(Either::Right(ResponseMessage::unlinked(
                id,
                Path::new(node_uri, lane_uri),
                unlinked_body,
            )))
        }
        RawEnvelope::Event {
            node_uri,
            lane_uri,
            body,
        } => Some(Either::Right(ResponseMessage::event(
            id,
            Path::new(node_uri, lane_uri),
            *body,
        ))),
        _ => None,
    }
}

type ResponseWriter = FramedWrite<ByteWriter, RawResponseMessageEncoder>;
type RequestWriter = FramedWrite<ByteWriter, RawRequestMessageEncoder>;
