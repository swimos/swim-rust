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
    num::NonZeroUsize,
    str::Utf8Error,
};

use bytes::BytesMut;
use either::Either;
use futures::{
    future::{join, join_all, ready, select},
    pin_mut,
    stream::{unfold, FuturesUnordered},
    Future, SinkExt, Stream, StreamExt,
};
use ratchet::{
    CloseCode, CloseReason, ExtensionDecoder, ExtensionEncoder, Message, PayloadType,
    SplittableExtension, WebSocket, WebSocketStream,
};
use smallvec::SmallVec;
use swim_api::error::DownlinkFailureReason;
use swim_messages::warp::{peel_envelope_header_str, RawEnvelope};
use swim_messages::{
    bytes_str::BytesStr,
    protocol::{
        BytesRequestMessage, BytesResponseMessage, Path, RawRequestMessageDecoder,
        RawRequestMessageEncoder, RawResponseMessageDecoder, RawResponseMessageEncoder,
        RequestMessage, ResponseMessage,
    },
};
use swim_model::{address::RelativeAddress, Text};
use swim_recon::parser::MessageExtractError;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    multi_reader::MultiReader,
    trigger,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};
use uuid::Uuid;

use tracing::{debug, error, info, info_span, trace, warn};
use tracing_futures::Instrument;

use crate::error::AgentResolutionError;
use crate::NoSuchAgent;

use self::envelopes::ReconEncoder;

mod envelopes;
#[cfg(test)]
mod tests;

/// Message to attach a new client to a socket.
#[derive(Debug)]
pub enum AttachClient {
    /// Attach a send only client.
    OneWay {
        receiver: ByteReader,
        done: trigger::Sender,
    },
    /// Attach a two way (downlink) client.
    AttachDownlink {
        downlink_id: Uuid,
        path: RelativeAddress<Text>,
        sender: ByteWriter,
        receiver: ByteReader,
        done: oneshot::Sender<Result<(), DownlinkFailureReason>>,
    },
}

/// Message type sent by the socket management task to find an agent node.
pub struct FindNode {
    pub source: Uuid,
    pub node: Text,
    pub lane: Text,
    pub provider: oneshot::Sender<Result<(ByteWriter, ByteReader), AgentResolutionError>>,
}

/// A task that manages a socket connection. Incoming envelopes are routed to the appropriate
/// downlink or agent. Agents will be resolved externally where required.
pub struct RemoteTask<S, E> {
    id: Uuid,
    stop_signal: trigger::Receiver,
    ws: WebSocket<S, E>,
    attach_rx: mpsc::Receiver<AttachClient>,
    find_tx: Option<mpsc::Sender<FindNode>>,
    registration_buffer_size: NonZeroUsize,
}

impl<S, E> RemoteTask<S, E> {
    pub fn new(
        id: Uuid,
        stop_signal: trigger::Receiver,
        ws: WebSocket<S, E>,
        attach_rx: mpsc::Receiver<AttachClient>,
        find_tx: Option<mpsc::Sender<FindNode>>,
        registration_buffer_size: NonZeroUsize,
    ) -> Self {
        RemoteTask {
            id,
            stop_signal,
            ws,
            attach_rx,
            find_tx,
            registration_buffer_size,
        }
    }
}

#[derive(Debug)]
enum IncomingEvent<M> {
    Register(RegisterIncoming),
    Message(Result<M, InputError>),
}

#[derive(Debug)]
enum OutgoingEvent {
    Message(OutgoingTaskMessage),
    Request(BytesRequestMessage),
    Response(BytesResponseMessage),
}

#[derive(Debug)]
enum InputError {
    WsError(ratchet::Error),
    BinaryFrame,
    BadUtf8(Utf8Error),
    InvalidEnvelope(MessageExtractError),
    Closed(Option<CloseReason>),
}

const STOPPING: &str = "Server is stopping.";
const BAD_ENCODING: &str = "Invalid encoding.";
const EXPECTED_STR: &str = "Expected string data.";
const BAD_WARP_ENV: &str = "Expected a valid Warp envelope.";

impl<S, E> RemoteTask<S, E>
where
    S: WebSocketStream + Send,
    E: SplittableExtension + Send,
{
    #[allow(clippy::manual_async_fn)] //To allow the static guarantee that this future is Send.
    pub fn run<'a>(self) -> impl Future<Output = ()> + Send + 'a
    where
        S: 'a,
        E: 'a,
    {
        async move { self.run_inner().await }
    }

    async fn run_inner(self) {
        let RemoteTask {
            id,
            stop_signal,
            ws,
            attach_rx,
            find_tx,
            registration_buffer_size,
        } = self;

        let (mut tx, mut rx) = ws.split().unwrap();

        let (kill_switch_tx, kill_switch_rx) = trigger::trigger();
        let (incoming_tx, incoming_rx) = mpsc::channel(registration_buffer_size.get());
        let (outgoing_tx, outgoing_rx) = mpsc::channel(registration_buffer_size.get());

        let combined_stop = select(stop_signal.clone(), kill_switch_rx);
        let reg = registration_task(attach_rx, incoming_tx, outgoing_tx.clone(), combined_stop)
            .instrument(info_span!("Websocket coordination task."));

        let input = text_frame_stream(&mut rx);

        let mut incoming = IncomingTask::new(id);

        let in_task = incoming
            .run(
                stop_signal.clone(),
                input,
                incoming_rx,
                find_tx,
                outgoing_tx,
            )
            .instrument(info_span!("Websocket incoming task.", id = %id));

        let mut outgoing = OutgoingTask::default();
        let out_task = outgoing
            .run(stop_signal, &mut tx, outgoing_rx)
            .instrument(info_span!("Websocket outgoing task."));

        let (_, result) = join(reg, await_io_tasks(in_task, out_task, kill_switch_tx)).await;
        let close_reason = match result {
            Ok(_) => Some(CloseReason::new(
                CloseCode::GoingAway,
                Some(STOPPING.to_string()),
            )),
            Err(InputError::BadUtf8(_)) => Some(CloseReason::new(
                CloseCode::Protocol,
                Some(BAD_ENCODING.to_string()),
            )),
            Err(InputError::BinaryFrame) => Some(CloseReason::new(
                CloseCode::Protocol,
                Some(EXPECTED_STR.to_string()),
            )),
            Err(InputError::InvalidEnvelope(_)) => Some(CloseReason::new(
                CloseCode::Protocol,
                Some(BAD_WARP_ENV.to_string()),
            )),
            _ => None, //Closed remotely or failed.
        };
        if let Some(reason) = close_reason {
            debug!(reason = ?reason, "Closing websocket connection.");
            if let Err(error) = tx.close(reason).await {
                error!(error = %error, "Failed to close websocket.");
            }
        }
    }
}

// Converts a websocket reader into a stream of text frames.
fn text_frame_stream<S, E>(
    rx: &mut ratchet::Receiver<S, E>,
) -> impl Stream<Item = Result<BytesStr, InputError>> + '_
where
    S: WebSocketStream,
    E: ExtensionDecoder,
{
    unfold((Some(rx), BytesMut::new()), |(rx, mut buffer)| async move {
        if let Some(rx) = rx {
            match rx.read(&mut buffer).await {
                Ok(Message::Binary) => {
                    let item = Some(Err(InputError::BinaryFrame));
                    Some((item, (None, buffer)))
                }
                Ok(Message::Text) => {
                    let bytes = buffer.split().freeze();
                    match BytesStr::try_from(bytes) {
                        Ok(string) => {
                            let item = Some(Ok(string));
                            Some((item, (Some(rx), buffer)))
                        }
                        Err(e) => {
                            let item = Some(Err(InputError::BadUtf8(e)));
                            Some((item, (None, buffer)))
                        }
                    }
                }
                Ok(Message::Close(reason)) => {
                    let item = Some(Err(InputError::Closed(reason)));
                    Some((item, (None, buffer)))
                }
                Err(e) => {
                    let item = Some(Err(InputError::WsError(e)));
                    Some((item, (None, buffer)))
                }
                _ => Some((None, (Some(rx), buffer))),
            }
        } else {
            None
        }
    })
    .filter_map(ready)
}

#[derive(Debug)]
struct RegisterIncoming {
    path: RelativeAddress<Text>,
    sender: ByteWriter,
    done: trigger::Sender,
}

#[derive(Debug, PartialEq, Eq)]
enum OutgoingKind {
    Client,
    Server,
}

#[derive(Debug)]
enum OutgoingTaskMessage {
    RegisterOutgoing {
        kind: OutgoingKind,
        receiver: ByteReader,
        done: trigger::Sender,
    },
    NotFound {
        error: AgentResolutionError,
    },
}

// The registration task manages requests to attach new clients and serves as the coordinator between
// the tasks managing the incoming and outgoing halves of the socket. When this task stops, the other
// tasks will also.
async fn registration_task<F>(
    rx: mpsc::Receiver<AttachClient>,
    incoming_tx: mpsc::Sender<RegisterIncoming>,
    outgoing_tx: mpsc::Sender<OutgoingTaskMessage>,
    combined_stop: F,
) where
    F: Future + Unpin,
{
    let mut trackers = FuturesUnordered::new();
    let mut requests = ReceiverStream::new(rx).take_until(combined_stop);
    loop {
        let request = tokio::select! {
            done = trackers.next(), if !trackers.is_empty() => {
                let done: Option<Option<oneshot::Sender<Result<(), DownlinkFailureReason>>>> = done;
                if let Some(Some(done)) = done {
                    if done.send(Ok(())).is_err() {
                        info!("Downlink request dropped before it was satisfied.");
                    };
                }
                continue;
            }
            maybe_request = requests.next() => {
                if let Some(request) = maybe_request {
                    request
                } else {
                    break;
                }
            }
        };

        match request {
            AttachClient::AttachDownlink {
                path,
                sender,
                receiver,
                done,
                ..
            } => {
                debug!(path = %path, "Attaching new client downlink.");
                if let (Ok(in_res), Ok(out_res)) =
                    join(incoming_tx.reserve(), outgoing_tx.reserve()).await
                {
                    let (in_done_tx, in_done_rx) = trigger::trigger();
                    let (out_done_tx, out_done_rx) = trigger::trigger();
                    in_res.send(RegisterIncoming {
                        path,
                        sender,
                        done: in_done_tx,
                    });
                    out_res.send(OutgoingTaskMessage::RegisterOutgoing {
                        kind: OutgoingKind::Client,
                        receiver,
                        done: out_done_tx,
                    });

                    trackers.push(async move {
                        let (res_in, res_out) = join(in_done_rx, out_done_rx).await;
                        if res_in.is_ok() && res_out.is_ok() {
                            Some(done)
                        } else {
                            None
                        }
                    });
                } else {
                    break;
                }
            }
            AttachClient::OneWay { receiver, done } => {
                debug!("Attaching send only client.");
                if outgoing_tx
                    .send(OutgoingTaskMessage::RegisterOutgoing {
                        kind: OutgoingKind::Client,
                        receiver,
                        done,
                    })
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

type RequestReader = FramedRead<ByteReader, RawRequestMessageDecoder>;
type ResponseReader = FramedRead<ByteReader, RawResponseMessageDecoder>;

/// The outgoing tasks writes out envelopes sent by agents and downlinks to the socket.
#[derive(Default)]
struct OutgoingTask {
    clients: MultiReader<RequestReader>,
    agents: MultiReader<ResponseReader>,
}

impl OutgoingTask {
    async fn run<S, E>(
        &mut self,
        mut stop_signal: trigger::Receiver,
        output: &mut ratchet::Sender<S, E>,
        mut messages_rx: mpsc::Receiver<OutgoingTaskMessage>,
    ) where
        S: WebSocketStream,
        E: ExtensionEncoder,
    {
        let OutgoingTask { clients, agents } = self;
        let mut buffer = BytesMut::new();
        let mut recon_encoder = ReconEncoder::default();

        loop {
            let event = tokio::select! {
                _ = &mut stop_signal => break,
                message = messages_rx.recv() => match message {
                    // todo: there seems to be some issue with using 'Some(message)' as the condition
                    //  of this branch as it never triggers any branch (including the else) when the
                    //  value is 'None'
                    Some(message) => OutgoingEvent::Message(message),
                    None => break,
                },
                Some(result) = clients.next(), if !clients.is_empty() => {
                    match result {
                        Ok(request) => OutgoingEvent::Request(request),
                        Err(error) => {
                            info!(error = %error, "Connection from client failed.");
                            continue;
                        }
                    }
                },
                Some(result) = agents.next(), if !agents.is_empty() => {
                    match result {
                        Ok(response) => OutgoingEvent::Response(response),
                        Err(error) => {
                            info!(error = %error, "Connection from agent failed.");
                            continue;
                        }
                    }
                },
                else => break,
            };

            match event {
                OutgoingEvent::Message(OutgoingTaskMessage::RegisterOutgoing {
                    kind,
                    receiver,
                    done,
                }) => {
                    debug!(kind = ?kind, "Registering new outgoing chanel.");
                    match kind {
                        OutgoingKind::Client => {
                            clients.add(RequestReader::new(receiver, Default::default()));
                        }
                        OutgoingKind::Server => {
                            agents.add(ResponseReader::new(receiver, Default::default()));
                        }
                    }
                    done.trigger();
                }
                OutgoingEvent::Message(OutgoingTaskMessage::NotFound {
                    error: AgentResolutionError::NotFound(error),
                }) => {
                    debug!(lane = ?error, "Sending node/lane not found envelope.");
                    buffer.clear();
                    recon_encoder
                        .encode(error, &mut buffer)
                        .expect("Encoding a frame should be infallible.");
                    if let Err(error) = output.write(&buffer, PayloadType::Text).await {
                        error!(error = %error, "Writing to the websocket connection failed.");
                        break;
                    }
                }
                OutgoingEvent::Message(OutgoingTaskMessage::NotFound {
                    error: AgentResolutionError::PlaneStopping,
                }) => {
                    info!("Omitting unlinked message as the plane is stopping.");
                }
                OutgoingEvent::Request(req) => {
                    trace!(envelope = ?req, "Sending request envelope.");
                    buffer.clear();
                    recon_encoder
                        .encode(req, &mut buffer)
                        .expect("Encoding a frame should be infallible.");
                    if let Err(error) = output.write(&buffer, PayloadType::Text).await {
                        error!(error = %error, "Writing to the websocket connection failed.");
                        break;
                    }
                }
                OutgoingEvent::Response(res) => {
                    trace!(envelope = ?res, "Sending response envelope.");
                    buffer.clear();
                    recon_encoder
                        .encode(res, &mut buffer)
                        .expect("Encoding a frame should be infallible.");
                    if let Err(error) = output.write(&buffer, PayloadType::Text).await {
                        error!(error = %error, "Writing to the websocket connection failed.");
                        break;
                    }
                }
            }
        }
    }
}

// The incoming task routes incoming envelopes to agents and downlinks.
struct IncomingTask {
    id: Uuid,
    client_subscriptions: HashMap<Text, HashMap<Text, ResponseWriters>>,
    agent_routes: HashMap<Text, RequestWriter>,
}

impl IncomingTask {
    fn new(id: Uuid) -> Self {
        IncomingTask {
            id,
            client_subscriptions: Default::default(),
            agent_routes: Default::default(),
        }
    }
}

impl IncomingTask {
    async fn run<In>(
        &mut self,
        mut stop_signal: trigger::Receiver,
        input: In,
        mut attach_rx: mpsc::Receiver<RegisterIncoming>,
        find_tx: Option<mpsc::Sender<FindNode>>,
        outgoing_tx: mpsc::Sender<OutgoingTaskMessage>,
    ) -> Result<(), InputError>
    where
        In: Stream<Item = Result<BytesStr, InputError>>,
    {
        let IncomingTask {
            id,
            client_subscriptions,
            agent_routes,
        } = self;
        pin_mut!(input);

        loop {
            let event: IncomingEvent<BytesStr> = tokio::select! {
                biased;
                _ = &mut stop_signal => break Ok(()),
                maybe_request = attach_rx.recv() => {
                    if let Some(request) = maybe_request {
                        IncomingEvent::Register(request)
                    } else {
                        break Ok(());
                    }
                },
                maybe_message = input.next() => {
                    if let Some(message) = maybe_message {
                        IncomingEvent::Message(message)
                    } else {
                        break Ok(());
                    }
                },
            };

            match event {
                IncomingEvent::Register(RegisterIncoming {
                    path: RelativeAddress { node, lane },
                    sender,
                    done,
                }) => {
                    debug!(node = %node, lane = %lane, "Registering client.");
                    client_subscriptions
                        .entry(node)
                        .or_default()
                        .entry(lane)
                        .or_default()
                        .push(FramedWrite::new(sender, Default::default()));
                    done.trigger();
                }
                IncomingEvent::Message(Ok(frame)) => {
                    trace!(frame = frame.as_str(), "Handling incoming frame.");
                    let body = frame.as_ref();
                    match peel_envelope_header_str(body) {
                        Ok(envelope) => match interpret_envelope(*id, envelope) {
                            Some(Either::Left(request)) => match &find_tx {
                                Some(find_tx) => {
                                    let node = request.path.node.as_ref();

                                    let dispatched = if let Some(writer) =
                                        agent_routes.get_mut(node)
                                    {
                                        if let Err(error) = writer.send(&request).await {
                                            debug!(error = %error, "Forwarding envelope to agent route failed.");
                                            agent_routes.remove(node);
                                            false
                                        } else {
                                            true
                                        }
                                    } else {
                                        false
                                    };
                                    if !dispatched {
                                        match connect_agent_route(
                                            *id,
                                            Text::new(node),
                                            Text::new(request.path.lane.as_ref()),
                                            &find_tx,
                                            &outgoing_tx,
                                        )
                                        .await
                                        {
                                            Ok(Some(writer)) => {
                                                let writer = agent_routes
                                                    .entry(Text::new(node))
                                                    .or_insert_with_key(move |_| writer);
                                                if let Err(error) = writer.send(&request).await {
                                                    error!(error = %error, "Envelope not dispatched as agent stopped immediately.");
                                                    agent_routes.remove(node);
                                                }
                                            }
                                            Err(_) => break Ok(()),
                                            _ => {}
                                        }
                                    }
                                }
                                None => {
                                    let RequestMessage { path, .. } = request;
                                    if outgoing_tx
                                        .send(OutgoingTaskMessage::NotFound {
                                            error: AgentResolutionError::NotFound(NoSuchAgent {
                                                node: path.node.into(),
                                                lane: path.lane.into(),
                                            }),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break Ok(());
                                    }
                                }
                            },
                            Some(Either::Right(response)) => {
                                let Path { node, lane } = response.path.clone();
                                if let Some(node_map) = client_subscriptions.get_mut(node.as_ref())
                                {
                                    if let Some(senders) = node_map.get_mut(lane.as_ref()) {
                                        if !send_response(senders, response).await {
                                            node_map.remove(lane.as_ref());
                                            if node_map.is_empty() {
                                                client_subscriptions.remove(node.as_ref());
                                            }
                                        }
                                    } else {
                                        info!(
                                            node = node.as_ref(),
                                            lane = lane.as_ref(),
                                            "Envelope received for downlink that does not exist."
                                        );
                                    };
                                } else {
                                    info!(
                                        node = node.as_ref(),
                                        lane = lane.as_ref(),
                                        "Envelope received for downlink that does not exist."
                                    );
                                }
                            }
                            _ => {
                                warn!("Auth and Deauth no yet implemented.");
                            }
                        },
                        Err(error) => {
                            error!(
                                frame = frame.as_str(),
                                "Received a frame that does not contain a valid Warp envelope."
                            );
                            break Err(InputError::InvalidEnvelope(error));
                        }
                    }
                }
                IncomingEvent::Message(Err(e)) => {
                    break Err(e);
                }
            }
        }
    }
}

// If a message arrives for an unknown agent, attempt to resolve it and open a link.
async fn connect_agent_route(
    source: Uuid,
    node: Text,
    lane: Text,
    find_tx: &mpsc::Sender<FindNode>,
    outgoing_tx: &mpsc::Sender<OutgoingTaskMessage>,
) -> Result<Option<RequestWriter>, ()> {
    debug!(node = %node, "Attempting to open route to agent.");
    let (tx, rx) = oneshot::channel();
    let find = FindNode {
        source,
        node,
        lane,
        provider: tx,
    };
    find_tx.send(find).await.map_err(|_| ())?;
    match rx.await {
        Ok(Ok((writer, reader))) => {
            let (done_tx, done_rx) = trigger::trigger();
            if outgoing_tx
                .send(OutgoingTaskMessage::RegisterOutgoing {
                    kind: OutgoingKind::Server,
                    receiver: reader,
                    done: done_tx,
                })
                .await
                .is_err()
            {
                return Err(());
            }

            if done_rx.await.is_err() {
                Err(())
            } else {
                Ok(Some(RequestWriter::new(writer, Default::default())))
            }
        }
        Ok(Err(error)) => outgoing_tx
            .send(OutgoingTaskMessage::NotFound { error })
            .await
            .map(move |_| None)
            .map_err(|_| ()),
        _ => Err(()),
    }
}

/// Broadcast a message to all subscribed downlinks.
async fn send_response(senders: &mut ResponseWriters, message: RawResponse<'_>) -> bool {
    let message_ref = &message;
    let failed = join_all(
        senders
            .iter_mut()
            .enumerate()
            .map(|(i, sender)| async move {
                if sender.send(message_ref).await.is_err() {
                    Some(i)
                } else {
                    None
                }
            }),
    )
    .await
    .into_iter()
    .flatten()
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

// Determine whether an envelope is for an agent or a downlink.
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

async fn await_io_tasks<F1, F2>(
    in_task: F1,
    out_task: F2,
    kill_switch_tx: trigger::Sender,
) -> Result<(), InputError>
where
    F1: Future<Output = Result<(), InputError>>,
    F2: Future<Output = ()>,
{
    use futures::future::Either as FEither;
    pin_mut!(in_task);
    pin_mut!(out_task);
    let first_finished = select(in_task, out_task).await;
    kill_switch_tx.trigger();
    match first_finished {
        FEither::Left((r, write_fut)) => {
            write_fut.await;
            r
        }
        FEither::Right((_, read_fut)) => read_fut.await,
    }
}
