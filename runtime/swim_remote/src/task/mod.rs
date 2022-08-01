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
    stream::unfold,
    Future, SinkExt, Stream, StreamExt,
};
use ratchet::{
    CloseCode, CloseReason, ExtensionDecoder, ExtensionEncoder, Message, PayloadType,
    SplittableExtension, WebSocket, WebSocketStream,
};
use smallvec::SmallVec;
use swim_messages::{
    bytes_str::BytesStr,
    protocol::{
        BytesRequestMessage, BytesResponseMessage, Path, RawRequestMessageDecoder,
        RawRequestMessageEncoder, RawResponseMessageDecoder, RawResponseMessageEncoder,
        RequestMessage, ResponseMessage,
    },
};
use swim_model::Text;
use swim_recon::parser::MessageExtractError;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    multi_reader::MultiReader,
    trigger,
};
use swim_warp::envelope::{peel_envelope_header_str, RawEnvelope};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};
use uuid::Uuid;

use tracing::{debug, error, info, info_span, trace, warn};
use tracing_futures::Instrument;

use crate::error::LaneNotFound;

use self::envelopes::ReconEncoder;

mod envelopes;

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
    pub source: Uuid,
    pub node: Text,
    pub lane: Text,
    pub provider: oneshot::Sender<Result<(ByteWriter, ByteReader), LaneNotFound>>,
}

pub struct RemoteTask<S, E> {
    id: Uuid,
    stop_signal: trigger::Receiver,
    ws: WebSocket<S, E>,
    attach_rx: mpsc::Receiver<AttachClient>,
    find_tx: mpsc::Sender<FindNode>,
    registration_buffer_size: NonZeroUsize,
}

impl<S, E> RemoteTask<S, E> {
    pub fn new(
        id: Uuid,
        stop_signal: trigger::Receiver,
        ws: WebSocket<S, E>,
        attach_rx: mpsc::Receiver<AttachClient>,
        find_tx: mpsc::Sender<FindNode>,
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

enum IncomingEvent<M> {
    Register(RegisterIncoming),
    Message(Result<M, InputError>),
}

enum OutgoingEvent {
    Message(OutgoingTaskMessage),
    Request(BytesRequestMessage),
    Response(BytesResponseMessage),
}

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
    #[allow(clippy::manual_async_fn)] //To allow the static guarnatee that this future is Send.
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

        let mut incoming = IncomingTask::default();

        let in_task = incoming
            .run(
                id,
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
            if let Err(error) = tx.close_with(reason).await {
                error!(error = %error, "Failed to close websocket.");
            }
        }
    }
}

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

struct RegisterIncoming {
    node: Text,
    lane: Text,
    sender: ByteWriter,
}

#[derive(Debug)]
enum OutgoingKind {
    Client,
    Server,
}

enum OutgoingTaskMessage {
    RegisterOutgoing {
        kind: OutgoingKind,
        receiver: ByteReader,
    },
    NotFound {
        error: LaneNotFound,
    },
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
                debug!(node = %node, lane = %lane, "Attaching new client downlink.");
                if let (Ok(in_res), Ok(out_res)) =
                    join(incoming_tx.reserve(), outgoing_tx.reserve()).await
                {
                    in_res.send(RegisterIncoming { node, lane, sender });
                    out_res.send(OutgoingTaskMessage::RegisterOutgoing {
                        kind: OutgoingKind::Client,
                        receiver,
                    });
                } else {
                    break;
                }
            }
            AttachClient::OneWay { receiver } => {
                debug!("Attaching send only client.");
                if outgoing_tx
                    .send(OutgoingTaskMessage::RegisterOutgoing {
                        kind: OutgoingKind::Client,
                        receiver,
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
                Some(message) = messages_rx.recv() => OutgoingEvent::Message(message),
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
                }
                OutgoingEvent::Message(OutgoingTaskMessage::NotFound { error }) => {
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

#[derive(Default)]
struct IncomingTask {
    client_subscriptions: HashMap<Text, HashMap<Text, ResponseWriters>>,
    agent_routes: HashMap<Text, RequestWriter>,
}

impl IncomingTask {
    async fn run<In>(
        &mut self,
        id: Uuid,
        mut stop_signal: trigger::Receiver,
        input: In,
        mut attach_rx: mpsc::Receiver<RegisterIncoming>,
        find_tx: mpsc::Sender<FindNode>,
        outgoing_tx: mpsc::Sender<OutgoingTaskMessage>,
    ) -> Result<(), InputError>
    where
        In: Stream<Item = Result<BytesStr, InputError>>,
    {
        let IncomingTask {
            client_subscriptions,
            agent_routes,
        } = self;
        pin_mut!(input);

        let mut key_temp: String = Default::default();

        loop {
            let event: IncomingEvent<BytesStr> = tokio::select! {
                biased;
                _ = &mut stop_signal => break Ok(()),
                Some(request) = attach_rx.recv() => IncomingEvent::Register(request),
                Some(result) = input.next() => IncomingEvent::Message(result),
                else => break Ok(()),
            };

            match event {
                IncomingEvent::Register(RegisterIncoming { node, lane, sender }) => {
                    debug!(node = %node, lane = %lane, "Registering client.");
                    client_subscriptions
                        .entry(node)
                        .or_default()
                        .entry(lane)
                        .or_default()
                        .push(FramedWrite::new(sender, Default::default()));
                }
                IncomingEvent::Message(Ok(frame)) => {
                    trace!(frame = frame.as_str(), "Handling incoming frame.");
                    let body = frame.as_ref();
                    match peel_envelope_header_str(body) {
                        Ok(envelope) => {
                            match interpret_envelope(id, envelope) {
                                Some(Either::Left(request)) => {
                                    key_temp.clear();
                                    key_temp.push_str(request.path.node.as_ref());
                                    if let Some(writer) = if let Some(writer) =
                                        agent_routes.get_mut(key_temp.as_str())
                                    {
                                        Some(writer)
                                    } else {
                                        match connect_agent_route(
                                            id,
                                            Text::new(key_temp.as_str()),
                                            Text::new(request.path.lane.as_ref()),
                                            &find_tx,
                                            &outgoing_tx,
                                        )
                                        .await
                                        {
                                            Ok(Some(writer)) => {
                                                let writer = agent_routes
                                                    .entry(Text::new(key_temp.as_str()))
                                                    .or_insert_with_key(move |_| writer);
                                                Some(writer)
                                            }
                                            Err(_) => break Ok(()),
                                            _ => None,
                                        }
                                    } {
                                        if let Err(error) = writer.send(request).await {
                                            //TODO In the case that this is an existing route, we probably want
                                            //to retry here.
                                            agent_routes.remove(key_temp.as_str());
                                            info!(error = %error, "Forwarding envelope to agent route failed.");
                                        }
                                    }
                                }
                                Some(Either::Right(response)) => {
                                    let Path { node, lane } = response.path.clone();
                                    if let Some(node_map) =
                                        client_subscriptions.get_mut(node.as_ref())
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
                            }
                        }
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
        Ok(Ok((writer, reader))) => outgoing_tx
            .send(OutgoingTaskMessage::RegisterOutgoing {
                kind: OutgoingKind::Server,
                receiver: reader,
            })
            .await
            .map(move |_| Some(RequestWriter::new(writer, Default::default())))
            .map_err(|_| ()),
        Ok(Err(error)) => outgoing_tx
            .send(OutgoingTaskMessage::NotFound { error })
            .await
            .map(move |_| None)
            .map_err(|_| ()),
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
