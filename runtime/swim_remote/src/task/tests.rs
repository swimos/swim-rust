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

use std::{num::NonZeroUsize, time::Duration};

use bytes::BytesMut;
use futures::{
    future::{join, join3, join4},
    Future, SinkExt, StreamExt,
};
use ratchet::{
    CloseCode, CloseReason, Message, NegotiatedExtension, NoExt, NoExtDecoder, Receiver, Role,
    WebSocket, WebSocketConfig,
};
use swim_messages::{
    bytes_str::BytesStr,
    protocol::{
        BytesRequestMessage, BytesResponseMessage, Notification, Operation, Path,
        RawRequestMessageDecoder, RawRequestMessageEncoder, RawResponseMessageDecoder,
        RawResponseMessageEncoder, RequestMessage, ResponseMessage,
    },
};
use swim_model::Text;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{self, byte_channel, ByteReader, ByteWriter},
    trigger,
};
use tokio::{
    io::{duplex, DuplexStream},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::{error::AgentResolutionError, task::OutgoingKind, AttachClient, FindNode, NoSuchAgent};

use super::{InputError, OutgoingTaskMessage, RegisterIncoming};

const ID: Uuid = Uuid::from_u128(1484);
const CHAN_SIZE: usize = 8;
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const NODE: &str = "/node";
const OTHER: &str = "/other";
const LANE: &str = "lane";

const DL_NODE: &str = "/remote";
const DL_LANE: &str = "remote_lane";

const AGENT_PATH: Path<BytesStr> = Path::from_static_strs(NODE, LANE);
const DL_PATH: Path<BytesStr> = Path::from_static_strs(DL_NODE, DL_LANE);

async fn test_registration_task<F, Fut>(test_case: F) -> Fut::Output
where
    F: FnOnce(
        mpsc::Sender<AttachClient>,
        mpsc::Receiver<RegisterIncoming>,
        mpsc::Receiver<OutgoingTaskMessage>,
        trigger::Sender,
    ) -> Fut,
    Fut: Future,
{
    let (att_tx, att_rx) = mpsc::channel(CHAN_SIZE);
    let (in_tx, in_rx) = mpsc::channel(CHAN_SIZE);
    let (out_tx, out_rx) = mpsc::channel(CHAN_SIZE);
    let (stop_tx, stop_rx) = trigger::trigger();

    let reg_task = super::registration_task(att_rx, in_tx, out_tx, stop_rx);
    let test_task = test_case(att_tx, in_rx, out_rx, stop_tx);
    let (_, result) = tokio::time::timeout(TEST_TIMEOUT, join(reg_task, test_task))
        .await
        .expect("Test timed out.");
    result
}

#[tokio::test]
async fn reg_task_stops_when_triggered() {
    let _parts = test_registration_task(|att_tx, in_rx, out_rx, stop_trigger| async move {
        stop_trigger.trigger();
        (att_tx, in_rx, out_rx)
    })
    .await;
}

#[tokio::test]
async fn reg_task_stops_when_channel_ends() {
    let _stop_trigger =
        test_registration_task(|_, _, _, stop_trigger| async move { stop_trigger }).await;
}

#[tokio::test]
async fn register_for_downlinks() {
    test_registration_task(|att_tx, mut in_rx, mut out_rx, stop_trigger| async move {
        let (tx1, rx1) = byte_channel::byte_channel(BUFFER_SIZE);
        let (tx2, rx2) = byte_channel::byte_channel(BUFFER_SIZE);
        let (done_tx, done_rx) = trigger::trigger();
        att_tx
            .send(AttachClient::AttachDownlink {
                node: Text::new(NODE),
                lane: Text::new(LANE),
                sender: tx1,
                receiver: rx2,
                done: done_tx,
            })
            .await
            .expect("Channel closed.");

        let (in_res, out_res) = join(in_rx.recv(), out_rx.recv()).await;
        let RegisterIncoming {
            node,
            lane,
            sender,
            done: done_in,
        } = in_res.expect("Channel closed.");

        assert_eq!(node, NODE);
        assert_eq!(lane, LANE);

        assert!(byte_channel::are_connected(&sender, &rx1));

        done_in.trigger();

        if let Some(OutgoingTaskMessage::RegisterOutgoing {
            kind,
            receiver,
            done: done_out,
        }) = out_res
        {
            assert_eq!(kind, OutgoingKind::Client);
            assert!(byte_channel::are_connected(&tx2, &receiver));

            done_out.trigger();

            assert!(done_rx.await.is_ok());
        } else {
            panic!("Incorrect message received.");
        }

        stop_trigger.trigger();
    })
    .await;
}

struct IncomingTestContext {
    stop_tx: Option<trigger::Sender>,
    in_tx: mpsc::Sender<Result<BytesStr, InputError>>,
    attach_tx: mpsc::Sender<RegisterIncoming>,
    outgoing_rx: mpsc::Receiver<OutgoingTaskMessage>,
    agent_in_rx: Option<ByteReader>,
    agent_replace_tx: mpsc::Sender<(ByteWriter, ByteReader)>,
}

impl IncomingTestContext {
    fn take_agent_reader(&mut self) -> ByteReader {
        self.agent_in_rx.take().expect("Reader already taken.")
    }

    fn stop(&mut self) {
        if let Some(stop_trigger) = self.stop_tx.take() {
            stop_trigger.trigger();
        }
    }
}

async fn test_incoming_task<F, Fut>(test_case: F) -> (Result<(), InputError>, Fut::Output)
where
    F: FnOnce(IncomingTestContext) -> Fut,
    Fut: Future,
{
    let (stop_tx, stop_rx) = trigger::trigger();
    let (in_tx, in_rx) = mpsc::channel(CHAN_SIZE);
    let (attach_tx, attach_rx) = mpsc::channel(CHAN_SIZE);
    let (find_tx, mut find_rx) = mpsc::channel(CHAN_SIZE);
    let (outgoing_tx, outgoing_rx) = mpsc::channel(CHAN_SIZE);
    let (agent_in_tx, agent_in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (_agent_out_tx, agent_out_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (agent_replace_tx, mut agent_replace_rx) = mpsc::channel(CHAN_SIZE);

    let context = IncomingTestContext {
        stop_tx: Some(stop_tx),
        in_tx,
        attach_tx,
        outgoing_rx,
        agent_in_rx: Some(agent_in_rx),
        agent_replace_tx,
    };

    let mut incoming = super::IncomingTask::new(ID);

    let incoming_task = incoming.run(
        stop_rx,
        ReceiverStream::new(in_rx),
        attach_rx,
        find_tx,
        outgoing_tx,
    );

    let mut agent = Some((agent_in_tx, agent_out_rx));
    let resolve_task = async move {
        while let Some(FindNode {
            node,
            lane,
            provider,
            ..
        }) = find_rx.recv().await
        {
            if node == NODE {
                if let Some(agent) = agent.take() {
                    provider.send(Ok(agent)).expect("Task stopped.");
                } else {
                    if let Some(agent) = agent_replace_rx.recv().await {
                        provider.send(Ok(agent)).expect("Task stopped.");
                    } else {
                        panic!("Agent cannot be attached.");
                    }
                }
            } else {
                provider
                    .send(Err(NoSuchAgent { node, lane }.into()))
                    .expect("Task stopped.");
            }
        }
    };

    let test_task = test_case(context);

    let (task_result, _, result) =
        tokio::time::timeout(TEST_TIMEOUT, join3(incoming_task, resolve_task, test_task))
            .await
            .expect("Test timed out.");
    (task_result, result)
}

#[tokio::test]
async fn incoming_clean_shutdown() {
    let (task_result, _context) = test_incoming_task(|mut context| async move {
        context.stop();
        context
    })
    .await;
    assert!(task_result.is_ok());
}

#[tokio::test]
async fn incoming_shutdown_end_of_input() {
    let (task_result, _parts) = test_incoming_task(|context| async move {
        let IncomingTestContext {
            stop_tx,
            in_tx,
            attach_tx,
            outgoing_rx,
            ..
        } = context;
        drop(in_tx);
        (stop_tx, attach_tx, outgoing_rx)
    })
    .await;
    assert!(task_result.is_ok());
}

const DL_BODY: &str = "5";

fn make_dl_envelope() -> BytesStr {
    let env = format!("@event(node:\"{}\",lane:{}) {}", DL_NODE, DL_LANE, DL_BODY);
    BytesStr::from(env)
}

struct DlReader(FramedRead<ByteReader, RawResponseMessageDecoder>);
impl DlReader {
    fn new(reader: ByteReader) -> Self {
        DlReader(FramedRead::new(reader, Default::default()))
    }

    async fn recv(&mut self) -> BytesResponseMessage {
        self.0
            .next()
            .await
            .expect("Channel dropped.")
            .expect("Read failed.")
    }
}

struct AgentReader(FramedRead<ByteReader, RawRequestMessageDecoder>);
impl AgentReader {
    fn new(reader: ByteReader) -> Self {
        AgentReader(FramedRead::new(reader, Default::default()))
    }

    async fn recv(&mut self) -> BytesRequestMessage {
        self.0
            .next()
            .await
            .expect("Channel dropped.")
            .expect("Read failed.")
    }

    async fn recv_opt(&mut self) -> Option<BytesRequestMessage> {
        self.0.next().await.and_then(Result::ok)
    }
}

#[tokio::test]
async fn incoming_route_downlink_env() {
    let (task_result, _) = test_incoming_task(|mut context| async move {
        let IncomingTestContext {
            in_tx, attach_tx, ..
        } = &mut context;

        let (channel_tx, channel_rx) = byte_channel::byte_channel(BUFFER_SIZE);
        let (done_tx, done_rx) = trigger::trigger();

        attach_tx
            .send(RegisterIncoming {
                node: Text::new(DL_NODE),
                lane: Text::new(DL_LANE),
                sender: channel_tx,
                done: done_tx,
            })
            .await
            .expect("Channel ended.");

        assert!(done_rx.await.is_ok());

        in_tx
            .send(Ok(make_dl_envelope()))
            .await
            .expect("Task stopped.");

        let mut dl_rx = DlReader::new(channel_rx);

        let ResponseMessage {
            origin,
            path,
            envelope,
        } = dl_rx.recv().await;

        assert_eq!(origin, ID);
        assert_eq!(path, DL_PATH);
        match envelope {
            Notification::Event(body) => {
                let body_str = std::str::from_utf8(body.as_ref()).expect("Invalid UTF8");
                assert_eq!(body_str, DL_BODY);
            }
            ow => panic!("Unexpected envelope: {:?}", ow),
        }

        context.stop();
        context
    })
    .await;

    assert!(task_result.is_ok());
}

fn make_agent_envelope(a: i32) -> BytesStr {
    let env = format!("@command(node:\"{}\",lane:{}) {{a:{}}}", NODE, LANE, a);
    BytesStr::from(env)
}

fn make_bad_agent_envelope() -> BytesStr {
    let env = format!("@command(node:\"{}\",lane:{}) {{a:2}}", OTHER, LANE);
    BytesStr::from(env)
}

async fn check_env(n: i32, agent_rx: &mut AgentReader) {
    let RequestMessage {
        origin,
        path,
        envelope,
    } = agent_rx.recv().await;

    assert_eq!(origin, ID);
    assert_eq!(path, AGENT_PATH);

    match envelope {
        Operation::Command(body) => {
            let body_str = std::str::from_utf8(body.as_ref()).expect("Invalid UTF8");
            let expected = format!("{{a:{}}}", n);
            assert_eq!(body_str, expected);
        }
        ow => panic!("Unexpected envelope: {:?}", ow),
    }
}

#[tokio::test]
async fn incoming_route_valid_agent_env() {
    let (task_result, _) = test_incoming_task(|mut context| async move {
        let mut agent_rx = AgentReader::new(context.take_agent_reader());

        let IncomingTestContext {
            in_tx, outgoing_rx, ..
        } = &mut context;

        in_tx
            .send(Ok(make_agent_envelope(8)))
            .await
            .expect("Task stopped.");

        match outgoing_rx.recv().await {
            Some(OutgoingTaskMessage::RegisterOutgoing {
                kind: OutgoingKind::Server,
                done,
                ..
            }) => {
                done.trigger();
            }
            ow => panic!("Unexpected registration: {:?}", ow),
        }

        check_env(8, &mut agent_rx).await;

        context.stop();
        context
    })
    .await;
    assert!(task_result.is_ok());
}

#[tokio::test]
async fn incoming_route_valid_agent_restart() {
    let (task_result, _) = test_incoming_task(|mut context| async move {
        let mut agent_rx = AgentReader::new(context.take_agent_reader());
        let IncomingTestContext {
            in_tx,
            outgoing_rx,
            agent_replace_tx,
            ..
        } = &mut context;

        in_tx
            .send(Ok(make_agent_envelope(8)))
            .await
            .expect("Task stopped.");

        match outgoing_rx.recv().await {
            Some(OutgoingTaskMessage::RegisterOutgoing {
                kind: OutgoingKind::Server,
                done,
                ..
            }) => {
                done.trigger();
            }
            ow => panic!("Unexpected registration: {:?}", ow),
        }

        check_env(8, &mut agent_rx).await;

        //Drop the agent and replace it.

        drop(agent_rx);
        let (agent_in_tx, agent_in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
        let (_agent_out_tx, agent_out_rx) = byte_channel::byte_channel(BUFFER_SIZE);

        agent_replace_tx
            .send((agent_in_tx, agent_out_rx))
            .await
            .expect("Replacing agent failed.");

        let mut agent_rx = AgentReader::new(agent_in_rx);

        //Send another envelope and check it is routed to the new agent.

        in_tx
            .send(Ok(make_agent_envelope(346)))
            .await
            .expect("Task stopped.");

        match outgoing_rx.recv().await {
            Some(OutgoingTaskMessage::RegisterOutgoing {
                kind: OutgoingKind::Server,
                done,
                ..
            }) => {
                done.trigger();
            }
            ow => panic!("Unexpected registration: {:?}", ow),
        }

        check_env(346, &mut agent_rx).await;
        context.stop();
        context
    })
    .await;
    assert!(task_result.is_ok());
}

#[tokio::test]
async fn incoming_route_multiple_agent_env() {
    let (task_result, _) = test_incoming_task(|mut context| async move {
        let mut agent_rx = AgentReader::new(context.take_agent_reader());

        let IncomingTestContext {
            in_tx, outgoing_rx, ..
        } = &mut context;

        in_tx
            .send(Ok(make_agent_envelope(8)))
            .await
            .expect("Task stopped.");

        match outgoing_rx.recv().await {
            Some(OutgoingTaskMessage::RegisterOutgoing {
                kind: OutgoingKind::Server,
                done,
                ..
            }) => {
                done.trigger();
            }
            ow => panic!("Unexpected registration: {:?}", ow),
        }

        check_env(8, &mut agent_rx).await;

        in_tx
            .send(Ok(make_agent_envelope(24)))
            .await
            .expect("Task stopped.");

        check_env(24, &mut agent_rx).await;

        context.stop();
        context
    })
    .await;
    assert!(task_result.is_ok());
}

#[tokio::test]
async fn incoming_route_not_found_env() {
    let (task_result, _) = test_incoming_task(|mut context| async move {
        let IncomingTestContext {
            in_tx, outgoing_rx, ..
        } = &mut context;

        in_tx
            .send(Ok(make_bad_agent_envelope()))
            .await
            .expect("Task stopped.");

        match outgoing_rx.recv().await {
            Some(OutgoingTaskMessage::NotFound {
                error: AgentResolutionError::NotFound(NoSuchAgent { node, lane }),
            }) => {
                assert_eq!(node, OTHER);
                assert_eq!(lane, LANE);
            }
            ow => panic!("Unexpected registration: {:?}", ow),
        }

        context.stop();
        context
    })
    .await;
    assert!(task_result.is_ok());
}

#[tokio::test]
async fn incoming_terminates_on_input_error() {
    let (task_result, _context) = test_incoming_task(|mut context| async move {
        let IncomingTestContext { in_tx, .. } = &mut context;

        in_tx
            .send(Err(InputError::BinaryFrame))
            .await
            .expect("Task stopped.");

        context
    })
    .await;
    assert!(matches!(task_result, Err(InputError::BinaryFrame)));
}

fn make_fake_ws() -> (
    WebSocket<DuplexStream, NoExt>,
    WebSocket<DuplexStream, NoExt>,
) {
    let (server, client) = duplex(BUFFER_SIZE.get());
    let config = WebSocketConfig::default();

    let server = WebSocket::from_upgraded(
        config,
        server,
        NegotiatedExtension::from(NoExt),
        BytesMut::new(),
        Role::Server,
    );
    let client = WebSocket::from_upgraded(
        config,
        client,
        NegotiatedExtension::from(NoExt),
        BytesMut::new(),
        Role::Client,
    );
    (server, client)
}

#[tokio::test]
async fn messages_from_ws() {
    let (server, mut client) = make_fake_ws();

    let (_server_tx, mut server_rx) = server.split().expect("Split failed.");
    let stream = super::text_frame_stream(&mut server_rx);

    client.write_text("first").await.expect("Send failed.");
    client.write_text("second").await.expect("Send failed.");
    client.write_text("third").await.expect("Send failed.");

    let frames: Vec<_> = tokio::time::timeout(
        TEST_TIMEOUT,
        stream
            .take(3)
            .map(|r| r.expect("Stream failed."))
            .map(|body| body.to_string())
            .collect(),
    )
    .await
    .expect("Timed out.");

    assert_eq!(
        frames,
        vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string()
        ]
    );
}

#[tokio::test]
async fn ws_close() {
    let (server, mut client) = make_fake_ws();

    let (_server_tx, mut server_rx) = server.split().expect("Split failed.");
    let stream = super::text_frame_stream(&mut server_rx);

    let close_reason = CloseReason::new(CloseCode::GoingAway, Some("gone".to_string()));
    client
        .close(close_reason.clone())
        .await
        .expect("Close failed.");

    let frames: Vec<_> = tokio::time::timeout(TEST_TIMEOUT, stream.collect())
        .await
        .expect("Timed out.");

    match frames.as_slice() {
        [Err(InputError::Closed(Some(reason)))] => {
            assert_eq!(reason, &close_reason);
        }
        ow => panic!("Unexpected frames: {:?}", ow),
    }
}

#[tokio::test]
async fn error_on_binary_frame() {
    let (server, mut client) = make_fake_ws();

    let (_server_tx, mut server_rx) = server.split().expect("Split failed.");
    let stream = super::text_frame_stream(&mut server_rx);

    client
        .write_binary(&[0, 1, 2, 3])
        .await
        .expect("Close failed.");

    let frames: Vec<_> = tokio::time::timeout(TEST_TIMEOUT, stream.collect())
        .await
        .expect("Timed out.");

    assert!(matches!(frames.as_slice(), [Err(InputError::BinaryFrame)]));
}

#[tokio::test]
#[ignore = "Pending fix in ratchet."]
async fn ignore_ping_pong() {
    let (server, mut client) = make_fake_ws();

    let (_server_tx, mut server_rx) = server.split().expect("Split failed.");
    let stream = super::text_frame_stream(&mut server_rx);

    client.write_text("first").await.expect("Send failed.");
    client.write_ping("ping!").await.expect("Send failed.");
    client.write_text("second").await.expect("Send failed.");
    client.write_text("third").await.expect("Send failed.");

    let frames: Vec<_> = tokio::time::timeout(
        TEST_TIMEOUT,
        stream
            .take(3)
            .map(|r| r.expect("Stream failed."))
            .map(|body| body.to_string())
            .collect(),
    )
    .await
    .expect("Timed out.");

    let mut buf = BytesMut::new();

    let message = client.read(&mut buf).await.expect("Read failed.");
    assert!(matches!(message, Message::Pong(_)));

    assert_eq!(
        frames,
        vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string()
        ]
    );
}

struct OutgoingTestContext {
    stop_tx: Option<trigger::Sender>,
    outgoing_tx: mpsc::Sender<OutgoingTaskMessage>,
    client: WebSocket<DuplexStream, NoExt>,
    _server_rx: Receiver<DuplexStream, NoExtDecoder>,
}

impl OutgoingTestContext {
    fn stop(&mut self) {
        if let Some(stop_trigger) = self.stop_tx.take() {
            stop_trigger.trigger();
        }
    }
}

async fn test_outgoing_task<F, Fut>(test_case: F) -> Fut::Output
where
    F: FnOnce(OutgoingTestContext) -> Fut,
    Fut: Future,
{
    let (stop_tx, stop_rx) = trigger::trigger();

    let (outgoing_tx, outgoing_rx) = mpsc::channel(CHAN_SIZE);

    let mut outgoing = super::OutgoingTask::default();
    let (server, client) = duplex(BUFFER_SIZE.get());
    let config = WebSocketConfig::default();

    let server = WebSocket::from_upgraded(
        config,
        server,
        NegotiatedExtension::from(NoExt),
        BytesMut::new(),
        Role::Server,
    );
    let client = WebSocket::from_upgraded(
        config,
        client,
        NegotiatedExtension::from(NoExt),
        BytesMut::new(),
        Role::Client,
    );

    let (mut server_tx, server_rx) = server.split().expect("Split failed.");

    let context = OutgoingTestContext {
        stop_tx: Some(stop_tx),
        outgoing_tx,
        client,
        _server_rx: server_rx,
    };

    let outgoing_task = outgoing.run(stop_rx, &mut server_tx, outgoing_rx);

    let test_task = test_case(context);

    let (_, result) = tokio::time::timeout(TEST_TIMEOUT, join(outgoing_task, test_task))
        .await
        .expect("Test timed out.");
    result
}

#[tokio::test]
async fn outgoing_clean_shutdown() {
    let _context = test_outgoing_task(|mut context| async move {
        context.stop();
        context
    })
    .await;
}

struct DlSender(FramedWrite<ByteWriter, RawRequestMessageEncoder>);

struct AgentSender(FramedWrite<ByteWriter, RawResponseMessageEncoder>);

const DL_ID: Uuid = Uuid::from_u128(99918833);
const AGENT_ID: Uuid = Uuid::from_u128(23648498);

impl DlSender {
    fn new(writer: ByteWriter) -> Self {
        DlSender(FramedWrite::new(writer, Default::default()))
    }

    async fn send(&mut self, node: &str, lane: &str, body: &str) {
        self.0
            .send(RequestMessage::command(DL_ID, Path::new(node, lane), body))
            .await
            .expect("Send failed.");
    }
}

impl AgentSender {
    fn new(writer: ByteWriter) -> Self {
        AgentSender(FramedWrite::new(writer, Default::default()))
    }

    async fn send(&mut self, node: &str, lane: &str, body: &str) {
        self.0
            .send(ResponseMessage::<_, _, &[u8]>::event(
                AGENT_ID,
                Path::new(node, lane),
                body,
            ))
            .await
            .expect("Send failed.");
    }

    async fn send_response(&mut self, msg: BytesResponseMessage) {
        self.0.send(msg).await.expect("Send failed.");
    }
}

#[tokio::test]
async fn outgoing_downlink_message() {
    let _context = test_outgoing_task(|mut context| async move {
        let OutgoingTestContext {
            outgoing_tx,
            client,
            ..
        } = &mut context;

        let (dl_tx, dl_rx) = byte_channel(BUFFER_SIZE);
        let (done_tx, done_rx) = trigger::trigger();

        outgoing_tx
            .send(OutgoingTaskMessage::RegisterOutgoing {
                kind: OutgoingKind::Client,
                receiver: dl_rx,
                done: done_tx,
            })
            .await
            .expect("Channel dropped");

        assert!(done_rx.await.is_ok());
        let mut dl_sender = DlSender::new(dl_tx);

        let body = "content";

        dl_sender.send(DL_NODE, DL_LANE, body).await;

        let mut buf = BytesMut::new();
        let message = client.read(&mut buf).await.expect("Output stopped.");

        assert_eq!(message, Message::Text);

        let env_str = std::str::from_utf8(buf.as_ref()).expect("Invalid UTF8.");

        let expected = format!("@command(node:\"{}\",lane:{}) {}", DL_NODE, DL_LANE, body);

        assert_eq!(env_str, expected);

        context.stop();
        context
    })
    .await;
}

#[tokio::test]
async fn outgoing_agent_message() {
    let _context = test_outgoing_task(|mut context| async move {
        let OutgoingTestContext {
            outgoing_tx,
            client,
            ..
        } = &mut context;

        let (agent_tx, agent_rx) = byte_channel(BUFFER_SIZE);
        let (done_tx, done_rx) = trigger::trigger();

        outgoing_tx
            .send(OutgoingTaskMessage::RegisterOutgoing {
                kind: OutgoingKind::Server,
                receiver: agent_rx,
                done: done_tx,
            })
            .await
            .expect("Channel dropped");

        assert!(done_rx.await.is_ok());
        let mut agent_sender = AgentSender::new(agent_tx);

        let body = "content";

        agent_sender.send(NODE, LANE, body).await;

        let mut buf = BytesMut::new();
        let message = client.read(&mut buf).await.expect("Output stopped.");

        assert_eq!(message, Message::Text);

        let env_str = std::str::from_utf8(buf.as_ref()).expect("Invalid UTF8.");

        let expected = format!("@event(node:\"{}\",lane:{}) {}", NODE, LANE, body);

        assert_eq!(env_str, expected);

        context.stop();
        context
    })
    .await;
}

#[tokio::test]
async fn outgoing_lane_not_found() {
    let _context = test_outgoing_task(|mut context| async move {
        let OutgoingTestContext {
            outgoing_tx,
            client,
            ..
        } = &mut context;

        outgoing_tx
            .send(OutgoingTaskMessage::NotFound {
                error: AgentResolutionError::NotFound(NoSuchAgent {
                    node: Text::new(OTHER),
                    lane: Text::new(LANE),
                }),
            })
            .await
            .expect("Channel dropped");

        let mut buf = BytesMut::new();
        let message = client.read(&mut buf).await.expect("Output stopped.");

        assert_eq!(message, Message::Text);

        let env_str = std::str::from_utf8(buf.as_ref()).expect("Invalid UTF8.");

        let expected = format!("@unlinked(node:\"{}\",lane:{})@nodeNotFound", OTHER, LANE);

        assert_eq!(env_str, expected);

        context.stop();
        context
    })
    .await;
}

struct CombinedTestContext {
    stop_tx: Option<trigger::Sender>,
    attach_tx: mpsc::Sender<AttachClient>,
    client: WebSocket<DuplexStream, NoExt>,
}

impl CombinedTestContext {
    fn stop(&mut self) {
        if let Some(stop_trigger) = self.stop_tx.take() {
            stop_trigger.trigger();
        }
    }
}

async fn test_combined_task<F, Fut>(test_case: F) -> Fut::Output
where
    F: FnOnce(CombinedTestContext) -> Fut,
    Fut: Future,
{
    let (stop_tx, stop_rx) = trigger::trigger();

    let (attach_tx, attach_rx) = mpsc::channel(CHAN_SIZE);
    let (find_tx, mut find_rx) = mpsc::channel(CHAN_SIZE);
    let (agent_in_tx, agent_in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (agent_out_tx, agent_out_rx) = byte_channel::byte_channel(BUFFER_SIZE);

    let (server, client) = duplex(BUFFER_SIZE.get());
    let config = WebSocketConfig::default();

    let server = WebSocket::from_upgraded(
        config,
        server,
        NegotiatedExtension::from(NoExt),
        BytesMut::new(),
        Role::Server,
    );
    let client = WebSocket::from_upgraded(
        config,
        client,
        NegotiatedExtension::from(NoExt),
        BytesMut::new(),
        Role::Client,
    );

    let context = CombinedTestContext {
        stop_tx: Some(stop_tx),
        attach_tx,
        client,
    };

    let remote = super::RemoteTask::new(
        ID,
        stop_rx,
        server,
        attach_rx,
        find_tx,
        non_zero_usize!(CHAN_SIZE),
    );

    let remote_task = remote.run();

    let mut agent = Some((agent_in_tx, agent_out_rx));
    let resolve_task = async move {
        while let Some(FindNode {
            node,
            lane,
            provider,
            ..
        }) = find_rx.recv().await
        {
            if node == NODE {
                if let Some(agent) = agent.take() {
                    provider.send(Ok(agent)).expect("Task stopped.");
                } else {
                    panic!("Agent connection requested twice.");
                }
            } else {
                provider
                    .send(Err(NoSuchAgent { node, lane }.into()))
                    .expect("Task stopped.");
            }
        }
    };

    let agent_task = async move {
        let mut rx = AgentReader::new(agent_in_rx);
        let mut tx = AgentSender::new(agent_out_tx);

        while let Some(RequestMessage { path, envelope, .. }) = rx.recv_opt().await {
            let echo = match envelope {
                Operation::Link => Notification::Linked,
                Operation::Sync => Notification::Synced,
                Operation::Unlink => Notification::Unlinked(None),
                Operation::Command(body) => Notification::Event(body),
            };
            let response = ResponseMessage {
                origin: AGENT_ID,
                path,
                envelope: echo,
            };
            tx.send_response(response).await;
        }
    };

    let test_task = test_case(context);

    let (_, _, _, result) = tokio::time::timeout(
        TEST_TIMEOUT,
        join4(remote_task, agent_task, resolve_task, test_task),
    )
    .await
    .expect("Test timed out.");
    result
}

#[tokio::test]
#[ignore = "Pending fix in ratchet."]
async fn combined_clean_shutdown() {
    test_combined_task(|mut context| async move {
        context.stop();

        let CombinedTestContext { client, .. } = &mut context;

        let mut buf = BytesMut::new();
        let result = client.read(&mut buf).await;
        let message = result.unwrap();
        let reason = CloseReason::new(CloseCode::GoingAway, Some(super::STOPPING.to_string()));
        assert_eq!(message, Message::Close(Some(reason)));

        context
    })
    .await;
}

#[tokio::test]
async fn combined_no_agent() {
    test_combined_task(|mut context| async move {
        let CombinedTestContext { client, .. } = &mut context;

        let envelope = make_bad_agent_envelope();

        client.write_text(envelope).await.expect("Write failed.");

        let mut buf = BytesMut::new();
        let message = client.read(&mut buf).await.expect("Channel closed.");
        assert_eq!(message, Message::Text);

        let frame_str = std::str::from_utf8(buf.as_ref()).expect("Invalid UTF8");
        let expected = format!("@unlinked(node:\"{}\",lane:{})@nodeNotFound", OTHER, LANE);
        assert_eq!(frame_str, expected);
        context.stop();
        context
    })
    .await;
}

#[tokio::test]
async fn combined_agent_io() {
    test_combined_task(|mut context| async move {
        let CombinedTestContext { client, .. } = &mut context;

        let n = -786;

        let envelope = make_agent_envelope(n);

        client.write_text(envelope).await.expect("Write failed.");

        let mut buf = BytesMut::new();
        let message = client.read(&mut buf).await.expect("Channel closed.");
        assert_eq!(message, Message::Text);

        let frame_str = std::str::from_utf8(buf.as_ref()).expect("Invalid UTF8");
        let expected = format!("@event(node:\"{}\",lane:{}) {{a:{}}}", NODE, LANE, n);
        assert_eq!(frame_str, expected);
        context.stop();
        context
    })
    .await;
}

#[tokio::test]
async fn combined_downlink_io() {
    test_combined_task(|mut context| async move {
        let CombinedTestContext {
            client, attach_tx, ..
        } = &mut context;
        let (tx1, rx1) = byte_channel::byte_channel(BUFFER_SIZE);
        let (tx2, rx2) = byte_channel::byte_channel(BUFFER_SIZE);
        let (done_tx, done_rx) = trigger::trigger();
        attach_tx
            .send(AttachClient::AttachDownlink {
                node: Text::new(DL_NODE),
                lane: Text::new(DL_LANE),
                sender: tx1,
                receiver: rx2,
                done: done_tx,
            })
            .await
            .expect("Channel closed.");

        assert!(done_rx.await.is_ok());

        let envelope_in = make_dl_envelope();

        let mut dl_rx = DlReader::new(rx1);
        let mut dl_tx = DlSender::new(tx2);

        client.write_text(envelope_in).await.expect("Write failed.");

        let ResponseMessage {
            origin,
            path,
            envelope,
        } = dl_rx.recv().await;

        assert_eq!(origin, ID);
        assert_eq!(path, DL_PATH);

        match envelope {
            Notification::Event(body) => {
                assert_eq!(body.as_ref(), DL_BODY.as_bytes());
            }
            ow => panic!("Unexpected envelope: {:?}", ow),
        }

        dl_tx.send(DL_NODE, DL_LANE, "22").await;

        let mut buf = BytesMut::new();
        let message = client.read(&mut buf).await.expect("Channel closed.");
        assert_eq!(message, Message::Text);

        let frame_str = std::str::from_utf8(buf.as_ref()).expect("Invalid UTF8");
        let expected = format!("@command(node:\"{}\",lane:{}) 22", DL_NODE, DL_LANE);
        assert_eq!(frame_str, expected);
        context.stop();
        context
    })
    .await;
}
