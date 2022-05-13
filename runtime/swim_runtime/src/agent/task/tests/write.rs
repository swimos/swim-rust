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
    collections::{HashMap, HashSet},
    time::Duration,
};

use bytes::Bytes;
use futures::{future::join3, Future, SinkExt, StreamExt};
use swim_api::protocol::map::MapOperation;
use swim_api::{
    agent::UplinkKind,
    protocol::agent::{
        LaneResponseKind, MapLaneResponse, MapLaneResponseEncoder, ValueLaneResponse,
        ValueLaneResponseEncoder,
    },
};
use swim_model::{path::RelativePath, Text};
use swim_utilities::{
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    trigger,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::{
    agent::task::{
        timeout_coord, write_task, LaneEndpoint, RwCoorindationMessage, WriteTaskConfiguration,
        WriteTaskMessage,
    },
    compat::{Notification, RawResponseMessageDecoder, ResponseMessage},
    routing::RoutingAddr,
};

use super::{make_config, BUFFER_SIZE, MAP_LANE, QUEUE_SIZE, TEST_TIMEOUT, VAL_LANE};

enum Instruction {
    ValueEvent {
        lane: Text,
        value: i32,
    },
    MapEvent {
        lane: Text,
        key: Text,
        value: i32,
        id: Option<Uuid>,
    },
    ValueSynced {
        lane: Text,
        value: i32,
        id: Uuid,
    },
    MapSynced {
        lane: Text,
        id: Uuid,
    },
}

struct Instructions(mpsc::UnboundedSender<Instruction>);

impl Instructions {
    fn new(inner: mpsc::UnboundedSender<Instruction>) -> Self {
        Instructions(inner)
    }

    fn value_event(&self, lane: &str, value: i32) {
        let Instructions(inner) = self;
        assert!(inner
            .send(Instruction::ValueEvent {
                lane: Text::new(lane),
                value
            })
            .is_ok());
    }
}

struct ValueLaneSender {
    inner: FramedWrite<ByteWriter, ValueLaneResponseEncoder>,
}

impl ValueLaneSender {
    fn new(writer: ByteWriter) -> Self {
        ValueLaneSender {
            inner: FramedWrite::new(writer, Default::default()),
        }
    }

    async fn event(&mut self, n: i32) {
        let ValueLaneSender { inner } = self;
        assert!(inner.send(ValueLaneResponse::event(n)).await.is_ok());
    }

    async fn synced(&mut self, id: Uuid, n: i32) {
        let ValueLaneSender { inner } = self;
        assert!(inner.send(ValueLaneResponse::synced(id, n)).await.is_ok());
    }
}

struct MapLaneSender {
    inner: FramedWrite<ByteWriter, MapLaneResponseEncoder>,
}

impl MapLaneSender {
    fn new(writer: ByteWriter) -> Self {
        MapLaneSender {
            inner: FramedWrite::new(writer, Default::default()),
        }
    }

    async fn event(&mut self, key: Text, value: i32) {
        let MapLaneSender { inner } = self;
        assert!(inner
            .send(MapLaneResponse::Event {
                kind: LaneResponseKind::StandardEvent,
                operation: MapOperation::Update { key, value }
            })
            .await
            .is_ok());
    }

    async fn sync_event(&mut self, id: Uuid, key: Text, value: i32) {
        let MapLaneSender { inner } = self;
        assert!(inner
            .send(MapLaneResponse::Event {
                kind: LaneResponseKind::SyncEvent(id),
                operation: MapOperation::Update { key, value }
            })
            .await
            .is_ok());
    }

    async fn synced(&mut self, id: Uuid) {
        let MapLaneSender { inner } = self;
        assert!(inner
            .send(MapLaneResponse::<Text, i32>::SyncComplete(id))
            .await
            .is_ok());
    }
}

struct FakeAgent {
    initial: Vec<LaneEndpoint<ByteWriter>>,
    stopping: trigger::Receiver,
    instr_rx: mpsc::UnboundedReceiver<Instruction>,
}

impl FakeAgent {
    fn new(
        initial: Vec<LaneEndpoint<ByteWriter>>,
        stopping: trigger::Receiver,
        instr_rx: mpsc::UnboundedReceiver<Instruction>,
    ) -> Self {
        FakeAgent {
            initial,
            stopping,
            instr_rx,
        }
    }

    async fn run(self) {
        let FakeAgent {
            initial,
            stopping,
            instr_rx,
        } = self;

        let mut value_lanes = HashMap::new();
        let mut map_lanes = HashMap::new();
        for endpoint in initial {
            let LaneEndpoint { name, kind, io } = endpoint;
            match kind {
                UplinkKind::Value => {
                    value_lanes.insert(name, ValueLaneSender::new(io));
                }
                UplinkKind::Map => {
                    map_lanes.insert(name, MapLaneSender::new(io));
                }
            }
        }

        let mut instruction_stream = UnboundedReceiverStream::new(instr_rx).take_until(stopping);

        while let Some(instruction) = instruction_stream.next().await {
            match instruction {
                Instruction::ValueEvent { lane, value } => {
                    if let Some(tx) = value_lanes.get_mut(&lane) {
                        tx.event(value).await;
                    }
                }
                Instruction::MapEvent {
                    lane,
                    key,
                    value,
                    id: Some(id),
                } => {
                    if let Some(tx) = map_lanes.get_mut(&lane) {
                        tx.sync_event(id, key, value).await;
                    }
                }
                Instruction::MapEvent {
                    lane, key, value, ..
                } => {
                    if let Some(tx) = map_lanes.get_mut(&lane) {
                        tx.event(key, value).await;
                    }
                }
                Instruction::ValueSynced { lane, id, value } => {
                    if let Some(tx) = value_lanes.get_mut(&lane) {
                        tx.synced(id, value).await;
                    }
                }
                Instruction::MapSynced { lane, id } => {
                    if let Some(tx) = map_lanes.get_mut(&lane) {
                        tx.synced(id).await;
                    }
                }
            }
        }
    }
}

struct TestContext {
    stop_sender: trigger::Sender,
    messages_tx: mpsc::Sender<WriteTaskMessage>,
    vote2: timeout_coord::Voter,
    vote_rx: timeout_coord::Receiver,
    instr_tx: Instructions,
}

const AGENT_ID: RoutingAddr = RoutingAddr::plane(1);
const NODE: &str = "/node";

async fn run_test_case<F, Fut>(inactive_timeout: Duration, test_case: F) -> Fut::Output
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future + Send,
{
    let (stop_tx, stop_rx) = trigger::trigger();
    let config = make_config(inactive_timeout);

    let endpoints = vec![
        LaneEndpoint {
            name: Text::new(VAL_LANE),
            kind: UplinkKind::Value,
            io: byte_channel(config.default_lane_config.input_buffer_size),
        },
        LaneEndpoint {
            name: Text::new(MAP_LANE),
            kind: UplinkKind::Map,
            io: byte_channel(config.default_lane_config.input_buffer_size),
        },
    ];

    let (endpoints_tx, endpoints_rx) = endpoints.into_iter().map(LaneEndpoint::split).unzip();
    let (instr_tx, instr_rx) = mpsc::unbounded_channel();
    let (vote1, vote2, vote_rx) = timeout_coord::timeout_coordinator();
    let (messages_tx, messages_rx) = mpsc::channel(QUEUE_SIZE.get());

    let fake_agent = FakeAgent::new(endpoints_tx, stop_rx.clone(), instr_rx);
    let write_config = WriteTaskConfiguration::new(AGENT_ID, Text::new(NODE), config);
    let write = write_task(write_config, endpoints_rx, messages_rx, vote1, stop_rx);

    let context = TestContext {
        stop_sender: stop_tx,
        messages_tx,
        vote2,
        vote_rx,
        instr_tx: Instructions::new(instr_tx),
    };

    let test_task = test_case(context);

    let (_, _, result) = join3(fake_agent.run(), write, test_task).await;
    result
}

#[tokio::test]
async fn clean_shutdown_no_remotes() {
    run_test_case(TEST_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx: _messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx: _instr_tx,
        } = context;

        stop_sender.trigger();
    })
    .await;
}

struct RemoteReceiver {
    inner: FramedRead<ByteReader, RawResponseMessageDecoder>,
}

impl RemoteReceiver {
    fn new(rx: ByteReader) -> Self {
        RemoteReceiver {
            inner: FramedRead::new(rx, Default::default()),
        }
    }

    async fn expect_envelope<F>(&mut self, lane: &str, f: F)
    where
        F: FnOnce(Notification<Bytes, Bytes>),
    {
        let next = self.inner.next().await;

        match next {
            Some(Ok(ResponseMessage {
                origin,
                path,
                envelope,
            })) => {
                assert_eq!(origin, AGENT_ID);
                assert_eq!(path, RelativePath::new(NODE, lane));
                f(envelope);
            }
            ow => {
                panic!("Unexpected result: {:?}", ow);
            }
        }
    }

    async fn expect_linked(&mut self, lane: &str) {
        self.expect_envelope(lane, |envelope| {
            assert!(matches!(envelope, Notification::Linked));
        })
        .await
    }

    async fn expect_value_event(&mut self, lane: &str, value: i32) {
        self.expect_envelope(lane, |envelope| {
            if let Notification::Event(body) = envelope {
                let expected_body = format!("{}", value);
                let body_str = std::str::from_utf8(body.as_ref()).expect("Corrupted body.");
                assert_eq!(body_str, expected_body);
            } else {
                panic!("Unexpected envelope: {:?}", envelope);
            }
        })
        .await
    }

    async fn expect_unlinked(&mut self, lane: &str) {
        self.expect_envelope(lane, |envelope| {
            assert!(matches!(envelope, Notification::Unlinked(_)));
        })
        .await
    }

    async fn expect_clean_shutdown(self, expected_lanes: Vec<&str>) {
        let mut lanes: HashSet<&str> = expected_lanes.into_iter().collect();
        let RemoteReceiver { inner } = self;
        let results = inner.collect::<Vec<_>>().await;
        for result in results {
            match result {
                Ok(ResponseMessage {
                    origin,
                    path,
                    envelope: Notification::Unlinked(_),
                }) => {
                    assert_eq!(origin, AGENT_ID);
                    assert_eq!(&path.node, NODE);
                    let lane = &path.lane;
                    assert!(lanes.remove(lane.as_str()));
                }
                ow => {
                    panic!("Unexpected result: {:?}", ow);
                }
            }
        }
        if !lanes.is_empty() {
            panic!("Some lanes were not unlinked: {:?}", lanes);
        }
    }
}

async fn attach_remote(
    remote_id: Uuid,
    messages_tx: &mpsc::Sender<WriteTaskMessage>,
) -> RemoteReceiver {
    let (tx, rx) = byte_channel(BUFFER_SIZE);
    assert!(messages_tx
        .send(WriteTaskMessage::Remote {
            id: remote_id,
            writer: tx
        })
        .await
        .is_ok());
    RemoteReceiver::new(rx)
}
async fn link_remote(remote_id: Uuid, lane: &str, messages_tx: &mpsc::Sender<WriteTaskMessage>) {
    let msg = RwCoorindationMessage::Link {
        origin: remote_id,
        lane: Text::new(lane),
    };
    assert!(messages_tx.send(WriteTaskMessage::Coord(msg)).await.is_ok());
}

async fn unlink_remote(remote_id: Uuid, lane: &str, messages_tx: &mpsc::Sender<WriteTaskMessage>) {
    let msg = RwCoorindationMessage::Unlink {
        origin: remote_id,
        lane: Text::new(lane),
    };
    assert!(messages_tx.send(WriteTaskMessage::Coord(msg)).await.is_ok());
}

const RID1: RoutingAddr = RoutingAddr::remote(1);

#[tokio::test]
async fn attach_remote_no_link() {
    run_test_case(TEST_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx: _instr_tx,
        } = context;

        let reader = attach_remote(RID1.into(), &messages_tx).await;

        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![]).await;
    })
    .await;
}

#[tokio::test]
async fn attach_and_link_remote() {
    run_test_case(TEST_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx: _instr_tx,
        } = context;

        let mut reader = attach_remote(RID1.into(), &messages_tx).await;
        link_remote(RID1.into(), VAL_LANE, &messages_tx).await;

        reader.expect_linked(VAL_LANE).await;
        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![VAL_LANE]).await;
    })
    .await;
}

#[tokio::test]
async fn receive_message_when_linked_remote() {
    run_test_case(TEST_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx,
        } = context;

        let mut reader = attach_remote(RID1.into(), &messages_tx).await;
        link_remote(RID1.into(), VAL_LANE, &messages_tx).await;
        reader.expect_linked(VAL_LANE).await;

        instr_tx.value_event(VAL_LANE, 747);
        reader.expect_value_event(VAL_LANE, 747).await;

        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![VAL_LANE]).await;
    })
    .await;
}

#[tokio::test]
async fn explicitly_unlink_remote() {
    run_test_case(TEST_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx: _instr_tx,
        } = context;

        let mut reader = attach_remote(RID1.into(), &messages_tx).await;

        link_remote(RID1.into(), VAL_LANE, &messages_tx).await;
        reader.expect_linked(VAL_LANE).await;

        unlink_remote(RID1.into(), VAL_LANE, &messages_tx).await;
        reader.expect_unlinked(VAL_LANE).await;

        stop_sender.trigger();
        // The remote shouldn't be unlinked again.
        reader.expect_clean_shutdown(vec![]).await;
    })
    .await;
}
