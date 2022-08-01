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

use std::{collections::HashMap, time::Duration};

use futures::{
    future::{join, join3},
    Future, StreamExt,
};
use swim_api::agent::UplinkKind;
use swim_messages::protocol::Notification;
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{byte_channel, ByteWriter},
    trigger::{self, promise},
};
use tokio::{sync::mpsc, time::Instant};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use crate::{
    agent::{
        task::{
            tests::RemoteReceiver, timeout_coord, write_task, LaneEndpoint, RwCoorindationMessage,
            WriteTaskConfiguration, WriteTaskMessage,
        },
        DisconnectionReason,
    },
    routing::RoutingAddr,
};

use super::{
    make_config, Instruction, Instructions, MapLaneSender, ValueLaneSender, BUFFER_SIZE,
    DEFAULT_TIMEOUT, INACTIVE_TEST_TIMEOUT, MAP_LANE, QUEUE_SIZE, TEST_TIMEOUT, VAL_LANE,
};

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
                        tx.update_event(key, value).await;
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

const AGENT_ID: Uuid = *RoutingAddr::plane(1).uuid();
const NODE: &str = "/node";

use std::fmt::Debug;

async fn run_test_case<F, Fut>(inactive_timeout: Duration, test_case: F) -> Fut::Output
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future + Send,
    Fut::Output: Debug,
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

    let (_, _, result) =
        tokio::time::timeout(TEST_TIMEOUT, join3(fake_agent.run(), write, test_task))
            .await
            .expect("Test timed out.");
    result
}

#[tokio::test]
async fn clean_shutdown_no_remotes() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
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

async fn attach_remote(
    remote_id: Uuid,
    messages_tx: &mpsc::Sender<WriteTaskMessage>,
) -> RemoteReceiver {
    let (completion_tx, completion_rx) = promise::promise();
    let (tx, rx) = byte_channel(BUFFER_SIZE);
    assert!(messages_tx
        .send(WriteTaskMessage::Remote {
            id: remote_id,
            writer: tx,
            completion: completion_tx,
            on_attached: None,
        })
        .await
        .is_ok());
    RemoteReceiver::new(AGENT_ID, NODE.to_string(), rx, completion_rx)
}

async fn attach_remote_and_wait(
    remote_id: Uuid,
    messages_tx: &mpsc::Sender<WriteTaskMessage>,
) -> RemoteReceiver {
    let (completion_tx, completion_rx) = promise::promise();
    let (tx, rx) = byte_channel(BUFFER_SIZE);
    let (attach_tx, attach_rx) = trigger::trigger();
    assert!(messages_tx
        .send(WriteTaskMessage::Remote {
            id: remote_id,
            writer: tx,
            completion: completion_tx,
            on_attached: Some(attach_tx),
        })
        .await
        .is_ok());
    assert!(attach_rx.await.is_ok());
    RemoteReceiver::new(AGENT_ID, NODE.to_string(), rx, completion_rx)
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
const RID2: RoutingAddr = RoutingAddr::remote(2);

#[tokio::test]
async fn attach_remote_no_link() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx: _instr_tx,
        } = context;

        let reader = attach_remote_and_wait(RID1.into(), &messages_tx).await;

        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![], None).await;
    })
    .await;
}

#[tokio::test]
async fn attach_and_link_remote() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
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
        reader.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn receive_message_when_linked_remote() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
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
        reader.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn receive_messages_when_linked_remote() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
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
        instr_tx.value_event(VAL_LANE, 367);
        reader.expect_value_event(VAL_LANE, 367).await;

        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn explicitly_unlink_remote() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
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
        reader.expect_clean_shutdown(vec![], None).await;
    })
    .await;
}

#[tokio::test]
async fn broadcast_message_when_linked_multiple_remotes() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx,
        } = context;

        let mut reader1 = attach_remote(RID1.into(), &messages_tx).await;
        link_remote(RID1.into(), VAL_LANE, &messages_tx).await;

        let mut reader2 = attach_remote(RID2.into(), &messages_tx).await;
        link_remote(RID2.into(), VAL_LANE, &messages_tx).await;

        reader1.expect_linked(VAL_LANE).await;
        reader2.expect_linked(VAL_LANE).await;

        instr_tx.value_event(VAL_LANE, 747);

        join(
            reader1.expect_value_event(VAL_LANE, 747),
            reader2.expect_value_event(VAL_LANE, 747),
        )
        .await;

        stop_sender.trigger();
        join(
            reader1.expect_clean_shutdown(vec![VAL_LANE], None),
            reader2.expect_clean_shutdown(vec![VAL_LANE], None),
        )
        .await;
    })
    .await;
}

#[tokio::test]
async fn value_synced_message_are_targetted() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx,
        } = context;

        let mut reader1 = attach_remote(RID1.into(), &messages_tx).await;
        link_remote(RID1.into(), VAL_LANE, &messages_tx).await;

        let mut reader2 = attach_remote(RID2.into(), &messages_tx).await;
        link_remote(RID2.into(), VAL_LANE, &messages_tx).await;

        reader1.expect_linked(VAL_LANE).await;
        reader2.expect_linked(VAL_LANE).await;

        instr_tx.value_synced_event(RID1.into(), VAL_LANE, 64);

        reader1.expect_value_synced(VAL_LANE, 64).await;

        stop_sender.trigger();
        join(
            reader1.expect_clean_shutdown(vec![VAL_LANE], None),
            reader2.expect_clean_shutdown(vec![VAL_LANE], None),
        )
        .await;
    })
    .await;
}

#[tokio::test]
async fn broadcast_map_message_when_linked_multiple_remotes() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx,
        } = context;

        let mut reader1 = attach_remote(RID1.into(), &messages_tx).await;
        link_remote(RID1.into(), MAP_LANE, &messages_tx).await;

        let mut reader2 = attach_remote(RID2.into(), &messages_tx).await;
        link_remote(RID2.into(), MAP_LANE, &messages_tx).await;

        reader1.expect_linked(MAP_LANE).await;
        reader2.expect_linked(MAP_LANE).await;

        instr_tx.map_event(MAP_LANE, "key", 49);

        join(
            reader1.expect_map_event(MAP_LANE, "key", 49),
            reader2.expect_map_event(MAP_LANE, "key", 49),
        )
        .await;

        stop_sender.trigger();
        join(
            reader1.expect_clean_shutdown(vec![MAP_LANE], None),
            reader2.expect_clean_shutdown(vec![MAP_LANE], None),
        )
        .await;
    })
    .await;
}

#[tokio::test]
async fn receive_map_messages_when_linked() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx,
        } = context;

        let mut reader = attach_remote(RID1.into(), &messages_tx).await;
        link_remote(RID1.into(), MAP_LANE, &messages_tx).await;

        reader.expect_linked(MAP_LANE).await;

        instr_tx.map_event(MAP_LANE, "key", 42);
        reader.expect_map_event(MAP_LANE, "key", 42).await;
        instr_tx.map_event(MAP_LANE, "key2", 56);
        reader.expect_map_event(MAP_LANE, "key2", 56).await;

        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![MAP_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn map_synced_message_are_targetted() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx,
        } = context;

        let mut reader1 = attach_remote(RID1.into(), &messages_tx).await;
        link_remote(RID1.into(), MAP_LANE, &messages_tx).await;

        let mut reader2 = attach_remote(RID2.into(), &messages_tx).await;
        link_remote(RID2.into(), MAP_LANE, &messages_tx).await;

        reader1.expect_linked(MAP_LANE).await;
        reader2.expect_linked(MAP_LANE).await;

        instr_tx.map_syncing_event(RID2.into(), MAP_LANE, "key", 389);
        instr_tx.map_synced_event(RID2.into(), MAP_LANE);

        reader2.expect_map_event(MAP_LANE, "key", 389).await;
        reader2.expect_map_synced(MAP_LANE).await;

        stop_sender.trigger();
        join(
            reader1.expect_clean_shutdown(vec![MAP_LANE], None),
            reader2.expect_clean_shutdown(vec![MAP_LANE], None),
        )
        .await;
    })
    .await;
}

#[tokio::test]
async fn write_task_stops_if_no_remotes() {
    run_test_case(INACTIVE_TEST_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx: _messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx: _instr_tx,
        } = context;

        stop_sender
    })
    .await;
}

#[tokio::test]
async fn write_task_votes_to_stop() {
    run_test_case(INACTIVE_TEST_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2,
            vote_rx,
            instr_tx: _instr_tx,
        } = context;

        let mut reader = attach_remote(RID1.into(), &messages_tx).await;

        let before = Instant::now();
        link_remote(RID1.into(), VAL_LANE, &messages_tx).await;

        reader.expect_linked(VAL_LANE).await;
        //Voting on behalf of the missing read task.
        assert!(!vote2.vote());
        vote_rx.await;
        let after = Instant::now();
        let elapsed = after.duration_since(before);
        assert!(elapsed >= INACTIVE_TEST_TIMEOUT);
        reader
            .expect_clean_shutdown(vec![VAL_LANE], Some(DisconnectionReason::AgentTimedOut))
            .await;
        stop_sender
    })
    .await;
}

#[tokio::test]
async fn write_task_rescinds_vote_to_stop() {
    run_test_case(INACTIVE_TEST_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2,
            vote_rx: _vote_rx,
            instr_tx,
        } = context;

        let mut reader = attach_remote(RID1.into(), &messages_tx).await;

        link_remote(RID1.into(), VAL_LANE, &messages_tx).await;

        reader.expect_linked(VAL_LANE).await;

        tokio::time::sleep(2 * INACTIVE_TEST_TIMEOUT).await;

        instr_tx.value_event(VAL_LANE, 747);
        reader.expect_value_event(VAL_LANE, 747).await;

        assert!(!vote2.vote());

        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

const NUM_RECORDS: i32 = 512;

#[tokio::test]
async fn backpressure_relief_on_value_lanes() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
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

        for i in 0..NUM_RECORDS {
            instr_tx.value_event(VAL_LANE, i);
        }

        let mut prev = None;

        while prev.unwrap_or_default() < NUM_RECORDS - 1 {
            reader
                .expect_envelope(VAL_LANE, |envelope| match envelope {
                    Notification::Event(body) => {
                        let body_str = std::str::from_utf8(body.as_ref()).expect("Invalid UTF8");
                        let n = body_str.parse::<i32>().expect("Invalid integer.");
                        assert!((0..NUM_RECORDS).contains(&n));
                        if let Some(m) = prev {
                            assert!(n > m);
                        }
                        prev = Some(n);
                    }
                    ow => panic!("Unexpected envelope: {:?}", ow),
                })
                .await;
        }

        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

const EXPECTED_PREFIX: &str = "@update(key:test) ";

#[tokio::test]
async fn backpressure_relief_on_map_lanes() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx,
        } = context;

        let mut reader = attach_remote(RID1.into(), &messages_tx).await;
        link_remote(RID1.into(), MAP_LANE, &messages_tx).await;
        reader.expect_linked(MAP_LANE).await;

        for i in 0..NUM_RECORDS {
            instr_tx.map_event(MAP_LANE, "test", i);
        }

        let mut prev = None;

        while prev.unwrap_or_default() < NUM_RECORDS - 1 {
            reader
                .expect_envelope(MAP_LANE, |envelope| match envelope {
                    Notification::Event(body) => {
                        let body_str = std::str::from_utf8(body.as_ref()).expect("Invalid UTF8");
                        assert!(body_str.starts_with(EXPECTED_PREFIX));
                        let value_str = &body_str[EXPECTED_PREFIX.len()..];
                        let n = value_str.parse::<i32>().expect("Invalid integer.");
                        assert!((0..NUM_RECORDS).contains(&n));
                        if let Some(m) = prev {
                            assert!(n > m);
                        } else {
                            assert_eq!(n, 0);
                        }
                        prev = Some(n);
                    }
                    ow => panic!("Unexpected envelope: {:?}", ow),
                })
                .await;
        }

        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![MAP_LANE], None).await;
    })
    .await;
}

const SYNCED_AT: i32 = 450;

#[tokio::test]
async fn backpressure_relief_on_map_lanes_with_synced() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
        let TestContext {
            stop_sender,
            messages_tx,
            vote2: _vote2,
            vote_rx: _vote_rx,
            instr_tx,
        } = context;

        let mut reader = attach_remote(RID1.into(), &messages_tx).await;
        link_remote(RID1.into(), MAP_LANE, &messages_tx).await;
        reader.expect_linked(MAP_LANE).await;

        for i in 0..SYNCED_AT {
            instr_tx.map_event(MAP_LANE, "test", i);
        }
        instr_tx.map_syncing_event(RID1.into(), MAP_LANE, "test", SYNCED_AT);
        instr_tx.map_synced_event(RID1.into(), MAP_LANE);
        for i in (SYNCED_AT + 1)..NUM_RECORDS {
            instr_tx.map_event(MAP_LANE, "test", i);
        }

        let mut prev = None;
        let mut synced = false;

        while prev.unwrap_or_default() < NUM_RECORDS - 1 {
            reader
                .expect_envelope(MAP_LANE, |envelope| match envelope {
                    Notification::Event(body) => {
                        let body_str = std::str::from_utf8(body.as_ref()).expect("Invalid UTF8");
                        assert!(body_str.starts_with(EXPECTED_PREFIX));
                        let value_str = &body_str[EXPECTED_PREFIX.len()..];
                        let n = value_str.parse::<i32>().expect("Invalid integer.");
                        assert!((0..NUM_RECORDS).contains(&n));
                        if let Some(m) = prev {
                            assert!(n > m);
                        } else {
                            assert_eq!(n, 0);
                        }
                        prev = Some(n);
                    }
                    Notification::Synced => {
                        if synced {
                            panic!("Synced twice.");
                        } else {
                            if let Some(m) = prev {
                                assert!(m >= SYNCED_AT);
                            } else {
                                panic!("Synced before seen any values.");
                            }
                            synced = true;
                        }
                    }
                    ow => panic!("Unexpected envelope: {:?}", ow),
                })
                .await;
        }
        assert!(synced);

        stop_sender.trigger();
        reader.expect_clean_shutdown(vec![MAP_LANE], None).await;
    })
    .await;
}
