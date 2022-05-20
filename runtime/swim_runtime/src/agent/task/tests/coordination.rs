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
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use crate::{
    agent::{
        task::{
            tests::{RemoteReceiver, RemoteSender},
            AgentRuntimeTask, InitialEndpoints, LaneEndpoint,
        },
        AgentAttachmentRequest, AgentRuntimeRequest, Io,
    },
    routing::RoutingAddr,
};
use futures::{
    future::{join, join3, Either},
    stream::SelectAll,
    Future, StreamExt,
};
use std::fmt::Debug;
use swim_api::{
    agent::UplinkKind,
    protocol::{agent::LaneRequest, map::MapMessage},
};
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::byte_channel,
    trigger::{self, promise},
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{
    make_config, LaneReader, MapLaneSender, ValueLaneSender, BUFFER_SIZE, DEFAULT_TIMEOUT,
    MAP_LANE, QUEUE_SIZE, TEST_TIMEOUT, VAL_LANE,
};

#[derive(Debug, Clone)]
enum Event {
    ValueCommand {
        name: Text,
        n: i32,
    },
    MapCommand {
        name: Text,
        cmd: MapMessage<Text, i32>,
    },
}

struct CreateLane {
    name: Text,
    kind: UplinkKind,
}

#[derive(Default)]
struct AgentState {
    value_lanes: HashMap<Text, i32>,
    map_lanes: HashMap<Text, BTreeMap<Text, i32>>,
}

impl AgentState {
    fn new(
        val: HashMap<Text, (i32, ValueLaneSender)>,
        map: HashMap<Text, (BTreeMap<Text, i32>, MapLaneSender)>,
    ) -> Self {
        let value_lanes = val.into_iter().map(|(k, (n, _))| (k, n)).collect();
        let map_lanes = map.into_iter().map(|(k, (map, _))| (k, map)).collect();
        AgentState {
            value_lanes,
            map_lanes,
        }
    }
}

struct FakeAgent {
    initial: Vec<LaneEndpoint<Io>>,
    initial_state: AgentState,
    stopping: trigger::Receiver,
    request_tx: mpsc::Sender<AgentRuntimeRequest>,
    create_rx: mpsc::UnboundedReceiver<CreateLane>,
    event_tx: mpsc::UnboundedSender<Event>,
}

impl FakeAgent {
    fn new(
        initial: Vec<LaneEndpoint<Io>>,
        initial_state: Option<AgentState>,
        stopping: trigger::Receiver,
        request_tx: mpsc::Sender<AgentRuntimeRequest>,
        create_rx: mpsc::UnboundedReceiver<CreateLane>,
        event_tx: mpsc::UnboundedSender<Event>,
    ) -> Self {
        FakeAgent {
            initial,
            initial_state: initial_state.unwrap_or_default(),
            stopping,
            request_tx,
            create_rx,
            event_tx,
        }
    }

    async fn run(self) -> AgentState {
        let FakeAgent {
            initial,
            mut initial_state,
            stopping,
            request_tx,
            create_rx,
            event_tx,
        } = self;

        let mut value_lanes = HashMap::new();
        let mut map_lanes = HashMap::new();
        let mut lanes = SelectAll::new();

        for endpoint in initial {
            let LaneEndpoint {
                name,
                kind,
                io: (io_tx, io_rx),
            } = endpoint;
            match kind {
                UplinkKind::Value => {
                    let init = initial_state.value_lanes.remove(&name).unwrap_or_default();
                    value_lanes.insert(name.clone(), (init, ValueLaneSender::new(io_tx)));
                }
                UplinkKind::Map => {
                    let init = initial_state.map_lanes.remove(&name).unwrap_or_default();
                    map_lanes.insert(name.clone(), (init, MapLaneSender::new(io_tx)));
                }
            }
            lanes.push(LaneReader::new(LaneEndpoint {
                name,
                kind,
                io: io_rx,
            }));
        }
        let mut create_stream = UnboundedReceiverStream::new(create_rx).take_until(stopping);
        loop {
            tokio::select! {
                maybe_result = lanes.next() => {
                    if let Some((name, lane_result)) = maybe_result {
                        match lane_result {
                            Ok(Either::Left(message)) => {
                                if let Some((value, sender)) = value_lanes.get_mut(name.as_str()) {
                                    match message {
                                        LaneRequest::Command(v) => {
                                            assert!(event_tx.send(Event::ValueCommand { name: name.clone(), n: v }).is_ok());
                                            *value = v;
                                            sender.event(v).await;
                                        },
                                        LaneRequest::Sync(id) => {
                                            sender.synced(id, *value).await;
                                        }
                                    }
                                }
                            }
                            Ok(Either::Right(message)) => {
                                if let Some((map, sender)) = map_lanes.get_mut(name.as_str()) {
                                    match message {
                                        LaneRequest::Command(msg) => {
                                            assert!(event_tx.send(Event::MapCommand { name: name.clone(), cmd: msg.clone() }).is_ok());
                                            match msg {
                                                MapMessage::Update { key, value } => {
                                                    map.insert(key.clone(), value);
                                                    sender.update_event(key, value).await;
                                                }
                                                MapMessage::Remove { key } => {
                                                    if map.remove(&key).is_some() {
                                                        sender.remove_event(key).await;
                                                    }
                                                },
                                                MapMessage::Clear => {
                                                    map.clear();
                                                    sender.clear_event().await;
                                                },
                                                MapMessage::Take(n) => {
                                                    let mut it = std::mem::take(map).into_iter();
                                                    for (k, v) in (&mut it).take(n as usize) {
                                                        map.insert(k, v);
                                                    }
                                                    for (k, _) in it {
                                                        sender.remove_event(k).await;
                                                    }
                                                },
                                                MapMessage::Drop(n) => {
                                                    let mut it = std::mem::take(map).into_iter();
                                                    for (k, _) in (&mut it).take(n as usize) {
                                                        sender.remove_event(k).await;
                                                    }
                                                    for (k, v) in it {
                                                        map.insert(k, v);
                                                    }
                                                },
                                            }
                                        },
                                        LaneRequest::Sync(id) => {
                                            for (k, v) in map {
                                                sender.sync_event(id, k.clone(), *v).await;
                                            }
                                            sender.synced(id).await;
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                panic!("Bad frame for {}: {:?}", name, e);
                            }
                        }
                    } else {
                        break;
                    }
                },
                maybe_create = create_stream.next() => {
                    if let Some(CreateLane { name, kind }) = maybe_create {
                        let (tx, rx) = oneshot::channel();
                        assert!(request_tx.send(AgentRuntimeRequest::AddLane {
                             name: name.clone(), kind, config: None, promise: tx,
                            }).await.is_ok());
                        let (io_tx, io_rx) = rx.await
                            .expect("Failed to receive response.")
                            .expect("Failed to add new lane.");
                        match kind {
                            UplinkKind::Value => {
                                value_lanes.insert(name.clone(), (0, ValueLaneSender::new(io_tx)));
                            }
                            UplinkKind::Map => {
                                let m: BTreeMap<Text, i32> = BTreeMap::new();
                                map_lanes.insert(name.clone(), (m, MapLaneSender::new(io_tx)));
                            }
                        }
                        lanes.push(LaneReader::new(LaneEndpoint { name, kind, io: io_rx }));
                    } else {
                        break;
                    }
                }
            }
        }
        AgentState::new(value_lanes, map_lanes)
    }
}

struct Events(mpsc::UnboundedReceiver<Event>);

impl Events {
    async fn await_value_command(&mut self, expected_name: &str, value: i32) {
        let Events(inner) = self;
        let event = inner.recv().await;
        match event {
            Some(Event::ValueCommand { name, n }) => {
                assert_eq!(name, expected_name);
                assert_eq!(n, value);
            }
            Some(ow) => panic!("Unexpected event: {:?}", ow),
            _ => panic!("Agent failed."),
        }
    }

    async fn await_map_command(
        &mut self,
        expected_name: &str,
        expected_key: &str,
        expected_value: i32,
    ) {
        let Events(inner) = self;
        let event = inner.recv().await;
        match event {
            Some(Event::MapCommand {
                name,
                cmd: MapMessage::Update { key, value },
            }) => {
                assert_eq!(name, expected_name);
                assert_eq!(key, expected_key);
                assert_eq!(value, expected_value);
            }
            Some(ow) => panic!("Unexpected event: {:?}", ow),
            _ => panic!("Agent failed."),
        }
    }
}

struct TestContext {
    att_tx: mpsc::Sender<AgentAttachmentRequest>,
    create_tx: mpsc::UnboundedSender<CreateLane>,
    event_rx: Events,
    stop_tx: trigger::Sender,
}

const AGENT_ID: RoutingAddr = RoutingAddr::plane(1);
const NODE: &str = "/node";
const RID1: RoutingAddr = RoutingAddr::remote(5);
const RID2: RoutingAddr = RoutingAddr::remote(89);
const RID3: RoutingAddr = RoutingAddr::remote(222);

async fn run_test_case<F, Fut>(
    inactive_timeout: Duration,
    intial_state: Option<AgentState>,
    test_case: F,
) -> (AgentState, Fut::Output)
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future + Send,
    Fut::Output: Debug,
{
    let config = make_config(inactive_timeout);
    let (req_tx, req_rx) = mpsc::channel(QUEUE_SIZE.get());
    let (att_tx, att_rx) = mpsc::channel(QUEUE_SIZE.get());
    let (create_tx, create_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = trigger::trigger();

    let mut runtime_endpoints = vec![];
    let mut agent_endpoints = vec![];
    let (tx_in_val, rx_in_val) = byte_channel(BUFFER_SIZE);
    let (tx_out_val, rx_out_val) = byte_channel(BUFFER_SIZE);
    let (tx_in_map, rx_in_map) = byte_channel(BUFFER_SIZE);
    let (tx_out_map, rx_out_map) = byte_channel(BUFFER_SIZE);

    runtime_endpoints.push(LaneEndpoint::new(
        Text::new(VAL_LANE),
        UplinkKind::Value,
        (tx_in_val, rx_out_val),
    ));
    runtime_endpoints.push(LaneEndpoint::new(
        Text::new(MAP_LANE),
        UplinkKind::Map,
        (tx_in_map, rx_out_map),
    ));

    agent_endpoints.push(LaneEndpoint::new(
        Text::new(VAL_LANE),
        UplinkKind::Value,
        (tx_out_val, rx_in_val),
    ));
    agent_endpoints.push(LaneEndpoint::new(
        Text::new(MAP_LANE),
        UplinkKind::Map,
        (tx_out_map, rx_in_map),
    ));

    let init = InitialEndpoints::new(req_rx, runtime_endpoints);

    let agent_task = AgentRuntimeTask::new(
        AGENT_ID,
        Text::new(NODE),
        init,
        att_rx,
        stop_rx.clone(),
        config,
    );

    let agent = FakeAgent::new(
        agent_endpoints,
        intial_state,
        stop_rx.clone(),
        req_tx,
        create_rx,
        event_tx,
    );

    let context = TestContext {
        att_tx,
        create_tx,
        stop_tx,
        event_rx: Events(event_rx),
    };

    let test_case_task = test_case(context);

    let (_, state, result) = tokio::time::timeout(
        TEST_TIMEOUT,
        join3(agent_task.run(), agent.run(), test_case_task),
    )
    .await
    .expect("Test timed out.");
    (state, result)
}

#[tokio::test]
async fn immediate_shutdown() {
    let (state, _) = run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx: _att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;

        stop_tx.trigger();
    })
    .await;

    let AgentState {
        mut value_lanes,
        mut map_lanes,
    } = state;
    assert_eq!(value_lanes.remove(VAL_LANE), Some(0));
    assert_eq!(map_lanes.remove(MAP_LANE), Some(BTreeMap::new()));
}

async fn attach_remote(
    remote_id: RoutingAddr,
    att_tx: &mpsc::Sender<AgentAttachmentRequest>,
) -> (RemoteSender, RemoteReceiver) {
    let (in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, out_rx) = byte_channel(BUFFER_SIZE);
    let (completion_tx, completion_rx) = promise::promise();
    let req = AgentAttachmentRequest::new(remote_id.into(), (out_tx, in_rx), completion_tx);
    assert!(att_tx.send(req).await.is_ok());

    let tx = RemoteSender::new(NODE.to_string(), remote_id, in_tx);
    let rx = RemoteReceiver::new(AGENT_ID, NODE.to_string(), out_rx, completion_rx);
    (tx, rx)
}

#[tokio::test]
async fn link_lane() {
    run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;
        let (mut sender, mut receiver) = attach_remote(RID1, &att_tx).await;

        sender.link(VAL_LANE).await;
        receiver.expect_linked(VAL_LANE).await;

        stop_tx.trigger();

        receiver.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn set_value() {
    let (mut state, _) = run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            mut event_rx,
            stop_tx,
        } = context;
        let (mut sender, receiver) = attach_remote(RID1, &att_tx).await;

        sender.value_command(VAL_LANE, 7).await;
        event_rx.await_value_command(VAL_LANE, 7).await;

        stop_tx.trigger();

        receiver.expect_clean_shutdown(vec![], None).await;
    })
    .await;
    assert_eq!(state.value_lanes.remove(VAL_LANE), Some(7));
}

#[tokio::test]
async fn insert_value() {
    let (mut state, _) = run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            mut event_rx,
            stop_tx,
        } = context;
        let (mut sender, receiver) = attach_remote(RID1, &att_tx).await;

        sender.map_command(MAP_LANE, "a", 1).await;
        event_rx.await_map_command(MAP_LANE, "a", 1).await;

        stop_tx.trigger();

        receiver.expect_clean_shutdown(vec![], None).await;
    })
    .await;
    let mut expected = BTreeMap::new();
    expected.insert(Text::new("a"), 1);
    assert_eq!(state.map_lanes.remove(MAP_LANE), Some(expected));
}

#[tokio::test]
async fn unlink_when_not_linked_does_nothing() {
    run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            mut event_rx,
            stop_tx,
        } = context;
        let (mut sender, receiver) = attach_remote(RID1, &att_tx).await;

        sender.unlink(VAL_LANE).await;

        //Sending a value and waiting for it to be processed ensures we are after the unlink has been processed.
        sender.value_command(VAL_LANE, 1).await;
        event_rx.await_value_command(VAL_LANE, 1).await;

        stop_tx.trigger();

        receiver.expect_clean_shutdown(vec![], None).await;
    })
    .await;
}

#[tokio::test]
async fn unlink_linked_lane() {
    run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;
        let (mut sender, mut receiver) = attach_remote(RID1, &att_tx).await;

        sender.link(VAL_LANE).await;
        receiver.expect_linked(VAL_LANE).await;

        sender.unlink(VAL_LANE).await;
        receiver.expect_unlinked(VAL_LANE).await;

        stop_tx.trigger();

        receiver.expect_clean_shutdown(vec![], None).await;
    })
    .await;
}

#[tokio::test]
async fn sync_value_lane() {
    let mut init_state = AgentState::default();
    init_state.value_lanes.insert(Text::new(VAL_LANE), 67);

    run_test_case(DEFAULT_TIMEOUT, Some(init_state), |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;
        let (mut sender, mut receiver) = attach_remote(RID1, &att_tx).await;

        sender.link(VAL_LANE).await;
        sender.sync(VAL_LANE).await;

        receiver.expect_linked(VAL_LANE).await;
        receiver.expect_value_synced(VAL_LANE, 67).await;

        stop_tx.trigger();

        receiver.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn sync_empty_map_lane() {
    run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;
        let (mut sender, mut receiver) = attach_remote(RID1, &att_tx).await;

        sender.link(MAP_LANE).await;
        sender.sync(MAP_LANE).await;

        receiver.expect_linked(MAP_LANE).await;
        receiver.expect_map_synced(MAP_LANE).await;

        stop_tx.trigger();

        receiver.expect_clean_shutdown(vec![MAP_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn sync_nonempty_map_lane() {
    let mut initial_state = AgentState::default();
    let mut init_map = BTreeMap::new();
    init_map.insert(Text::new("a"), 1);
    init_map.insert(Text::new("b"), 2);
    initial_state
        .map_lanes
        .insert(Text::new(MAP_LANE), init_map.clone());

    run_test_case(DEFAULT_TIMEOUT, Some(initial_state), |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;
        let (mut sender, mut receiver) = attach_remote(RID1, &att_tx).await;

        sender.link(MAP_LANE).await;
        sender.sync(MAP_LANE).await;

        receiver.expect_linked(MAP_LANE).await;
        let mut synced_map = BTreeMap::new();
        for _ in 0..init_map.len() {
            receiver
                .expect_any_map_event(MAP_LANE, |message| match message {
                    MapMessage::Update { key, value } => {
                        assert!(!synced_map.contains_key(&key));
                        synced_map.insert(key, value);
                    }
                    ow => panic!("Unexpected map message: {:?}", ow),
                })
                .await;
        }
        assert_eq!(synced_map, init_map);
        receiver.expect_map_synced(MAP_LANE).await;

        stop_tx.trigger();

        receiver.expect_clean_shutdown(vec![MAP_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn sync_lane_implicit_link() {
    let mut init_state = AgentState::default();
    init_state.value_lanes.insert(Text::new(VAL_LANE), 67);

    run_test_case(DEFAULT_TIMEOUT, Some(init_state), |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;
        let (mut sender, mut receiver) = attach_remote(RID1, &att_tx).await;

        sender.sync(VAL_LANE).await;

        receiver.expect_linked(VAL_LANE).await;
        receiver.expect_value_synced(VAL_LANE, 67).await;

        stop_tx.trigger();

        receiver.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn receive_messages_when_linked() {
    run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;

        let v = 34;

        let (linked_tx, linked_rx) = trigger::trigger();

        let producer = async {
            let (mut sender, receiver) = attach_remote(RID1, &att_tx).await;
            assert!(linked_rx.await.is_ok());
            sender.value_command(VAL_LANE, v).await;
            receiver
        };

        let consumer = async {
            let (mut sender, mut receiver) = attach_remote(RID2, &att_tx).await;
            sender.link(VAL_LANE).await;
            receiver.expect_linked(VAL_LANE).await;
            linked_tx.trigger();
            receiver.expect_value_event(VAL_LANE, v).await;
            receiver
        };

        let (receiver1, receiver2) = join(producer, consumer).await;

        stop_tx.trigger();

        receiver1.expect_clean_shutdown(vec![], None).await;
        receiver2.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn link_two_consumers() {
    run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;

        let consumer1 = async {
            let (mut sender, mut receiver) = attach_remote(RID1, &att_tx).await;
            sender.link(VAL_LANE).await;
            receiver.expect_linked(VAL_LANE).await;
            receiver
        };

        let consumer2 = async {
            let (mut sender, mut receiver) = attach_remote(RID2, &att_tx).await;
            sender.link(VAL_LANE).await;
            receiver.expect_linked(VAL_LANE).await;
            receiver
        };

        let (receiver1, receiver2) = join(consumer1, consumer2).await;

        stop_tx.trigger();
        receiver1.expect_clean_shutdown(vec![VAL_LANE], None).await;
        receiver2.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}

#[tokio::test]
async fn receive_messages_when_liked_multiple_consumers() {
    run_test_case(DEFAULT_TIMEOUT, None, |context| async move {
        let TestContext {
            att_tx,
            create_tx: _create_tx,
            event_rx: _event_rx,
            stop_tx,
        } = context;

        let (linked_tx1, linked_rx1) = trigger::trigger();
        let (linked_tx2, linked_rx2) = trigger::trigger();

        let v = 7394784;

        let producer = async {
            let (mut sender, receiver) = attach_remote(RID1, &att_tx).await;
            assert!(linked_rx1.await.is_ok());
            assert!(linked_rx2.await.is_ok());
            sender.value_command(VAL_LANE, v).await;
            receiver
        };

        let consumer1 = async {
            let (mut sender, mut receiver) = attach_remote(RID2, &att_tx).await;
            sender.link(VAL_LANE).await;
            receiver.expect_linked(VAL_LANE).await;
            linked_tx1.trigger();
            receiver.expect_value_event(VAL_LANE, v).await;
            receiver
        };

        let consumer2 = async {
            let (mut sender, mut receiver) = attach_remote(RID3, &att_tx).await;
            sender.link(VAL_LANE).await;
            receiver.expect_linked(VAL_LANE).await;
            linked_tx2.trigger();
            receiver.expect_value_event(VAL_LANE, v).await;
            receiver
        };

        let (receiver1, receiver2, receiver3) = join3(producer, consumer1, consumer2).await;

        stop_tx.trigger();

        receiver1.expect_clean_shutdown(vec![], None).await;
        receiver2.expect_clean_shutdown(vec![VAL_LANE], None).await;
        receiver3.expect_clean_shutdown(vec![VAL_LANE], None).await;
    })
    .await;
}
