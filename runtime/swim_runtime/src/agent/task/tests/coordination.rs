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
        task::{AgentRuntimeTask, InitialEndpoints, LaneEndpoint},
        AgentAttachmentRequest, AgentRuntimeRequest, Io,
    },
    routing::RoutingAddr,
};
use futures::{
    future::{join3, Either},
    stream::SelectAll,
    Future, StreamExt,
};
use std::fmt::Debug;
use swim_api::{
    agent::UplinkKind,
    protocol::{agent::LaneRequest, map::MapMessage},
};
use swim_model::Text;
use swim_utilities::{io::byte_channel::byte_channel, trigger};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{
    make_config, LaneReader, MapLaneSender, ValueLaneSender, BUFFER_SIZE, DEFAULT_TIMEOUT,
    MAP_LANE, QUEUE_SIZE, TEST_TIMEOUT, VAL_LANE,
};

struct CreateLane {
    name: Text,
    kind: UplinkKind,
}

struct FakeAgent {
    initial: Vec<LaneEndpoint<Io>>,
    stopping: trigger::Receiver,
    request_tx: mpsc::Sender<AgentRuntimeRequest>,
    create_rx: mpsc::UnboundedReceiver<CreateLane>,
}

impl FakeAgent {
    fn new(
        initial: Vec<LaneEndpoint<Io>>,
        stopping: trigger::Receiver,
        request_tx: mpsc::Sender<AgentRuntimeRequest>,
        create_rx: mpsc::UnboundedReceiver<CreateLane>,
    ) -> Self {
        FakeAgent {
            initial,
            stopping,
            request_tx,
            create_rx,
        }
    }

    async fn run(self) {
        let FakeAgent {
            initial,
            stopping,
            request_tx,
            create_rx,
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
                    value_lanes.insert(name.clone(), (0, ValueLaneSender::new(io_tx)));
                }
                UplinkKind::Map => {
                    let m: BTreeMap<Text, i32> = BTreeMap::new();
                    map_lanes.insert(name.clone(), (m, MapLaneSender::new(io_tx)));
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
    }
}

struct TestContext {
    att_tx: mpsc::Sender<AgentAttachmentRequest>,
    create_tx: mpsc::UnboundedSender<CreateLane>,
    stop_tx: trigger::Sender,
}

const AGENT_ID: RoutingAddr = RoutingAddr::plane(1);
const NODE: &str = "/node";

async fn run_test_case<F, Fut>(inactive_timeout: Duration, test_case: F) -> Fut::Output
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future + Send,
    Fut::Output: Debug,
{
    let config = make_config(inactive_timeout);
    let (req_tx, req_rx) = mpsc::channel(QUEUE_SIZE.get());
    let (att_tx, att_rx) = mpsc::channel(QUEUE_SIZE.get());
    let (create_tx, create_rx) = mpsc::unbounded_channel();
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

    let agent = FakeAgent::new(agent_endpoints, stop_rx.clone(), req_tx, create_rx);

    let context = TestContext {
        att_tx,
        create_tx,
        stop_tx,
    };

    let test_case_task = test_case(context);

    let (_, _, result) = tokio::time::timeout(
        TEST_TIMEOUT,
        join3(agent_task.run(), agent.run(), test_case_task),
    )
    .await
    .expect("Test timed out.");
    result
}

#[tokio::test]
async fn immediate_shutdown() {
    run_test_case(DEFAULT_TIMEOUT, |context| async move {
        let TestContext {
            att_tx: _att_tx,
            create_tx: _create_tx,
            stop_tx,
        } = context;

        stop_tx.trigger();
    })
    .await;
}
