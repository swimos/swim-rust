// Copyright 2015-2023 Swim Inc.
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
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
};

use bytes::BytesMut;
use swim_api::{
    agent::HttpLaneRequest,
    protocol::{
        agent::{LaneResponse, MapLaneResponse, MapLaneResponseEncoder, ValueLaneResponseEncoder},
        map::{MapMessage, MapOperation},
    },
};
use swim_model::Text;
use tokio::sync::mpsc;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::{
    agent_model::{AgentSpec, ItemFlags, ItemKind, ItemSpec, WriteResult},
    event_handler::{ActionContext, HandlerAction, Modification, StepResult},
    meta::AgentMetadata,
};

use super::{TestEvent, CMD_ID, CMD_LANE, MAP_ID, MAP_LANE, SYNC_VALUE, VAL_ID, VAL_LANE};

#[derive(Debug)]
pub struct TestAgent {
    receiver: Option<mpsc::UnboundedReceiver<TestEvent>>,
    sender: mpsc::UnboundedSender<TestEvent>,
    staged_value: RefCell<Option<i32>>,
    staged_map: RefCell<Option<MapOperation<i32, i32>>>,
    sync_ids: RefCell<VecDeque<Uuid>>,
    cmd: RefCell<Option<i32>>,
}

impl Default for TestAgent {
    fn default() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            receiver: Some(rx),
            sender: tx,
            staged_value: Default::default(),
            staged_map: Default::default(),
            sync_ids: Default::default(),
            cmd: Default::default(),
        }
    }
}

impl TestAgent {
    pub fn take_receiver(&mut self) -> mpsc::UnboundedReceiver<TestEvent> {
        self.receiver.take().expect("Receiver taken twice.")
    }

    pub fn take_cmd(&self) -> i32 {
        let mut guard = self.cmd.borrow_mut();
        guard.take().expect("No command present.")
    }

    pub fn stage_value(&self, n: i32) {
        self.staged_value.borrow_mut().replace(n);
    }

    pub fn stage_map(&self, op: MapOperation<i32, i32>) {
        self.staged_map.borrow_mut().replace(op);
    }

    pub fn add_sync(&self, id: Uuid) {
        self.sync_ids.borrow_mut().push_back(id);
    }
}

pub struct TestHandler {
    event: Option<TestEvent>,
}

impl From<TestEvent> for TestHandler {
    fn from(ev: TestEvent) -> Self {
        TestHandler { event: Some(ev) }
    }
}

impl AgentSpec for TestAgent {
    type ValCommandHandler = TestHandler;

    type MapCommandHandler = TestHandler;

    type OnSyncHandler = TestHandler;

    type HttpRequestHandler = TestHandler;

    fn value_like_item_specs() -> HashMap<&'static str, crate::agent_model::ItemSpec> {
        let mut lanes = HashMap::new();
        lanes.insert(
            VAL_LANE,
            ItemSpec::new(ItemKind::Lane, ItemFlags::TRANSIENT),
        );
        lanes.insert(
            CMD_LANE,
            ItemSpec::new(ItemKind::Lane, ItemFlags::TRANSIENT),
        );
        lanes
    }

    fn map_like_item_specs() -> HashMap<&'static str, crate::agent_model::ItemSpec> {
        let mut lanes = HashMap::new();
        lanes.insert(
            MAP_LANE,
            ItemSpec::new(ItemKind::Lane, ItemFlags::TRANSIENT),
        );
        lanes
    }

    fn item_ids() -> HashMap<u64, Text> {
        [(VAL_ID, VAL_LANE), (MAP_ID, MAP_LANE), (CMD_ID, CMD_LANE)]
            .into_iter()
            .map(|(k, v)| (k, Text::new(v)))
            .collect()
    }

    fn on_value_command(&self, lane: &str, body: BytesMut) -> Option<Self::ValCommandHandler> {
        match lane {
            VAL_LANE => Some(
                TestEvent::Value {
                    body: bytes_to_i32(body),
                }
                .into(),
            ),
            CMD_LANE => Some(
                TestEvent::Cmd {
                    body: bytes_to_i32(body),
                }
                .into(),
            ),
            _ => None,
        }
    }

    fn on_map_command(
        &self,
        lane: &str,
        body: MapMessage<BytesMut, BytesMut>,
    ) -> Option<Self::MapCommandHandler> {
        if lane == MAP_LANE {
            Some(
                TestEvent::Map {
                    body: interpret_map_op(body),
                }
                .into(),
            )
        } else {
            None
        }
    }

    fn on_sync(&self, lane: &str, id: Uuid) -> Option<Self::OnSyncHandler> {
        if lane == VAL_LANE {
            Some(TestEvent::Sync { id }.into())
        } else {
            None
        }
    }

    fn write_event(&self, lane: &str, buffer: &mut bytes::BytesMut) -> Option<WriteResult> {
        match lane {
            VAL_LANE => {
                let mut encoder = ValueLaneResponseEncoder::default();
                if let Some(id) = self.sync_ids.borrow_mut().pop_front() {
                    let sync_message = LaneResponse::sync_event(id, SYNC_VALUE);
                    let synced_message = LaneResponse::<i32>::Synced(id);
                    encoder
                        .encode(sync_message, buffer)
                        .expect("Serialization failed.");
                    encoder
                        .encode(synced_message, buffer)
                        .expect("Serialization failed.");
                    if self.staged_value.borrow().is_some() {
                        Some(WriteResult::DataStillAvailable)
                    } else {
                        Some(WriteResult::Done)
                    }
                } else {
                    let mut guard = self.staged_value.borrow_mut();
                    if let Some(body) = guard.take() {
                        let response = LaneResponse::event(body);
                        encoder
                            .encode(response, buffer)
                            .expect("Serialization failed.");
                        Some(WriteResult::Done)
                    } else {
                        Some(WriteResult::NoData)
                    }
                }
            }
            MAP_LANE => {
                let mut guard = self.staged_map.borrow_mut();
                if let Some(body) = guard.take() {
                    let mut encoder = MapLaneResponseEncoder::default();
                    let response = MapLaneResponse::event(body);
                    encoder
                        .encode(response, buffer)
                        .expect("Serialization failed.");
                    Some(WriteResult::Done)
                } else {
                    Some(WriteResult::NoData)
                }
            }
            _ => None,
        }
    }

    fn init_value_like_item(
        &self,
        _item: &str,
    ) -> Option<Box<dyn crate::agent_model::ItemInitializer<Self, BytesMut> + Send + 'static>>
    where
        Self: 'static,
    {
        None
    }

    fn init_map_like_item(
        &self,
        _item: &str,
    ) -> Option<
        Box<
            dyn crate::agent_model::ItemInitializer<Self, MapMessage<BytesMut, BytesMut>>
                + Send
                + 'static,
        >,
    >
    where
        Self: 'static,
    {
        None
    }

    fn http_lane_names() -> HashSet<&'static str> {
        HashSet::new()
    }

    fn on_http_request(
        &self,
        _lane: &str,
        request: HttpLaneRequest,
    ) -> Result<Self::HttpRequestHandler, HttpLaneRequest> {
        Err(request)
    }
}

impl HandlerAction<TestAgent> for TestHandler {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let TestHandler { event } = self;
        if let Some(event) = event.take() {
            let modified_item = match &event {
                TestEvent::Value { body } => {
                    context.stage_value(*body);
                    Some(Modification::of(VAL_ID))
                }
                TestEvent::Cmd { body } => {
                    let mut cmd = context.cmd.borrow_mut();
                    *cmd = Some(*body);
                    Some(Modification::of(CMD_ID))
                }
                TestEvent::Map { body } => {
                    context.stage_map(to_op(*body));
                    Some(Modification::of(MAP_ID))
                }
                TestEvent::Sync { id } => {
                    context.add_sync(*id);
                    Some(Modification::no_trigger(VAL_ID))
                }
            };
            context.sender.send(event).expect("Receiver dropped.");
            StepResult::Complete {
                modified_item,
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

fn bytes_to_i32(bytes: impl AsRef<[u8]>) -> i32 {
    std::str::from_utf8(bytes.as_ref())
        .expect("Bad UTF8.")
        .parse()
        .expect("Invalid integer.")
}

fn interpret_map_op(op: MapMessage<BytesMut, BytesMut>) -> MapMessage<i32, i32> {
    match op {
        MapMessage::Update { key, value } => MapMessage::Update {
            key: bytes_to_i32(key),
            value: bytes_to_i32(value),
        },
        MapMessage::Remove { key } => MapMessage::Remove {
            key: bytes_to_i32(key),
        },
        MapMessage::Clear => MapMessage::Clear,
        MapMessage::Take(n) => MapMessage::Take(n),
        MapMessage::Drop(n) => MapMessage::Drop(n),
    }
}

fn to_op(msg: MapMessage<i32, i32>) -> MapOperation<i32, i32> {
    match msg {
        MapMessage::Update { key, value } => MapOperation::Update { key, value },
        MapMessage::Remove { key } => MapOperation::Remove { key },
        MapMessage::Clear => MapOperation::Clear,
        _ => panic!("No support for take/drop."),
    }
}
