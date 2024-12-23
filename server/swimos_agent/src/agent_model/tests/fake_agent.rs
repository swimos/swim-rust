// Copyright 2015-2024 Swim Inc.
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
    cell::RefCell,
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::{Bytes, BytesMut};
use swimos_agent_protocol::{
    encoding::lane::{MapLaneResponseEncoder, ValueLaneResponseEncoder},
    LaneResponse, MapLaneResponse, MapMessage, MapOperation,
};
use swimos_api::{
    agent::{HttpLaneRequest, WarpLaneKind},
    error::DynamicRegistrationError,
    http::{HttpRequest, HttpResponse, StatusCode, SupportedMethod, Version},
};
use tokio::sync::mpsc;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::agent_model::{ItemDescriptor, ItemSpec, MapLikeInitializer, ValueLikeInitializer};
use crate::{
    agent_model::{AgentDescription, AgentSpec, ItemFlags, WriteResult},
    event_handler::{ActionContext, HandlerAction, Modification, StepResult},
    meta::AgentMetadata,
};

use super::{
    TestEvent, CMD_ID, CMD_LANE, DYN_MAP_LANE, DYN_VAL_LANE, FIRST_DYN_ID, HTTP_ID, HTTP_LANE,
    MAP_ID, MAP_LANE, SYNC_VALUE, VAL_ID, VAL_LANE,
};

#[derive(Debug)]
pub struct TestAgent {
    receiver: Option<mpsc::UnboundedReceiver<TestEvent>>,
    http_receiver: Option<mpsc::UnboundedReceiver<HttpRequest<Bytes>>>,
    sender: mpsc::UnboundedSender<TestEvent>,
    http_sender: mpsc::UnboundedSender<HttpRequest<Bytes>>,
    staged_value: RefCell<Option<i32>>,
    staged_dyn_value: RefCell<Option<i32>>,
    staged_map: RefCell<Option<MapOperation<i32, i32>>>,
    staged_dyn_map: RefCell<Option<MapOperation<i32, i32>>>,
    sync_ids: RefCell<VecDeque<Uuid>>,
    cmd: RefCell<Option<i32>>,
    http_requests: RefCell<Vec<HttpLaneRequest>>,
    dyn_lanes: RefCell<HashMap<String, (u64, ItemDescriptor)>>,
    dyn_id: AtomicU64,
}

impl Default for TestAgent {
    fn default() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (http_tx, http_rx) = mpsc::unbounded_channel();
        Self {
            receiver: Some(rx),
            http_receiver: Some(http_rx),
            sender: tx,
            http_sender: http_tx,
            staged_value: Default::default(),
            staged_dyn_value: Default::default(),
            staged_map: Default::default(),
            staged_dyn_map: Default::default(),
            sync_ids: Default::default(),
            cmd: Default::default(),
            http_requests: Default::default(),
            dyn_lanes: Default::default(),
            dyn_id: AtomicU64::new(FIRST_DYN_ID),
        }
    }
}

impl TestAgent {
    pub fn take_receiver(&mut self) -> mpsc::UnboundedReceiver<TestEvent> {
        self.receiver.take().expect("Receiver taken twice.")
    }

    pub fn take_http_receiver(&mut self) -> mpsc::UnboundedReceiver<HttpRequest<Bytes>> {
        self.http_receiver.take().expect("Receiver taken twice.")
    }

    pub fn take_cmd(&self) -> i32 {
        let mut guard = self.cmd.borrow_mut();
        guard.take().expect("No command present.")
    }

    pub fn stage_value(&self, n: i32) {
        self.staged_value.borrow_mut().replace(n);
    }

    pub fn stage_dyn_value(&self, n: i32) {
        self.staged_dyn_value.borrow_mut().replace(n);
    }

    pub fn stage_map(&self, op: MapOperation<i32, i32>) {
        self.staged_map.borrow_mut().replace(op);
    }

    pub fn stage_dyn_map(&self, op: MapOperation<i32, i32>) {
        self.staged_dyn_map.borrow_mut().replace(op);
    }

    pub fn add_sync(&self, id: Uuid) {
        self.sync_ids.borrow_mut().push_back(id);
    }

    pub fn stage_http_request(&self, request: HttpLaneRequest) {
        self.http_requests.borrow_mut().push(request);
    }

    pub fn satisfy_http_requests(&self) {
        for req in self.http_requests.borrow_mut().drain(..) {
            let (req, tx) = req.into_parts();
            let payload = match req.method.supported_method() {
                Some(SupportedMethod::Get) => Bytes::from(b"Hello".as_ref()),
                None => panic!("Unsupported method."),
                _ => Bytes::new(),
            };
            let response = HttpResponse {
                status_code: StatusCode::OK,
                version: Version::HTTP_1_1,
                headers: vec![],
                payload,
            };
            tx.send(response).expect("Request dropped.");
        }
    }
}

pub struct TestHandler {
    event: Option<TestEvent>,
}

pub struct TestHttpHandler {
    request: Option<HttpLaneRequest>,
}

impl From<TestEvent> for TestHandler {
    fn from(ev: TestEvent) -> Self {
        TestHandler { event: Some(ev) }
    }
}

impl AgentDescription for TestAgent {
    fn item_name(&self, id: u64) -> Option<Cow<'_, str>> {
        match id {
            VAL_ID => Some(Cow::Borrowed(VAL_LANE)),
            CMD_ID => Some(Cow::Borrowed(CMD_LANE)),
            MAP_ID => Some(Cow::Borrowed(MAP_LANE)),
            HTTP_ID => Some(Cow::Borrowed(HTTP_LANE)),
            _ => None,
        }
    }
}

impl AgentSpec for TestAgent {
    type ValCommandHandler<'a> = TestHandler
    where
        Self: 'a;

    type MapCommandHandler<'a> = TestHandler
    where
        Self: 'a;

    type OnSyncHandler = TestHandler;

    type HttpRequestHandler = TestHttpHandler;

    fn item_specs() -> HashMap<&'static str, ItemSpec> {
        let mut lanes = HashMap::new();

        lanes.insert(
            VAL_LANE,
            ItemSpec::new(
                VAL_ID,
                VAL_LANE,
                ItemDescriptor::WarpLane {
                    kind: WarpLaneKind::Value,
                    flags: ItemFlags::TRANSIENT,
                },
            ),
        );
        lanes.insert(
            CMD_LANE,
            ItemSpec::new(
                CMD_ID,
                CMD_LANE,
                ItemDescriptor::WarpLane {
                    kind: WarpLaneKind::Command,
                    flags: ItemFlags::TRANSIENT,
                },
            ),
        );
        lanes.insert(
            MAP_LANE,
            ItemSpec::new(
                MAP_ID,
                MAP_LANE,
                ItemDescriptor::WarpLane {
                    kind: WarpLaneKind::Map,
                    flags: ItemFlags::TRANSIENT,
                },
            ),
        );
        lanes.insert(
            HTTP_LANE,
            ItemSpec::new(HTTP_ID, HTTP_LANE, ItemDescriptor::Http),
        );
        lanes
    }

    fn on_value_command<'a>(
        &self,
        _: &'a mut (),
        lane: &str,
        body: BytesMut,
    ) -> Option<Self::ValCommandHandler<'a>> {
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
            DYN_VAL_LANE if self.dyn_lanes.borrow().contains_key(DYN_VAL_LANE) => {
                if let Some((id, _)) = self.dyn_lanes.borrow().get(DYN_VAL_LANE) {
                    Some(
                        TestEvent::DynValue {
                            id: *id,
                            body: bytes_to_i32(body),
                        }
                        .into(),
                    )
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn init_value_like_item(&self, _item: &str) -> Option<ValueLikeInitializer<Self>>
    where
        Self: 'static,
    {
        None
    }

    fn init_map_like_item(&self, _item: &str) -> Option<MapLikeInitializer<Self>>
    where
        Self: 'static,
    {
        None
    }

    fn on_map_command<'a>(
        &self,
        _: &'a mut (),
        lane: &str,
        body: MapMessage<BytesMut, BytesMut>,
    ) -> Option<Self::MapCommandHandler<'a>> {
        match lane {
            MAP_LANE => Some(
                TestEvent::Map {
                    body: interpret_map_op(body),
                }
                .into(),
            ),
            DYN_MAP_LANE if self.dyn_lanes.borrow().contains_key(DYN_MAP_LANE) => {
                if let Some((id, _)) = self.dyn_lanes.borrow().get(DYN_MAP_LANE) {
                    Some(
                        TestEvent::DynMap {
                            id: *id,
                            body: interpret_map_op(body),
                        }
                        .into(),
                    )
                } else {
                    None
                }
            }
            _ => None,
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
            DYN_VAL_LANE => {
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
                    if self.staged_dyn_value.borrow().is_some() {
                        Some(WriteResult::DataStillAvailable)
                    } else {
                        Some(WriteResult::Done)
                    }
                } else {
                    let mut guard = self.staged_dyn_value.borrow_mut();
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
            DYN_MAP_LANE => {
                let mut guard = self.staged_dyn_map.borrow_mut();
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

    fn on_http_request(
        &self,
        lane: &str,
        request: HttpLaneRequest,
    ) -> Result<Self::HttpRequestHandler, HttpLaneRequest> {
        if lane == HTTP_LANE {
            Ok(TestHttpHandler {
                request: Some(request),
            })
        } else {
            Err(request)
        }
    }

    fn register_dynamic_item(
        &self,
        name: &str,
        descriptor: ItemDescriptor,
    ) -> Result<u64, DynamicRegistrationError> {
        let mut guard = self.dyn_lanes.borrow_mut();
        if let Entry::Vacant(entry) = guard.entry(name.to_string()) {
            let id = self.dyn_id.fetch_add(1, Ordering::SeqCst);
            entry.insert((id, descriptor));
            self.sender
                .send(TestEvent::LaneRegistration {
                    id,
                    name: name.to_string(),
                    descriptor,
                })
                .expect("Channel closed.");
            Ok(id)
        } else {
            Err(DynamicRegistrationError::DuplicateName(name.to_string()))
        }
    }

    type Deserializers = ();

    fn initialize_deserializers(&self) -> Self::Deserializers {}
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
        if let Some(mut event) = event.take() {
            let modified_item = match &mut event {
                TestEvent::Value { body } => {
                    context.stage_value(*body);
                    Some(Modification::of(VAL_ID))
                }
                TestEvent::DynValue { id, body } => {
                    context.stage_dyn_value(*body);
                    Some(Modification::of(*id))
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
                TestEvent::DynMap { id, body } => {
                    context.stage_dyn_map(to_op(*body));
                    Some(Modification::of(*id))
                }
                TestEvent::Sync { id } => {
                    context.add_sync(*id);
                    Some(Modification::no_trigger(VAL_ID))
                }
                TestEvent::LaneRegistration { .. } => None,
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

impl HandlerAction<TestAgent> for TestHttpHandler {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let TestHttpHandler { request } = self;
        if let Some(request) = request.take() {
            let req_cpy = request.request.clone();
            context.stage_http_request(request);
            context
                .http_sender
                .send(req_cpy)
                .expect("Receiver dropped.");
            StepResult::Complete {
                modified_item: Some(Modification::trigger_only(HTTP_ID)),
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
