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

use std::collections::HashMap;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
use swimos_agent::agent_lifecycle::item_event::ItemEvent;
use swimos_agent::agent_lifecycle::on_start::OnStart;
use swimos_agent::agent_lifecycle::on_stop::OnStop;
use swimos_agent::agent_lifecycle::on_timer::OnTimer;
use swimos_agent::agent_lifecycle::HandlerContext;
use swimos_agent::agent_model::AgentSpec;
use swimos_agent::agent_model::{ItemDescriptor, ItemFlags};
use swimos_agent::event_handler::{
    ActionContext, EventHandler, HandlerAction, HandlerActionExt, StepResult,
};
use swimos_agent::lanes::{MapLaneSelectRemove, MapLaneSelectUpdate, ValueLaneSelectSet};
use swimos_agent::AgentMetadata;
use swimos_agent_protocol::MapMessage;
use swimos_api::address::Address;
use swimos_api::agent::{DownlinkKind, WarpLaneKind};
use swimos_model::{Text, Value};
use swimos_utilities::trigger;
use thiserror::Error;

use crate::connector::EgressContext;
use crate::lifecycle::fixture::{
    drive_downlink, run_handle_with_futs, DownlinkRecord, RequestsRecord, TimerRecord,
};
use crate::{
    BaseConnector, ConnectorAgent, ConnectorFuture, EgressConnector, EgressConnectorLifecycle,
    EgressConnectorSender, MapLaneSelectorFn, MessageSource, SendResult, ValueLaneSelectorFn,
};

#[derive(Default)]
struct State {
    events: Vec<Event>,
    busy_for: usize,
}

impl State {
    fn push(&mut self, event: Event) {
        self.events.push(event)
    }
}

#[derive(Default, Clone)]
struct TestConnector {
    state: Arc<Mutex<State>>,
}

impl TestConnector {
    fn push(&self, event: Event) {
        self.state.lock().push(event);
    }

    fn make_handler(&self, event: Event) -> TestHandler {
        TestHandler {
            event: Some(event),
            state: self.state.clone(),
        }
    }

    fn take_events(&self) -> Vec<Event> {
        let mut guard = self.state.lock();
        std::mem::take(&mut guard.events)
    }
}

impl TestConnector {
    fn new(busy: usize) -> Self {
        TestConnector {
            state: Arc::new(Mutex::new(State {
                events: vec![],
                busy_for: busy,
            })),
        }
    }
}

#[derive(Clone)]
struct TestSender {
    state: Arc<Mutex<State>>,
}

#[derive(Copy, Clone, Default, Debug, Error)]
#[error("Failed")]
struct TestError;

#[derive(Clone, PartialEq, Eq, Debug)]
enum Event {
    Start,
    CreateSender,
    Stop,
    SendLane {
        name: String,
        key: Option<Value>,
        value: Value,
    },
    SendDownlink {
        address: Address<String>,
        key: Option<Value>,
        value: Value,
    },
    Timer(u64),
    SendHandler,
}

struct TestHandler {
    event: Option<Event>,
    state: Arc<Mutex<State>>,
}

impl HandlerAction<ConnectorAgent> for TestHandler {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<ConnectorAgent>,
        _meta: AgentMetadata,
        _context: &ConnectorAgent,
    ) -> StepResult<Self::Completion> {
        if let Some(ev) = self.event.take() {
            self.state.lock().push(ev);
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}

impl BaseConnector for TestConnector {
    fn on_start(
        &self,
        init_complete: trigger::Sender,
    ) -> impl EventHandler<crate::ConnectorAgent> + '_ {
        let context: HandlerContext<ConnectorAgent> = Default::default();
        self.make_handler(Event::Start)
            .followed_by(context.effect(move || {
                init_complete.trigger();
            }))
    }

    fn on_stop(&self) -> impl EventHandler<crate::ConnectorAgent> + '_ {
        self.make_handler(Event::Stop)
    }
}

const HOST: &str = "ws://host:8080";
const NODE1: &str = "/node1";
const NODE2: &str = "/node2";
const LANE1: &str = "value_lane";
const LANE2: &str = "map_lane";

fn value_lane_addr() -> Address<&'static str> {
    Address::new(Some(HOST), NODE1, LANE1)
}

fn map_lane_addr() -> Address<&'static str> {
    Address::new(None, NODE2, LANE2)
}

impl EgressConnector for TestConnector {
    type SendError = TestError;

    type Sender = TestSender;

    fn initialize(&self, context: &mut dyn EgressContext) {
        context.open_event_downlink(value_lane_addr());
        context.open_map_downlink(map_lane_addr());
    }

    fn make_sender(
        &self,
        _agent_params: &HashMap<String, String>,
    ) -> Result<Self::Sender, Self::SendError> {
        self.push(Event::CreateSender);
        Ok(TestSender {
            state: self.state.clone(),
        })
    }
}

const TIMEOUT: Duration = Duration::from_secs(11);
const ID: u64 = 746347;

impl EgressConnectorSender<TestError> for TestSender {
    fn send(
        &self,
        name: MessageSource<'_>,
        key: Option<&Value>,
        value: &Value,
    ) -> Option<SendResult<impl ConnectorFuture<TestError>, TestError>> {
        let TestSender { state } = self;
        let mut guard = state.lock();
        let State { events, busy_for } = &mut *guard;
        if *busy_for > 0 {
            *busy_for -= 1;
            Some(SendResult::RequestCallback(TIMEOUT, ID))
        } else {
            match name {
                MessageSource::Lane(name) => events.push(Event::SendLane {
                    name: name.to_string(),
                    key: key.cloned(),
                    value: value.clone(),
                }),
                MessageSource::Downlink(addr) => events.push(Event::SendDownlink {
                    address: addr.clone(),
                    key: key.cloned(),
                    value: value.clone(),
                }),
            }
            let handler = TestHandler {
                event: Some(Event::SendHandler),
                state: self.state.clone(),
            };
            Some(SendResult::Suspend(async move { Ok(handler) }.boxed()))
        }
    }

    fn timer_event(
        &self,
        timer_id: u64,
    ) -> Option<SendResult<impl ConnectorFuture<TestError>, TestError>> {
        assert_eq!(timer_id, ID);
        let TestSender { state } = self;
        let mut guard = state.lock();
        let State { busy_for, events } = &mut *guard;
        if *busy_for > 0 {
            *busy_for -= 1;
            Some(SendResult::RequestCallback(TIMEOUT, ID))
        } else {
            events.push(Event::Timer(timer_id));
            let handler = TestHandler {
                event: Some(Event::SendHandler),
                state: self.state.clone(),
            };
            Some(SendResult::Suspend(async move { Ok(handler) }.boxed()))
        }
    }
}

async fn init_connector(
    agent: &ConnectorAgent,
    connector: &TestConnector,
    lifecycle: &EgressConnectorLifecycle<TestConnector>,
) -> (DownlinkRecord, DownlinkRecord) {
    let handler = lifecycle.on_start();

    let RequestsRecord {
        mut downlinks,
        timers,
        ..
    } = run_handle_with_futs(agent, handler)
        .await
        .expect("Handler failed.");

    let events = connector.take_events();
    assert_eq!(events, vec![Event::Start, Event::CreateSender,]);

    assert!(timers.is_empty());
    let second_dl = downlinks.pop();
    let first_dl = downlinks.pop();
    assert!(downlinks.is_empty());
    match (first_dl, second_dl) {
        (Some(first), Some(second)) => {
            let DownlinkRecord {
                path, make_channel, ..
            } = &first;
            assert_eq!(path, &value_lane_addr());
            assert_eq!(make_channel.kind(), DownlinkKind::Event);

            let DownlinkRecord {
                path, make_channel, ..
            } = &second;
            assert_eq!(path, &map_lane_addr());
            assert_eq!(make_channel.kind(), DownlinkKind::MapEvent);
            (first, second)
        }
        _ => panic!("Expected 2 downlinks, found {}.", downlinks.len()),
    }
}

const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";

fn create_lanes(agent: &ConnectorAgent) {
    let value_desc = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Value,
        flags: ItemFlags::TRANSIENT,
    };
    let map_desc = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Map,
        flags: ItemFlags::TRANSIENT,
    };
    agent
        .register_dynamic_item(VALUE_LANE, value_desc)
        .expect("Value lane registration failed.");
    agent
        .register_dynamic_item(MAP_LANE, map_desc)
        .expect("Map lane registration failed.");
}

#[tokio::test]
async fn connector_lifecycle_start() {
    let connector = TestConnector::default();
    let agent = ConnectorAgent::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    init_connector(&agent, &connector, &lifecycle).await;
}

#[tokio::test]
async fn connector_lifecycle_stop_uninitialized() {
    let connector = TestConnector::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    let handler = lifecycle.on_stop();
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let events = connector.take_events();
    assert_eq!(events, vec![Event::Stop]);
}

#[tokio::test]
async fn connector_lifecycle_stop_initialized() {
    let connector = TestConnector::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    init_connector(&agent, &connector, &lifecycle).await;

    let handler = lifecycle.on_stop();
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let events = connector.take_events();
    assert_eq!(events, vec![Event::Stop]);
}

fn set_value_lane(value: Value) -> impl EventHandler<ConnectorAgent> + 'static {
    ValueLaneSelectSet::new(ValueLaneSelectorFn::new(VALUE_LANE.to_string()), value)
}

#[tokio::test]
async fn connector_lifecycle_value_lane_event_immediate() {
    let connector = TestConnector::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    init_connector(&agent, &connector, &lifecycle).await;
    create_lanes(&agent);

    let handler = set_value_lane(Value::Int32Value(4));
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let handler = lifecycle
        .item_event(&agent, VALUE_LANE)
        .expect("No pending event.");
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let events = connector.take_events();

    let expected = vec![
        Event::SendLane {
            name: VALUE_LANE.to_string(),
            key: None,
            value: Value::Int32Value(4),
        },
        Event::SendHandler,
    ];
    assert_eq!(events, expected);
}

#[tokio::test]
async fn connector_lifecycle_value_lane_event_busy() {
    let connector = TestConnector::new(1);
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    init_connector(&agent, &connector, &lifecycle).await;
    create_lanes(&agent);

    let handler = set_value_lane(Value::Int32Value(4));
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let handler = lifecycle
        .item_event(&agent, VALUE_LANE)
        .expect("No pending event.");
    let RequestsRecord {
        downlinks,
        timers,
        lanes,
    } = run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.");

    assert!(downlinks.is_empty());
    assert!(lanes.is_empty());

    let id = match timers.as_slice() {
        &[TimerRecord { id, .. }] => id,
        _ => panic!("Timer expected."),
    };

    assert!(connector.take_events().is_empty());

    let handler = lifecycle.on_timer(id);

    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let events = connector.take_events();

    let expected = vec![Event::Timer(id), Event::SendHandler];
    assert_eq!(events, expected);
}

#[tokio::test]
async fn connector_lifecycle_value_lane_event_busy_twice() {
    let connector = TestConnector::new(2);
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    init_connector(&agent, &connector, &lifecycle).await;
    create_lanes(&agent);

    let handler = set_value_lane(Value::Int32Value(4));
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    // Item event triggers send which is busy and schedules a callback.

    let handler = lifecycle
        .item_event(&agent, VALUE_LANE)
        .expect("No pending event.");
    let RequestsRecord {
        downlinks,
        timers,
        lanes,
    } = run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.");

    assert!(downlinks.is_empty());
    assert!(lanes.is_empty());

    let id = match timers.as_slice() {
        &[TimerRecord { id, .. }] => id,
        _ => panic!("Timer expected."),
    };

    assert!(connector.take_events().is_empty());

    // Callback occurs but the sender is still busy and another callback is requested.

    let handler = lifecycle.on_timer(id);

    let RequestsRecord {
        downlinks,
        timers,
        lanes,
    } = run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.");
    assert!(downlinks.is_empty());
    assert!(lanes.is_empty());

    let id = match timers.as_slice() {
        &[TimerRecord { id, .. }] => id,
        _ => panic!("Timer expected."),
    };

    assert!(connector.take_events().is_empty());

    // The second callback occurs and now the send can go ahead.

    let handler = lifecycle.on_timer(id);

    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let events = connector.take_events();

    let expected = vec![Event::Timer(id), Event::SendHandler];
    assert_eq!(events, expected);
}

#[tokio::test]
async fn connector_lifecycle_value_from_downlink() {
    let connector = TestConnector::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    let (
        DownlinkRecord {
            path, make_channel, ..
        },
        _,
    ) = init_connector(&agent, &connector, &lifecycle).await;

    let data = futures::stream::iter([Value::from(56)]);
    let mut dl_stream = pin!(drive_downlink::<Value>(make_channel, &agent, data.boxed()));

    while let Some(requests) = dl_stream.next().await {
        assert!(requests.is_empty());
    }

    let events = connector.take_events();

    let expected = vec![
        Event::SendDownlink {
            address: addr(path),
            key: None,
            value: Value::from(56),
        },
        Event::SendHandler,
    ];
    assert_eq!(events, expected);
}

fn addr(address: Address<Text>) -> Address<String> {
    Address {
        host: address.host.map(|h| h.into()),
        node: address.node.into(),
        lane: address.lane.into(),
    }
}

fn update_map_lane(key: Value, value: Value) -> impl EventHandler<ConnectorAgent> + 'static {
    MapLaneSelectUpdate::new(MapLaneSelectorFn::new(MAP_LANE.to_string()), key, value)
}

fn remove_map_lane(key: Value) -> impl EventHandler<ConnectorAgent> + 'static {
    MapLaneSelectRemove::new(MapLaneSelectorFn::new(MAP_LANE.to_string()), key)
}

#[tokio::test]
async fn connector_lifecycle_map_lane_update_event_immediate() {
    let connector = TestConnector::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    init_connector(&agent, &connector, &lifecycle).await;
    create_lanes(&agent);

    let handler = update_map_lane(Value::from("hello"), Value::from(78));
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let handler = lifecycle
        .item_event(&agent, MAP_LANE)
        .expect("No pending event.");
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let events = connector.take_events();

    let expected = vec![
        Event::SendLane {
            name: MAP_LANE.to_string(),
            key: Some(Value::from("hello")),
            value: Value::from(78),
        },
        Event::SendHandler,
    ];
    assert_eq!(events, expected);
}

#[tokio::test]
async fn connector_lifecycle_map_lane_remove_event_immediate() {
    let connector = TestConnector::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    init_connector(&agent, &connector, &lifecycle).await;
    create_lanes(&agent);

    //Run update so there is something to remove.
    let handler = update_map_lane(Value::from("hello"), Value::from(78));
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let handler = lifecycle
        .item_event(&agent, MAP_LANE)
        .expect("No pending event.");
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let events = connector.take_events();

    let expected = vec![
        Event::SendLane {
            name: MAP_LANE.to_string(),
            key: Some(Value::from("hello")),
            value: Value::from(78),
        },
        Event::SendHandler,
    ];
    assert_eq!(events, expected);

    // Remove the entry we just added.

    let handler = remove_map_lane(Value::from("hello"));
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let handler = lifecycle
        .item_event(&agent, MAP_LANE)
        .expect("No pending event.");
    assert!(run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.")
        .is_empty());

    let events = connector.take_events();

    let expected = vec![
        Event::SendLane {
            name: MAP_LANE.to_string(),
            key: Some(Value::from("hello")),
            value: Value::Extant,
        },
        Event::SendHandler,
    ];
    assert_eq!(events, expected);
}

#[tokio::test]
async fn connector_lifecycle_map_update_from_downlink() {
    let connector = TestConnector::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    let (
        _,
        DownlinkRecord {
            path, make_channel, ..
        },
    ) = init_connector(&agent, &connector, &lifecycle).await;

    let data = futures::stream::iter([MapMessage::Update {
        key: Value::from("hello"),
        value: Value::from(945),
    }]);
    let mut dl_stream = pin!(drive_downlink::<MapMessage<Value, Value>>(
        make_channel,
        &agent,
        data.boxed()
    ));

    while let Some(requests) = dl_stream.next().await {
        assert!(requests.is_empty());
    }

    let events = connector.take_events();

    let expected = vec![
        Event::SendDownlink {
            address: addr(path),
            key: Some(Value::from("hello")),
            value: Value::from(945),
        },
        Event::SendHandler,
    ];
    assert_eq!(events, expected);
}

#[tokio::test]
async fn connector_lifecycle_map_remove_from_downlink() {
    let connector = TestConnector::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    let (
        _,
        DownlinkRecord {
            path, make_channel, ..
        },
    ) = init_connector(&agent, &connector, &lifecycle).await;

    let data = futures::stream::iter([MapMessage::Remove {
        key: Value::from("hello"),
    }]);
    let mut dl_stream = pin!(drive_downlink::<MapMessage<Value, Value>>(
        make_channel,
        &agent,
        data.boxed()
    ));

    while let Some(requests) = dl_stream.next().await {
        assert!(requests.is_empty());
    }

    let events = connector.take_events();

    let expected = vec![
        Event::SendDownlink {
            address: addr(path),
            key: Some(Value::from("hello")),
            value: Value::Extant,
        },
        Event::SendHandler,
    ];
    assert_eq!(events, expected);
}
