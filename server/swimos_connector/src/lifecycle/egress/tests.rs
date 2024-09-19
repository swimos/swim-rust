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
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use parking_lot::Mutex;
use swimos_agent::agent_lifecycle::on_start::OnStart;
use swimos_agent::agent_lifecycle::on_stop::OnStop;
use swimos_agent::agent_lifecycle::HandlerContext;
use swimos_agent::agent_model::AgentSpec;
use swimos_agent::agent_model::{ItemDescriptor, ItemFlags};
use swimos_agent::event_handler::{
    ActionContext, EventHandler, HandlerAction, HandlerActionExt, StepResult,
};
use swimos_agent::AgentMetadata;
use swimos_api::address::Address;
use swimos_api::agent::{DownlinkKind, WarpLaneKind};
use swimos_model::Value;
use swimos_utilities::trigger;
use thiserror::Error;

use crate::lifecycle::fixture::{run_handle_with_futs, DownlinkRecord};
use crate::{
    BaseConnector, ConnectorAgent, ConnectorFuture, EgressConnector, EgressConnectorLifecycle,
    EgressConnectorSender, EgressContext, MessageSource, SendResult,
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

fn value_lane_addr() -> Address<String> {
    Address::new(Some(HOST.to_string()), NODE1.to_string(), LANE1.to_string())
}

fn map_lane_addr() -> Address<String> {
    Address::new(None, NODE2.to_string(), LANE2.to_string())
}

impl EgressConnector for TestConnector {
    type SendError = TestError;

    type Sender = TestSender;

    fn open_downlinks(&self, context: &mut dyn EgressContext) {
        context.open_value_downlink(value_lane_addr());
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

const TIMEOUT: Duration = Duration::from_secs(1);
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
        let TestSender { state } = self;
        let mut guard = state.lock();
        let State { events, busy_for } = &mut *guard;
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
) {
    let handler = lifecycle.on_start();

    let downlinks = run_handle_with_futs(agent, handler)
        .await
        .expect("Handler failed.");

    let events = connector.take_events();
    assert_eq!(events, vec![Event::Start, Event::CreateSender,]);

    match downlinks.as_slice() {
        [first, second] => {
            let DownlinkRecord {
                path, make_channel, ..
            } = first;
            assert_eq!(path, &value_lane_addr());
            assert_eq!(make_channel.kind(), DownlinkKind::Event);

            let DownlinkRecord {
                path, make_channel, ..
            } = second;
            assert_eq!(path, &map_lane_addr());
            assert_eq!(make_channel.kind(), DownlinkKind::MapEvent);
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
    let downlinks = run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.");

    let events = connector.take_events();
    assert_eq!(events, vec![Event::Stop]);

    assert!(downlinks.is_empty());
}

#[tokio::test]
async fn connector_lifecycle_stop_initialized() {
    let connector = TestConnector::default();
    let lifecycle = EgressConnectorLifecycle::new(connector.clone());
    let agent = ConnectorAgent::default();

    init_connector(&agent, &connector, &lifecycle).await;

    let handler = lifecycle.on_stop();
    let downlinks = run_handle_with_futs(&agent, handler)
        .await
        .expect("Handler failed.");

    let events = connector.take_events();
    assert_eq!(events, vec![Event::Stop]);

    assert!(downlinks.is_empty());
}
