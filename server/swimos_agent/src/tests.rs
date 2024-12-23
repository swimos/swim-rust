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

use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::{collections::HashMap, sync::Arc};

use crate::agent_lifecycle::on_init::OnInit;
use crate::agent_lifecycle::on_start::OnStart;
use crate::agent_lifecycle::on_stop::OnStop;
use crate::agent_lifecycle::{
    item_event::ItemEvent, HandlerContext, JoinMapContext, JoinValueContext,
};
use crate::agent_model::WriteResult;
use crate::event_handler::{
    BoxJoinLaneInit, EventHandler, HandlerAction, HandlerFuture, Modification, Spawner, StepResult,
};
use crate::item::{AgentItem, MapItem};
use crate::lanes::demand::Cue;
use crate::lanes::demand_map::{CueKey, DemandMapLaneSync};
use crate::lanes::http::lifecycle::HttpRequestContext;
use crate::lanes::http::{HttpLaneAccept, Recon, Response, UnitResponse};
use crate::lanes::join_map::lifecycle::JoinMapLaneLifecycle;
use crate::lanes::join_map::JoinMapLaneUpdate;
use crate::lanes::join_value::lifecycle::JoinValueLaneLifecycle;
use crate::lanes::join_value::{AfterClosed, JoinValueLaneUpdate};
use crate::lanes::{
    CommandLane, DemandLane, DemandMapLane, JoinMapLane, JoinValueLane, LaneItem,
    LinkClosedResponse, MapLane, SimpleHttpLane, ValueLane,
};
use crate::meta::AgentMetadata;
use crate::model::MapMessage;
use crate::reexport::bytes::BytesMut;
use crate::reexport::uuid::Uuid;
use crate::stores::{MapStore, ValueStore};
use crate::test_context::{NO_DOWNLINKS, NO_DYN_LANES};
use parking_lot::Mutex;
use swimos_agent_derive::{lifecycle, AgentLaneModel};
use swimos_api::address::Address;
use swimos_api::agent::DownlinkKind;
use swimos_api::error::{DownlinkRuntimeError, OpenStoreError};
use swimos_api::{
    agent::{AgentConfig, HttpLaneRequest, HttpResponseReceiver, StoreKind, WarpLaneKind},
    http::{Header, HttpRequest, HttpResponse, Method, StandardHeaderName, StatusCode, Version},
};
use swimos_form::Form;
use swimos_model::Text;
use swimos_recon::print_recon_compact;
use swimos_utilities::routing::RouteUri;

use futures::future::BoxFuture;
use swimos_api::{
    agent::{AgentContext, LaneConfig},
    error::AgentRuntimeError,
};
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};

use crate::event_handler::ActionContext;

struct NoSpawn;
pub struct DummyAgentContext;

const NO_SPAWN: NoSpawn = NoSpawn;

pub fn dummy_context<'a, Context>(
    join_lane_init: &'a mut HashMap<u64, BoxJoinLaneInit<'static, Context>>,
    command_buffer: &'a mut BytesMut,
) -> ActionContext<'a, Context> {
    ActionContext::new(
        &NO_SPAWN,
        &NO_DOWNLINKS,
        &NO_DYN_LANES,
        join_lane_init,
        command_buffer,
    )
}

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }

    fn schedule_timer(&self, _at: tokio::time::Instant, _id: u64) {
        panic!("Unexpected timer.");
    }
}

impl AgentContext for DummyAgentContext {
    fn command_channel(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        panic!("Dummy context used.");
    }

    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: WarpLaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Dummy context used.");
    }

    fn open_downlink(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        panic!("Dummy context used.");
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Dummy context used.");
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<swimos_api::agent::HttpLaneRequestChannel, AgentRuntimeError>>
    {
        panic!("Dummy context used.");
    }
}

#[derive(AgentLaneModel)]
#[agent(root(crate))]
struct TestAgent {
    value: ValueLane<i32>,
    value2: ValueLane<i32>,
    command: CommandLane<i32>,
    map: MapLane<i32, Text>,
    value_store: ValueStore<i32>,
    map_store: MapStore<i32, Text>,
    join_value: JoinValueLane<i32, Text>,
    demand: DemandLane<i32>,
    demand_map: DemandMapLane<i32, Text>,
    http: SimpleHttpLane<i32, Recon>,
    join_map: JoinMapLane<Text, i32, Text>,
}

impl From<(i32, i32, HashMap<i32, Text>, i32, HashMap<i32, Text>)> for TestAgent {
    fn from(
        (value_init, value2_init, map_init, val_store_init, map_store_init): (
            i32,
            i32,
            HashMap<i32, Text>,
            i32,
            HashMap<i32, Text>,
        ),
    ) -> Self {
        TestAgent {
            value: ValueLane::new(0, value_init),
            value2: ValueLane::new(1, value2_init),
            command: CommandLane::new(2),
            map: MapLane::new(3, map_init),
            value_store: ValueStore::new(4, val_store_init),
            map_store: MapStore::new(5, map_store_init),
            join_value: JoinValueLane::new(6),
            demand: DemandLane::new(7),
            demand_map: DemandMapLane::new(8),
            http: SimpleHttpLane::new(9),
            join_map: JoinMapLane::new(10),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ValueEvent<T> {
    Event(T),
    Set(T, Option<T>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MapEvent {
    Clear(HashMap<i32, Text>),
    Remove(HashMap<i32, Text>, i32, Text),
    Update(HashMap<i32, Text>, i32, Option<Text>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    StartOrStop,
    Value(ValueEvent<i32>),
    Command(i32),
    Map(MapEvent),
    Cued,
    Keys,
    CueKey(i32),
    HttpGet(HttpRequestContext),
    HttpPost(HttpRequestContext, i32),
    HttpPut(HttpRequestContext, i32),
    HttpDelete(HttpRequestContext),
}

#[derive(Clone, Default)]
struct LifecycleInner {
    data: Arc<Mutex<Vec<Event>>>,
}

impl LifecycleInner {
    fn push(&self, event: Event) {
        self.data.lock().push(event)
    }

    fn take(&self) -> Vec<Event> {
        let mut guard = self.data.lock();
        std::mem::take(&mut *guard)
    }
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

fn run_handler_mod<Agent, H: EventHandler<Agent>>(
    agent: &Agent,
    mut handler: H,
    modified: Option<Modification>,
) {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();
    let mut seen_mod = None;
    loop {
        match handler.step(
            &mut dummy_context(&mut join_lane_init, &mut command_buffer),
            meta,
            agent,
        ) {
            StepResult::Continue { modified_item } => match (&seen_mod, &modified_item) {
                (None, _) => seen_mod = modified_item,
                (Some(m1), Some(m2)) => {
                    assert_eq!(m1, m2);
                }
                _ => {}
            },
            StepResult::Fail(e) => {
                panic!("{}", e);
            }
            StepResult::Complete { modified_item, .. } => {
                match (&seen_mod, &modified_item) {
                    (None, _) => seen_mod = modified_item,
                    (Some(m1), Some(m2)) => {
                        assert_eq!(m1, m2);
                    }
                    _ => {}
                }
                break;
            }
        }
    }
    assert_eq!(seen_mod, modified);
    assert!(join_lane_init.is_empty());
    assert!(command_buffer.is_empty());
}

fn run_handler<Agent, H: EventHandler<Agent>>(agent: &Agent, handler: H) {
    run_handler_mod(agent, handler, None)
}

#[test]
fn on_start_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_start]
        fn my_on_start(
            &self,
            context: HandlerContext<TestAgent>,
        ) -> impl EventHandler<TestAgent> + '_ {
            context.effect(|| {
                self.0.push(Event::StartOrStop);
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    let handler = lifecycle.on_start();
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![Event::StartOrStop]);
}

#[test]
fn on_stop_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_stop]
        fn my_on_stop(
            &self,
            context: HandlerContext<TestAgent>,
        ) -> impl EventHandler<TestAgent> + '_ {
            context.effect(|| {
                self.0.push(Event::StartOrStop);
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    let handler = lifecycle.on_stop();
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![Event::StartOrStop]);
}

#[test]
fn on_start_and_stop_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_start]
        #[on_stop]
        fn my_on_start(
            &self,
            context: HandlerContext<TestAgent>,
        ) -> impl EventHandler<TestAgent> + '_ {
            context.effect(|| {
                self.0.push(Event::StartOrStop);
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    let handler = lifecycle.on_start();
    run_handler(&agent, handler);

    let handler = lifecycle.on_stop();
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![Event::StartOrStop, Event::StartOrStop]);
}

const TEST_VALUE: i32 = 12;

#[test]
fn on_command_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_command(command)]
        fn my_on_command(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Command(n));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.command.command(TEST_VALUE);
    let handler = lifecycle
        .item_event(&agent, "command")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![Event::Command(TEST_VALUE)]);
}

#[test]
fn on_event_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_event(value)]
        fn my_on_event(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Event(n)));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.value.set(TEST_VALUE);
    let handler = lifecycle
        .item_event(&agent, "value")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![Event::Value(ValueEvent::Event(TEST_VALUE))]);
}

#[test]
fn on_event_handler_store() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_event(value_store)]
        fn my_on_event(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Event(n)));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.value_store.set(TEST_VALUE);
    let handler = lifecycle
        .item_event(&agent, "value_store")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![Event::Value(ValueEvent::Event(TEST_VALUE))]);
}

#[test]
fn on_set_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_set(value)]
        fn my_on_set(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
            prev: Option<i32>,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Set(n, prev)));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.value.set(TEST_VALUE);
    let handler = lifecycle
        .item_event(&agent, "value")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(
        events,
        vec![Event::Value(ValueEvent::Set(TEST_VALUE, Some(0)))]
    );
}

#[test]
fn on_set_handler_store() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_set(value_store)]
        fn my_on_set(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
            prev: Option<i32>,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Set(n, prev)));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.value_store.set(TEST_VALUE);
    let handler = lifecycle
        .item_event(&agent, "value_store")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(
        events,
        vec![Event::Value(ValueEvent::Set(TEST_VALUE, Some(0)))]
    );
}

#[test]
fn on_event_and_set_handlers() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_event(value)]
        fn my_on_event(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Event(n)));
            })
        }

        #[on_set(value)]
        fn my_on_set(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
            prev: Option<i32>,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Set(n, prev)));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.value.set(TEST_VALUE);
    let handler = lifecycle
        .item_event(&agent, "value")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(
        events,
        vec![
            Event::Value(ValueEvent::Event(TEST_VALUE)),
            Event::Value(ValueEvent::Set(TEST_VALUE, Some(0)))
        ]
    );
}

#[test]
fn on_event_and_set_handlers_store() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_event(value_store)]
        fn my_on_event(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Event(n)));
            })
        }

        #[on_set(value_store)]
        fn my_on_set(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
            prev: Option<i32>,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Set(n, prev)));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.value_store.set(TEST_VALUE);
    let handler = lifecycle
        .item_event(&agent, "value_store")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(
        events,
        vec![
            Event::Value(ValueEvent::Event(TEST_VALUE)),
            Event::Value(ValueEvent::Set(TEST_VALUE, Some(0)))
        ]
    );
}

#[test]
fn on_event_shared_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_event(value, value2)]
        fn my_on_event(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Event(n)));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.value.set(TEST_VALUE);
    agent.value2.set(TEST_VALUE + 1);

    let handler = lifecycle
        .item_event(&agent, "value")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let handler = lifecycle
        .item_event(&agent, "value2")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(
        events,
        vec![
            Event::Value(ValueEvent::Event(TEST_VALUE)),
            Event::Value(ValueEvent::Event(TEST_VALUE + 1))
        ]
    );
}

const K1: i32 = 4;
const K2: i32 = -839;
const V1: &str = "hello";
const V2: &str = "world";

fn init_map() -> HashMap<i32, Text> {
    let mut map = HashMap::new();

    map.insert(K1, Text::new(V1));
    map.insert(K2, Text::new(V2));
    map
}

#[test]
fn on_clear_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_clear(map)]
        fn my_on_clear(
            &self,
            context: HandlerContext<TestAgent>,
            old: HashMap<i32, Text>,
        ) -> impl EventHandler<TestAgent> + '_ {
            context.effect(move || {
                self.0.push(Event::Map(MapEvent::Clear(old)));
            })
        }
    }

    let agent = TestAgent::from((0, 0, init_map(), 0, HashMap::new()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map.clear();
    let handler = lifecycle
        .item_event(&agent, "map")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let expected = init_map();
    assert_eq!(events, vec![Event::Map(MapEvent::Clear(expected))]);
}

#[test]
fn on_clear_handler_store() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_clear(map_store)]
        fn my_on_clear(
            &self,
            context: HandlerContext<TestAgent>,
            old: HashMap<i32, Text>,
        ) -> impl EventHandler<TestAgent> + '_ {
            context.effect(move || {
                self.0.push(Event::Map(MapEvent::Clear(old)));
            })
        }
    }

    let agent = TestAgent::from((0, 0, HashMap::new(), 0, init_map()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map_store.clear();
    let handler = lifecycle
        .item_event(&agent, "map_store")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let expected = init_map();
    assert_eq!(events, vec![Event::Map(MapEvent::Clear(expected))]);
}

#[test]
fn on_remove_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_remove(map)]
        fn my_on_remove(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            removed: Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Remove(map_state, key, removed)));
            })
        }
    }

    let agent = TestAgent::from((0, 0, init_map(), 0, HashMap::new()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map.remove(&K1);
    let handler = lifecycle
        .item_event(&agent, "map")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = init_map();
    expected_map.remove(&K1);
    assert_eq!(
        events,
        vec![Event::Map(MapEvent::Remove(
            expected_map,
            K1,
            Text::new(V1)
        ))]
    );
}

#[test]
fn on_remove_handler_store() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_remove(map_store)]
        fn my_on_remove(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            removed: Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Remove(map_state, key, removed)));
            })
        }
    }

    let agent = TestAgent::from((0, 0, HashMap::new(), 0, init_map()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map_store.remove(&K1);
    let handler = lifecycle
        .item_event(&agent, "map_store")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = init_map();
    expected_map.remove(&K1);
    assert_eq!(
        events,
        vec![Event::Map(MapEvent::Remove(
            expected_map,
            K1,
            Text::new(V1)
        ))]
    );
}

#[test]
fn on_update_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_update(map)]
        fn my_on_update(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            prev: Option<Text>,
            _new_value: &Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Update(map_state, key, prev)));
            })
        }
    }

    let agent = TestAgent::from((0, 0, init_map(), 0, HashMap::new()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map.update(K2, Text::new("changed"));
    let handler = lifecycle
        .item_event(&agent, "map")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = init_map();
    expected_map.insert(K2, Text::new("changed"));
    assert_eq!(
        events,
        vec![Event::Map(MapEvent::Update(
            expected_map,
            K2,
            Some(Text::new(V2))
        ))]
    );
}

#[test]
fn on_update_handler_store() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_update(map_store)]
        fn my_on_update(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            prev: Option<Text>,
            _new_value: &Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Update(map_state, key, prev)));
            })
        }
    }

    let agent = TestAgent::from((0, 0, HashMap::new(), 0, init_map()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map_store.update(K2, Text::new("changed"));
    let handler = lifecycle
        .item_event(&agent, "map_store")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = init_map();
    expected_map.insert(K2, Text::new("changed"));
    assert_eq!(
        events,
        vec![Event::Map(MapEvent::Update(
            expected_map,
            K2,
            Some(Text::new(V2))
        ))]
    );
}

#[test]
fn all_handlers() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_start]
        fn my_on_start(
            &self,
            context: HandlerContext<TestAgent>,
        ) -> impl EventHandler<TestAgent> + '_ {
            context.effect(|| {
                self.0.push(Event::StartOrStop);
            })
        }

        #[on_stop]
        fn my_on_stop(
            &self,
            context: HandlerContext<TestAgent>,
        ) -> impl EventHandler<TestAgent> + '_ {
            context.effect(|| {
                self.0.push(Event::StartOrStop);
            })
        }

        #[on_event(value)]
        fn my_on_event(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Event(n)));
            })
        }

        #[on_set(value)]
        fn my_on_set(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
            prev: Option<i32>,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Value(ValueEvent::Set(n, prev)));
            })
        }

        #[on_command(command)]
        fn my_on_command(
            &self,
            context: HandlerContext<TestAgent>,
            value: &i32,
        ) -> impl EventHandler<TestAgent> + '_ {
            let n = *value;
            context.effect(move || {
                self.0.push(Event::Command(n));
            })
        }

        #[on_remove(map)]
        fn my_on_remove(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            removed: Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Remove(map_state, key, removed)));
            })
        }

        #[on_update(map)]
        fn my_on_update(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            prev: Option<Text>,
            _new_value: &Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Update(map_state, key, prev)));
            })
        }

        #[on_clear(map)]
        fn my_on_clear(
            &self,
            context: HandlerContext<TestAgent>,
            old: HashMap<i32, Text>,
        ) -> impl EventHandler<TestAgent> + '_ {
            context.effect(move || {
                self.0.push(Event::Map(MapEvent::Clear(old)));
            })
        }
    }

    let agent = TestAgent::from((0, 0, init_map(), 0, HashMap::new()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    //The tests primarily verifies that the lifecycle compiles so we just test one representative event.
    agent.value.set(TEST_VALUE);
    let handler = lifecycle
        .item_event(&agent, "value")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(
        events,
        vec![
            Event::Value(ValueEvent::Event(TEST_VALUE)),
            Event::Value(ValueEvent::Set(TEST_VALUE, Some(0))),
        ]
    );
}

#[derive(AgentLaneModel)]
#[agent(root(crate))]
struct BorrowAgent {
    array: ValueLane<Vec<i32>>,
    string: CommandLane<String>,
    map: MapLane<i32, String>,
}

#[derive(PartialEq, Eq, Debug)]
enum BorrowEvent {
    StrCommand(String),
    VecEvent(Vec<i32>),
    VecSet(Option<Vec<i32>>, Vec<i32>),
    MapUpdate(HashMap<i32, String>, i32, Option<String>, String),
}
#[derive(Default, Clone)]
struct BorrowLcInner {
    data: Arc<Mutex<Vec<BorrowEvent>>>,
}

impl BorrowLcInner {
    fn push(&self, event: BorrowEvent) {
        self.data.lock().push(event)
    }

    fn take(&self) -> Vec<BorrowEvent> {
        let mut guard = self.data.lock();
        std::mem::take(&mut *guard)
    }
}

#[test]
fn on_command_borrow_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(BorrowLcInner);

    #[lifecycle(BorrowAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_command(string)]
        fn my_on_command(
            &self,
            context: HandlerContext<BorrowAgent>,
            value: &str,
        ) -> impl EventHandler<BorrowAgent> + '_ {
            let s = value.to_string();
            context.effect(move || {
                self.0.push(BorrowEvent::StrCommand(s));
            })
        }
    }

    let agent = BorrowAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.string.command("text".to_string());
    let handler = lifecycle
        .item_event(&agent, "string")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![BorrowEvent::StrCommand("text".to_string())]);
}

#[test]
fn on_event_borrow_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(BorrowLcInner);

    #[lifecycle(BorrowAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_event(array)]
        fn my_on_event(
            &self,
            context: HandlerContext<BorrowAgent>,
            value: &[i32],
        ) -> impl EventHandler<BorrowAgent> + '_ {
            let v = value.to_vec();
            context.effect(move || {
                self.0.push(BorrowEvent::VecEvent(v));
            })
        }
    }

    let agent = BorrowAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.array.set(vec![1, 2, 3]);
    let handler = lifecycle
        .item_event(&agent, "array")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![BorrowEvent::VecEvent(vec![1, 2, 3])]);
}

#[test]
fn on_set_borrow_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(BorrowLcInner);

    #[lifecycle(BorrowAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_set(array)]
        fn my_on_set(
            &self,
            context: HandlerContext<BorrowAgent>,
            value: &[i32],
            prev: Option<Vec<i32>>,
        ) -> impl EventHandler<BorrowAgent> + '_ {
            let v = value.to_vec();
            context.effect(move || {
                self.0.push(BorrowEvent::VecSet(prev, v));
            })
        }
    }

    let agent = BorrowAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.array.set(vec![1, 2, 3]);
    let handler = lifecycle
        .item_event(&agent, "array")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(
        events,
        vec![BorrowEvent::VecSet(Some(vec![]), vec![1, 2, 3])]
    );
}

#[test]
fn on_update_borrow_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(BorrowLcInner);

    #[lifecycle(BorrowAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_update(map)]
        fn my_on_update(
            &self,
            context: HandlerContext<BorrowAgent>,
            map: &HashMap<i32, String>,
            key: i32,
            prev: Option<String>,
            new_value: &str,
        ) -> impl EventHandler<BorrowAgent> + '_ {
            let map_state = map.clone();
            let v = new_value.to_string();
            context.effect(move || {
                self.0.push(BorrowEvent::MapUpdate(map_state, key, prev, v));
            })
        }
    }

    let agent = BorrowAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map.update(1, "hello".to_string());
    let handler = lifecycle
        .item_event(&agent, "map")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let expected_map: HashMap<i32, String> = [(1, "hello".to_string())].into_iter().collect();
    assert_eq!(
        events,
        vec![BorrowEvent::MapUpdate(
            expected_map,
            1,
            None,
            "hello".to_string()
        )]
    );
}

#[test]
fn on_update_handler_join_value() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_update(join_value)]
        fn my_on_update(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            prev: Option<Text>,
            _new_value: &Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Update(map_state, key, prev)));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    update_join_value(&agent, K2, Text::new("a"), |agent| &agent.join_value);

    let handler = lifecycle
        .item_event(&agent, "join_value")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = HashMap::new();
    expected_map.insert(K2, Text::new("a"));
    assert_eq!(
        events,
        vec![Event::Map(MapEvent::Update(expected_map, K2, None))]
    );
}

#[test]
fn on_update_handler_join_map() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_update(join_map)]
        fn my_on_update(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            prev: Option<Text>,
            _new_value: &Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Update(map_state, key, prev)));
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    update_join_map(&agent, Text::new("link"), K2, Text::new("a"), |agent| {
        &agent.join_map
    });

    let handler = lifecycle
        .item_event(&agent, "join_map")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = HashMap::new();
    expected_map.insert(K2, Text::new("a"));
    assert_eq!(
        events,
        vec![Event::Map(MapEvent::Update(expected_map, K2, None))]
    );
}

#[test]
fn on_remove_handler_join_value() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_remove(join_value)]
        fn my_on_remove(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            removed: Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Remove(map_state, key, removed)));
            })
        }
    }

    let agent = TestAgent::default();
    agent.join_value.init(init_map());
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    remove_join_value(&agent, K1, |agent| &agent.join_value);

    let handler = lifecycle
        .item_event(&agent, "join_value")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = init_map();
    expected_map.remove(&K1);
    assert_eq!(
        events,
        vec![Event::Map(MapEvent::Remove(
            expected_map,
            K1,
            Text::new(V1)
        ))]
    );
}

#[test]
fn on_remove_handler_join_map() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_remove(join_map)]
        fn my_on_remove(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            removed: Text,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Remove(map_state, key, removed)));
            })
        }
    }

    let agent = TestAgent::default();
    agent.join_map.init(init_map());
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    remove_join_map(&agent, Text::new("link"), K1, |agent| &agent.join_map);

    let handler = lifecycle
        .item_event(&agent, "join_map")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = init_map();
    expected_map.remove(&K1);
    assert_eq!(
        events,
        vec![Event::Map(MapEvent::Remove(
            expected_map,
            K1,
            Text::new(V1)
        ))]
    );
}

fn update_join_value<K, V>(
    agent: &TestAgent,
    key: K,
    value: V,
    projection: fn(&TestAgent) -> &JoinValueLane<K, V>,
) where
    K: Clone + Eq + Hash,
{
    let handler = JoinValueLaneUpdate::new(projection, key, value);
    let id = projection(agent).id();
    run_handler_mod(agent, handler, Some(Modification::of(id)));
}

fn update_join_map<L, K, V>(
    agent: &TestAgent,
    link_key: L,
    key: K,
    value: V,
    projection: fn(&TestAgent) -> &JoinMapLane<L, K, V>,
) where
    L: Clone + Eq + Hash,
    K: Clone + Eq + Hash + Ord,
{
    let handler = JoinMapLaneUpdate::new(
        projection,
        link_key,
        MapMessage::Update { key, value },
        true,
    );
    let id = projection(agent).id();
    run_handler_mod(agent, handler, Some(Modification::of(id)));
}

fn remove_join_map<L, K, V>(
    agent: &TestAgent,
    link_key: L,
    key: K,
    projection: fn(&TestAgent) -> &JoinMapLane<L, K, V>,
) where
    L: Clone + Eq + Hash,
    K: Clone + Eq + Hash + Ord,
{
    let handler = JoinMapLaneUpdate::new(projection, link_key, MapMessage::Remove { key }, true);
    let id = projection(agent).id();
    run_handler_mod(agent, handler, Some(Modification::of(id)));
}

fn remove_join_value<K, V>(
    agent: &TestAgent,
    key: K,
    projection: fn(&TestAgent) -> &JoinValueLane<K, V>,
) where
    K: Clone + Eq + Hash + Send + 'static,
    V: Form + Send + 'static,
    V::Rec: Send,
{
    let addr = Address::text(None, "/node", "lane");
    let handler = AfterClosed::new(projection, &addr, key, LinkClosedResponse::Delete);
    let id = projection(agent).id();
    run_handler_mod(agent, handler, Some(Modification::of(id)));
}

#[test]
fn register_join_value_lifecycle() {
    #[derive(Default, Clone)]
    struct TestLifecycle;

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[join_value_lifecycle(join_value)]
        fn register_lifecycle(
            &self,
            context: JoinValueContext<TestAgent, i32, Text>,
        ) -> impl JoinValueLaneLifecycle<i32, Text, TestAgent> + 'static {
            context.builder().done()
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle;

    let lifecycle = template.into_lifecycle();

    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();
    let mut action_context = dummy_context(&mut join_lane_init, &mut command_buffer);
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    lifecycle.initialize(&mut action_context, meta, &agent);

    let lane_id = agent.join_value.id();

    assert_eq!(join_lane_init.len(), 1);
    assert!(join_lane_init.contains_key(&lane_id));
    assert!(command_buffer.is_empty());
}

#[test]
fn register_join_map_lifecycle() {
    #[derive(Default, Clone)]
    struct TestLifecycle;

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[join_map_lifecycle(join_map)]
        fn register_lifecycle(
            &self,
            context: JoinMapContext<TestAgent, Text, i32, Text>,
        ) -> impl JoinMapLaneLifecycle<Text, i32, TestAgent> + 'static {
            context.builder().done()
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle;

    let lifecycle = template.into_lifecycle();

    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();
    let mut action_context = dummy_context(&mut join_lane_init, &mut command_buffer);
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    lifecycle.initialize(&mut action_context, meta, &agent);

    let lane_id = agent.join_map.id();

    assert_eq!(join_lane_init.len(), 1);
    assert!(join_lane_init.contains_key(&lane_id));
    assert!(command_buffer.is_empty());
}

#[derive(AgentLaneModel)]
#[agent(root(crate))]
struct TwoJoinValueAgent {
    join_value1: JoinValueLane<i32, Text>,
    join_value2: JoinValueLane<i32, i64>,
}

#[test]
fn register_two_join_value_lifecycles() {
    #[derive(Default, Clone)]
    struct TestLifecycle;

    #[lifecycle(TwoJoinValueAgent, agent_root(crate))]
    impl TestLifecycle {
        #[join_value_lifecycle(join_value1)]
        fn register_lifecycle1(
            &self,
            context: JoinValueContext<TwoJoinValueAgent, i32, Text>,
        ) -> impl JoinValueLaneLifecycle<i32, Text, TwoJoinValueAgent> + 'static {
            context.builder().done()
        }

        #[join_value_lifecycle(join_value2)]
        fn register_lifecycle2(
            &self,
            context: JoinValueContext<TwoJoinValueAgent, i32, i64>,
        ) -> impl JoinValueLaneLifecycle<i32, i64, TwoJoinValueAgent> + 'static {
            context.builder().done()
        }
    }

    let agent = TwoJoinValueAgent::default();
    let template = TestLifecycle;

    let lifecycle = template.into_lifecycle();

    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();
    let mut action_context = dummy_context(&mut join_lane_init, &mut command_buffer);
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    lifecycle.initialize(&mut action_context, meta, &agent);

    let lane_id1 = agent.join_value1.id();
    let lane_id2 = agent.join_value2.id();

    assert_eq!(join_lane_init.len(), 2);
    assert!(join_lane_init.contains_key(&lane_id1));
    assert!(join_lane_init.contains_key(&lane_id2));
    assert!(command_buffer.is_empty());
}

#[derive(AgentLaneModel)]
#[agent(root(crate))]
struct TwoJoinMapAgent {
    join_map1: JoinMapLane<Text, i32, Text>,
    join_map2: JoinMapLane<Text, i32, i64>,
}

#[test]
fn register_two_join_map_lifecycles() {
    #[derive(Default, Clone)]
    struct TestLifecycle;

    #[lifecycle(TwoJoinMapAgent, agent_root(crate))]
    impl TestLifecycle {
        #[join_map_lifecycle(join_map1)]
        fn register_lifecycle1(
            &self,
            context: JoinMapContext<TwoJoinMapAgent, Text, i32, Text>,
        ) -> impl JoinMapLaneLifecycle<Text, i32, TwoJoinMapAgent> + 'static {
            context.builder().done()
        }

        #[join_map_lifecycle(join_map2)]
        fn register_lifecycle2(
            &self,
            context: JoinMapContext<TwoJoinMapAgent, Text, i32, i64>,
        ) -> impl JoinMapLaneLifecycle<Text, i32, TwoJoinMapAgent> + 'static {
            context.builder().done()
        }
    }

    let agent = TwoJoinMapAgent::default();
    let template = TestLifecycle;

    let lifecycle = template.into_lifecycle();

    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();
    let mut action_context = dummy_context(&mut join_lane_init, &mut command_buffer);
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    lifecycle.initialize(&mut action_context, meta, &agent);

    let lane_id1 = agent.join_map1.id();
    let lane_id2 = agent.join_map2.id();

    assert_eq!(join_lane_init.len(), 2);
    assert!(join_lane_init.contains_key(&lane_id1));
    assert!(join_lane_init.contains_key(&lane_id2));
    assert!(command_buffer.is_empty());
}

#[test]
fn on_cue_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_cue(demand)]
        fn my_on_cue(
            &self,
            context: HandlerContext<TestAgent>,
        ) -> impl HandlerAction<TestAgent, Completion = i32> + '_ {
            context.effect(move || {
                self.0.push(Event::Cued);
                38
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();
    let lifecycle = template.clone().into_lifecycle();

    let cue_handler = Cue::new(|agent: &TestAgent| &agent.demand);

    run_handler_mod(
        &agent,
        cue_handler,
        Some(Modification::of(agent.demand.id())),
    );

    let handler = lifecycle
        .item_event(&agent, "demand")
        .expect("Expected handler for lane.");

    run_handler_mod(
        &agent,
        handler,
        Some(Modification::no_trigger(agent.demand.id())),
    );

    let events = template.0.take();

    assert_eq!(events, vec![Event::Cued]);
}

const SYNC_ID: Uuid = Uuid::from_u128(84747);

#[test]
fn keys_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[keys(demand_map)]
        fn my_keys(
            &self,
            context: HandlerContext<TestAgent>,
        ) -> impl HandlerAction<TestAgent, Completion = HashSet<i32>> + '_ {
            context.effect(move || {
                self.0.push(Event::Keys);
                [0, 1, 2].into_iter().collect::<HashSet<_>>()
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();
    let lifecycle = template.clone().into_lifecycle();

    let sync_handler = DemandMapLaneSync::new(|agent: &TestAgent| &agent.demand_map, SYNC_ID);

    run_handler_mod(
        &agent,
        sync_handler,
        Some(Modification::of(agent.demand_map.id())),
    );

    let handler = lifecycle
        .item_event(&agent, "demand_map")
        .expect("Expected handler for lane.");

    run_handler_mod(
        &agent,
        handler,
        Some(Modification::of(agent.demand_map.id())),
    );

    let events = template.0.take();

    assert_eq!(events, vec![Event::Keys]);
}

#[test]
fn on_cue_key_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_cue_key(demand_map)]
        fn my_cue_key(
            &self,
            context: HandlerContext<TestAgent>,
            key: i32,
        ) -> impl HandlerAction<TestAgent, Completion = Option<Text>> + '_ {
            context.effect(move || {
                let text = Text::from(key.to_string());
                self.0.push(Event::CueKey(key));
                Some(text)
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();
    let lifecycle = template.clone().into_lifecycle();

    let cue_key_handler = CueKey::new(|agent: &TestAgent| &agent.demand_map, 3);

    run_handler_mod(
        &agent,
        cue_key_handler,
        Some(Modification::of(agent.demand_map.id())),
    );

    let handler = lifecycle
        .item_event(&agent, "demand_map")
        .expect("Expected handler for lane.");

    run_handler_mod(
        &agent,
        handler,
        Some(Modification::no_trigger(agent.demand_map.id())),
    );

    let events = template.0.take();

    assert_eq!(events, vec![Event::CueKey(3)]);
}

#[test]
fn both_demand_map_handlers() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[keys(demand_map)]
        fn my_keys(
            &self,
            context: HandlerContext<TestAgent>,
        ) -> impl HandlerAction<TestAgent, Completion = HashSet<i32>> + '_ {
            context.effect(move || {
                self.0.push(Event::Keys);
                [0, 1, 2].into_iter().collect::<HashSet<_>>()
            })
        }

        #[on_cue_key(demand_map)]
        fn my_cue_key(
            &self,
            context: HandlerContext<TestAgent>,
            key: i32,
        ) -> impl HandlerAction<TestAgent, Completion = Option<Text>> + '_ {
            context.effect(move || {
                let text = Text::from(key.to_string());
                self.0.push(Event::CueKey(key));
                Some(text)
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();
    let lifecycle = template.clone().into_lifecycle();

    let sync_handler = DemandMapLaneSync::new(|agent: &TestAgent| &agent.demand_map, SYNC_ID);
    let cue_key_handler = CueKey::new(|agent: &TestAgent| &agent.demand_map, 3);
    let mut buffer = BytesMut::new();

    run_handler_mod(
        &agent,
        cue_key_handler,
        Some(Modification::of(agent.demand_map.id())),
    );

    let handler = lifecycle
        .item_event(&agent, "demand_map")
        .expect("Expected handler for lane.");

    run_handler_mod(
        &agent,
        handler,
        Some(Modification::no_trigger(agent.demand_map.id())),
    );

    assert_eq!(
        agent.demand_map.write_to_buffer(&mut buffer),
        WriteResult::Done
    );

    run_handler_mod(
        &agent,
        sync_handler,
        Some(Modification::of(agent.demand_map.id())),
    );

    let handler = lifecycle
        .item_event(&agent, "demand_map")
        .expect("Expected handler for lane.");

    run_handler_mod(
        &agent,
        handler,
        Some(Modification::of(agent.demand_map.id())),
    );

    let events = template.0.take();

    assert_eq!(events, vec![Event::CueKey(3), Event::Keys]);
}

const HTTP_URI: &str = "http://example/node?lane=http";
const RECON: &str = "application/x-recon";

fn get_request() -> (HttpLaneRequest, HttpResponseReceiver) {
    let headers = vec![Header::new(StandardHeaderName::Accept, RECON)];
    HttpLaneRequest::new(HttpRequest {
        method: Method::GET,
        version: Version::HTTP_1_1,
        uri: HTTP_URI.parse().unwrap(),
        headers,
        payload: Default::default(),
    })
}

fn post_request(n: i32) -> (HttpLaneRequest, HttpResponseReceiver) {
    let headers = vec![Header::new(StandardHeaderName::ContentType, RECON)];
    let body = format!("{}", print_recon_compact(&n));
    HttpLaneRequest::new(HttpRequest {
        method: Method::POST,
        version: Version::HTTP_1_1,
        uri: HTTP_URI.parse().unwrap(),
        headers,
        payload: body.into(),
    })
}

fn put_request(n: i32) -> (HttpLaneRequest, HttpResponseReceiver) {
    let headers = vec![Header::new(StandardHeaderName::ContentType, RECON)];
    let body = format!("{}", print_recon_compact(&n));
    HttpLaneRequest::new(HttpRequest {
        method: Method::PUT,
        version: Version::HTTP_1_1,
        uri: HTTP_URI.parse().unwrap(),
        headers,
        payload: body.into(),
    })
}

fn delete_request() -> (HttpLaneRequest, HttpResponseReceiver) {
    let headers = vec![];
    HttpLaneRequest::new(HttpRequest {
        method: Method::DELETE,
        version: Version::HTTP_1_1,
        uri: HTTP_URI.parse().unwrap(),
        headers,
        payload: Default::default(),
    })
}

#[test]
fn on_get_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_get(http)]
        fn my_on_get(
            &self,
            context: HandlerContext<TestAgent>,
            http_context: HttpRequestContext,
        ) -> impl HandlerAction<TestAgent, Completion = Response<i32>> + '_ {
            context.effect(move || {
                self.0.push(Event::HttpGet(http_context));
                Response::from(27)
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();
    let lifecycle = template.clone().into_lifecycle();

    let (request, mut rx) = get_request();
    let get_handler = HttpLaneAccept::new(|agent: &TestAgent| &agent.http, request);

    run_handler_mod(
        &agent,
        get_handler,
        Some(Modification::trigger_only(agent.http.id())),
    );

    let handler = lifecycle
        .item_event(&agent, "http")
        .expect("Expected handler for lane.");

    run_handler_mod(&agent, handler, None);

    let HttpResponse {
        status_code,
        payload,
        ..
    } = rx.try_recv().expect("No response provided.");
    assert_eq!(status_code, StatusCode::OK);
    assert_eq!(payload.as_ref(), b"27");

    let events = template.0.take();

    match events.as_slice() {
        [Event::HttpGet(context)] => {
            let expected_headers = vec![Header::new(StandardHeaderName::Accept, RECON)];
            assert_eq!(context.uri().to_string(), HTTP_URI);
            assert_eq!(context.headers(), &expected_headers);
        }
        ow => panic!("Events not as expected: {:?}", ow),
    }
}

#[test]
fn on_post_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_post(http)]
        fn my_on_post(
            &self,
            context: HandlerContext<TestAgent>,
            http_context: HttpRequestContext,
            value: i32,
        ) -> impl HandlerAction<TestAgent, Completion = UnitResponse> + '_ {
            context.effect(move || {
                self.0.push(Event::HttpPost(http_context, value));
                UnitResponse::default()
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();
    let lifecycle = template.clone().into_lifecycle();

    let (request, mut rx) = post_request(987);
    let post_handler = HttpLaneAccept::new(|agent: &TestAgent| &agent.http, request);

    run_handler_mod(
        &agent,
        post_handler,
        Some(Modification::trigger_only(agent.http.id())),
    );

    let handler = lifecycle
        .item_event(&agent, "http")
        .expect("Expected handler for lane.");

    run_handler_mod(&agent, handler, None);

    let HttpResponse {
        status_code,
        payload,
        ..
    } = rx.try_recv().expect("No response provided.");
    assert_eq!(status_code, StatusCode::OK);
    assert!(payload.is_empty());

    let events = template.0.take();

    match events.as_slice() {
        [Event::HttpPost(context, value)] => {
            assert_eq!(*value, 987);
            let expected_headers = vec![Header::new(StandardHeaderName::ContentType, RECON)];
            assert_eq!(context.uri().to_string(), HTTP_URI);
            assert_eq!(context.headers(), &expected_headers);
        }
        ow => panic!("Events not as expected: {:?}", ow),
    }
}

#[test]
fn on_put_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_put(http)]
        fn my_on_put(
            &self,
            context: HandlerContext<TestAgent>,
            http_context: HttpRequestContext,
            value: i32,
        ) -> impl HandlerAction<TestAgent, Completion = UnitResponse> + '_ {
            context.effect(move || {
                self.0.push(Event::HttpPut(http_context, value));
                UnitResponse::default()
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();
    let lifecycle = template.clone().into_lifecycle();

    let (request, mut rx) = put_request(546);
    let put_handler = HttpLaneAccept::new(|agent: &TestAgent| &agent.http, request);

    run_handler_mod(
        &agent,
        put_handler,
        Some(Modification::trigger_only(agent.http.id())),
    );

    let handler = lifecycle
        .item_event(&agent, "http")
        .expect("Expected handler for lane.");

    run_handler_mod(&agent, handler, None);

    let HttpResponse {
        status_code,
        payload,
        ..
    } = rx.try_recv().expect("No response provided.");
    assert_eq!(status_code, StatusCode::OK);
    assert!(payload.is_empty());

    let events = template.0.take();

    match events.as_slice() {
        [Event::HttpPut(context, value)] => {
            assert_eq!(*value, 546);
            let expected_headers = vec![Header::new(StandardHeaderName::ContentType, RECON)];
            assert_eq!(context.uri().to_string(), HTTP_URI);
            assert_eq!(context.headers(), &expected_headers);
        }
        ow => panic!("Events not as expected: {:?}", ow),
    }
}

#[test]
fn on_delete_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_delete(http)]
        fn my_on_delete(
            &self,
            context: HandlerContext<TestAgent>,
            http_context: HttpRequestContext,
        ) -> impl HandlerAction<TestAgent, Completion = UnitResponse> + '_ {
            context.effect(move || {
                self.0.push(Event::HttpDelete(http_context));
                UnitResponse::default()
            })
        }
    }

    let agent = TestAgent::default();
    let template = TestLifecycle::default();
    let lifecycle = template.clone().into_lifecycle();

    let (request, mut rx) = delete_request();
    let put_handler = HttpLaneAccept::new(|agent: &TestAgent| &agent.http, request);

    run_handler_mod(
        &agent,
        put_handler,
        Some(Modification::trigger_only(agent.http.id())),
    );

    let handler = lifecycle
        .item_event(&agent, "http")
        .expect("Expected handler for lane.");

    run_handler_mod(&agent, handler, None);

    let HttpResponse {
        status_code,
        payload,
        ..
    } = rx.try_recv().expect("No response provided.");
    assert_eq!(status_code, StatusCode::OK);
    assert!(payload.is_empty());

    let events = template.0.take();

    match events.as_slice() {
        [Event::HttpDelete(context)] => {
            assert_eq!(context.uri().to_string(), HTTP_URI);
            assert!(context.headers().is_empty());
        }
        ow => panic!("Events not as expected: {:?}", ow),
    }
}

#[derive(AgentLaneModel)]
#[agent(root(crate))]
struct OrderedMapsAgent {
    lane: MapLane<i32, u64, BTreeMap<i32, u64>>,
    store: MapStore<i32, u64, BTreeMap<i32, u64>>,
}

const OV1: u64 = 8374749;
const OV2: u64 = 128374747;

impl OrderedMapsAgent {
    fn with_data() -> Self {
        let content = init_ordered();
        OrderedMapsAgent {
            lane: MapLane::new(0, content.clone()),
            store: MapStore::new(1, content),
        }
    }
}

fn init_ordered() -> BTreeMap<i32, u64> {
    let mut content = BTreeMap::new();
    content.insert(K1, OV1);
    content.insert(K2, OV2);
    content
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OrderedMapEvent {
    Clear(BTreeMap<i32, u64>),
    Remove(BTreeMap<i32, u64>, i32, u64),
    Update(BTreeMap<i32, u64>, i32, Option<u64>),
}

#[derive(Default, Debug, Clone)]
struct OrderedMapLifecycle {
    events: Arc<Mutex<Vec<OrderedMapEvent>>>,
}

impl OrderedMapLifecycle {
    fn push(&self, event: OrderedMapEvent) {
        self.events.lock().push(event)
    }

    fn take(&self) -> Vec<OrderedMapEvent> {
        let mut guard = self.events.lock();
        std::mem::take(&mut *guard)
    }
}

#[test]
fn on_update_handler_lane_ordered() {
    #[derive(Default, Clone)]
    struct TestLifecycle(OrderedMapLifecycle);

    #[lifecycle(OrderedMapsAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_update(lane)]
        fn my_on_update(
            &self,
            context: HandlerContext<OrderedMapsAgent>,
            map: &BTreeMap<i32, u64>,
            key: i32,
            prev: Option<u64>,
            _new_value: &u64,
        ) -> impl EventHandler<OrderedMapsAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0.push(OrderedMapEvent::Update(map_state, key, prev));
            })
        }
    }

    let agent = OrderedMapsAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    let v = 657383;
    agent.lane.update(K2, v);
    let handler = lifecycle
        .item_event(&agent, "lane")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = BTreeMap::new();
    expected_map.insert(K2, v);
    assert_eq!(
        events,
        vec![OrderedMapEvent::Update(expected_map, K2, None)]
    );
}

#[test]
fn on_update_handler_store_ordered() {
    #[derive(Default, Clone)]
    struct TestLifecycle(OrderedMapLifecycle);

    #[lifecycle(OrderedMapsAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_update(store)]
        fn my_on_update(
            &self,
            context: HandlerContext<OrderedMapsAgent>,
            map: &BTreeMap<i32, u64>,
            key: i32,
            prev: Option<u64>,
            _new_value: &u64,
        ) -> impl EventHandler<OrderedMapsAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0.push(OrderedMapEvent::Update(map_state, key, prev));
            })
        }
    }

    let agent = OrderedMapsAgent::default();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    let v = 93747;
    agent.store.update(K2, v);
    let handler = lifecycle
        .item_event(&agent, "store")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = BTreeMap::new();
    expected_map.insert(K2, v);
    assert_eq!(
        events,
        vec![OrderedMapEvent::Update(expected_map, K2, None)]
    );
}

#[test]
fn on_remove_handler_lane_ordered() {
    #[derive(Default, Clone)]
    struct TestLifecycle(OrderedMapLifecycle);

    #[lifecycle(OrderedMapsAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_remove(lane)]
        fn my_on_remove(
            &self,
            context: HandlerContext<OrderedMapsAgent>,
            map: &BTreeMap<i32, u64>,
            key: i32,
            prev: u64,
        ) -> impl EventHandler<OrderedMapsAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0.push(OrderedMapEvent::Remove(map_state, key, prev));
            })
        }
    }

    let agent = OrderedMapsAgent::with_data();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.lane.remove(&K2);
    let handler = lifecycle
        .item_event(&agent, "lane")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = BTreeMap::new();
    expected_map.insert(K1, OV1);
    assert_eq!(events, vec![OrderedMapEvent::Remove(expected_map, K2, OV2)]);
}

#[test]
fn on_remove_handler_store_ordered() {
    #[derive(Default, Clone)]
    struct TestLifecycle(OrderedMapLifecycle);

    #[lifecycle(OrderedMapsAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_remove(store)]
        fn my_on_remove(
            &self,
            context: HandlerContext<OrderedMapsAgent>,
            map: &BTreeMap<i32, u64>,
            key: i32,
            prev: u64,
        ) -> impl EventHandler<OrderedMapsAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0.push(OrderedMapEvent::Remove(map_state, key, prev));
            })
        }
    }

    let agent = OrderedMapsAgent::with_data();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.store.remove(&K2);
    let handler = lifecycle
        .item_event(&agent, "store")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let mut expected_map = BTreeMap::new();
    expected_map.insert(K1, OV1);
    expected_map.insert(K1, OV1);
    assert_eq!(events, vec![OrderedMapEvent::Remove(expected_map, K2, OV2)]);
}

#[test]
fn on_clear_handler_lane_ordered() {
    #[derive(Default, Clone)]
    struct TestLifecycle(OrderedMapLifecycle);

    #[lifecycle(OrderedMapsAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_clear(lane)]
        fn my_on_clear(
            &self,
            context: HandlerContext<OrderedMapsAgent>,
            map: BTreeMap<i32, u64>,
        ) -> impl EventHandler<OrderedMapsAgent> + '_ {
            context.effect(move || {
                self.0.push(OrderedMapEvent::Clear(map));
            })
        }
    }

    let agent = OrderedMapsAgent::with_data();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.lane.clear();
    let handler = lifecycle
        .item_event(&agent, "lane")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let expected_map = init_ordered();
    assert_eq!(events, vec![OrderedMapEvent::Clear(expected_map)]);
}

#[test]
fn on_clear_handler_store_ordered() {
    #[derive(Default, Clone)]
    struct TestLifecycle(OrderedMapLifecycle);

    #[lifecycle(OrderedMapsAgent, agent_root(crate))]
    impl TestLifecycle {
        #[on_clear(store)]
        fn my_on_clear(
            &self,
            context: HandlerContext<OrderedMapsAgent>,
            map: BTreeMap<i32, u64>,
        ) -> impl EventHandler<OrderedMapsAgent> + '_ {
            context.effect(move || {
                self.0.push(OrderedMapEvent::Clear(map));
            })
        }
    }

    let agent = OrderedMapsAgent::with_data();
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.store.clear();
    let handler = lifecycle
        .item_event(&agent, "store")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    let expected_map = init_ordered();
    assert_eq!(events, vec![OrderedMapEvent::Clear(expected_map)]);
}
