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

use std::hash::Hash;
use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use swim::agent::lifecycle;
use swim::agent::{
    agent_lifecycle::{
        item_event::ItemEvent, on_start::OnStart, on_stop::OnStop, utility::HandlerContext,
    },
    event_handler::{EventHandler, StepResult},
    lanes::{CommandLane, MapLane, ValueLane},
    AgentLaneModel,
};
use swim_agent::agent_lifecycle::on_init::OnInit;
use swim_agent::agent_lifecycle::utility::JoinValueContext;
use swim_agent::agent_model::downlink::handlers::BoxDownlinkChannel;
use swim_agent::event_handler::{
    BoxJoinValueInit, HandlerFuture, Modification, Spawner, WriteStream,
};
use swim_agent::item::{AgentItem, MapItem};
use swim_agent::lanes::join_value::lifecycle::JoinValueLaneLifecycle;
use swim_agent::lanes::join_value::{AfterClosed, JoinValueLaneUpdate, LinkClosedResponse};
use swim_agent::lanes::JoinValueLane;
use swim_agent::meta::AgentMetadata;
use swim_agent::stores::{MapStore, ValueStore};
use swim_api::agent::AgentConfig;
use swim_api::downlink::DownlinkKind;
use swim_api::error::{DownlinkRuntimeError, OpenStoreError};
use swim_api::meta::lane::LaneKind;
use swim_api::store::StoreKind;
use swim_model::Text;
use swim_utilities::routing::route_uri::RouteUri;

use futures::future::BoxFuture;
use swim_api::{
    agent::{AgentContext, LaneConfig},
    error::AgentRuntimeError,
};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};

use swim_agent::event_handler::ActionContext;

struct NoSpawn;
pub struct DummyAgentContext;

const NO_SPAWN: NoSpawn = NoSpawn;
const NO_AGENT: DummyAgentContext = DummyAgentContext;
pub fn no_downlink<Context>(
    _dl: BoxDownlinkChannel<Context>,
    _write_stream: WriteStream,
) -> Result<(), DownlinkRuntimeError> {
    panic!("Launching downlinks no supported.");
}

pub fn dummy_context<'a, Context>(
    join_value_init: &'a mut HashMap<u64, BoxJoinValueInit<'static, Context>>,
) -> ActionContext<'a, Context> {
    ActionContext::new(&NO_SPAWN, &NO_AGENT, &no_downlink, join_value_init)
}

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }
}

impl AgentContext for DummyAgentContext {
    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: LaneKind,
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
}

#[derive(AgentLaneModel)]
#[agent_root(::swim_agent)]
struct TestAgent {
    value: ValueLane<i32>,
    value2: ValueLane<i32>,
    command: CommandLane<i32>,
    map: MapLane<i32, Text>,
    value_store: ValueStore<i32>,
    map_store: MapStore<i32, Text>,
    join_value: JoinValueLane<i32, Text>,
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

fn make_meta(uri: &RouteUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
}

fn run_handler_mod<Agent, H: EventHandler<Agent>>(
    agent: &Agent,
    mut handler: H,
    modified: Option<u64>,
) {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let mut join_value_init = HashMap::new();
    loop {
        match handler.step(&mut dummy_context(&mut join_value_init), meta, agent) {
            StepResult::Continue { modified_item } => {
                assert_eq!(modified_item, modified.map(|id| Modification::of(id)));
            }
            StepResult::Fail(e) => {
                panic!("{}", e);
            }
            StepResult::Complete { modified_item, .. } => {
                assert_eq!(modified_item, modified.map(|id| Modification::of(id)));
                break;
            }
        }
    }
    assert!(join_value_init.is_empty());
}

fn run_handler<Agent, H: EventHandler<Agent>>(agent: &Agent, handler: H) {
    run_handler_mod(agent, handler, None)
}

#[test]
fn on_start_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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
#[agent_root(::swim_agent)]
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

    #[lifecycle(BorrowAgent, agent_root(::swim_agent))]
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

    #[lifecycle(BorrowAgent, agent_root(::swim_agent))]
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

    #[lifecycle(BorrowAgent, agent_root(::swim_agent))]
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

    #[lifecycle(BorrowAgent, agent_root(::swim_agent))]
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

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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
fn on_remove_handler_join_value() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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
    run_handler_mod(agent, handler, Some(id));
}

fn remove_join_value<K, V>(
    agent: &TestAgent,
    key: K,
    projection: fn(&TestAgent) -> &JoinValueLane<K, V>,
) where
    K: Clone + Eq + Hash,
{
    let handler = AfterClosed::new(projection, key, LinkClosedResponse::Delete);
    let id = projection(agent).id();
    run_handler_mod(agent, handler, Some(id));
}

#[test]
fn register_join_value_lifecycle() {
    #[derive(Default, Clone)]
    struct TestLifecycle;

    #[lifecycle(TestAgent, agent_root(::swim_agent))]
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
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    let mut join_value_init = HashMap::new();
    let mut action_context = dummy_context(&mut join_value_init);
    let uri = make_uri();
    let meta = make_meta(&uri);

    lifecycle.initialize(&mut action_context, meta, &agent);

    let lane_id = agent.join_value.id();

    assert_eq!(join_value_init.len(), 1);
    assert!(join_value_init.contains_key(&lane_id));
}

#[derive(AgentLaneModel)]
#[agent_root(::swim_agent)]
struct TwoJoinValueAgent {
    join_value1: JoinValueLane<i32, Text>,
    join_value2: JoinValueLane<i32, i64>,
}

#[test]
fn register_two_join_value_lifecycles() {
    #[derive(Default, Clone)]
    struct TestLifecycle;

    #[lifecycle(TwoJoinValueAgent, agent_root(::swim_agent))]
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
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    let mut join_value_init = HashMap::new();
    let mut action_context = dummy_context(&mut join_value_init);
    let uri = make_uri();
    let meta = make_meta(&uri);

    lifecycle.initialize(&mut action_context, meta, &agent);

    let lane_id1 = agent.join_value1.id();
    let lane_id2 = agent.join_value1.id();

    assert_eq!(join_value_init.len(), 2);
    assert!(join_value_init.contains_key(&lane_id1));
    assert!(join_value_init.contains_key(&lane_id2));
}
