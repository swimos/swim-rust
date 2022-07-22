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

use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use swim_agent::lifecycle;
use swim_agent::{
    event_handler::{EventHandler, StepResult},
    lanes::{CommandLane, MapLane, ValueLane},
    lifecycle::{
        lane_event::LaneEvent, on_start::OnStart, on_stop::OnStop, utility::HandlerContext,
    },
    meta::AgentMetadata,
    AgentLaneModel,
};
use swim_api::agent::AgentConfig;
use swim_model::Text;
use swim_utilities::routing::uri::RelativeUri;

#[derive(AgentLaneModel)]
struct TestAgent {
    value: ValueLane<i32>,
    value2: ValueLane<i32>,
    command: CommandLane<i32>,
    map: MapLane<i32, Text>,
}

impl From<(i32, i32, HashMap<i32, Text>)> for TestAgent {
    fn from((value_init, value2_init, map_init): (i32, i32, HashMap<i32, Text>)) -> Self {
        TestAgent {
            value: ValueLane::new(0, value_init),
            value2: ValueLane::new(1, value2_init),
            command: CommandLane::new(2),
            map: MapLane::new(3, map_init),
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

const CONFIG: AgentConfig = AgentConfig {};
const NODE_URI: &str = "/node";

fn make_uri() -> RelativeUri {
    RelativeUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta(uri: &RelativeUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
}

fn run_handler<H: EventHandler<TestAgent>>(agent: &TestAgent, mut handler: H) {
    let uri = make_uri();
    let meta = make_meta(&uri);
    loop {
        match handler.step(meta, agent) {
            StepResult::Continue { modified_lane } => {
                assert!(modified_lane.is_none());
            }
            StepResult::Fail(e) => {
                panic!("{}", e);
            }
            StepResult::Complete { modified_lane, .. } => {
                assert!(modified_lane.is_none());
                break;
            }
        }
    }
}

#[test]
fn on_start_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent)]
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

    #[lifecycle(TestAgent)]
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

    #[lifecycle(TestAgent)]
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

    #[lifecycle(TestAgent)]
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
        .lane_event(&agent, "command")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![Event::Command(TEST_VALUE)]);
}

#[test]
fn on_event_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent)]
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
        .lane_event(&agent, "value")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let events = template.0.take();

    assert_eq!(events, vec![Event::Value(ValueEvent::Event(TEST_VALUE))]);
}

#[test]
fn on_set_handler() {
    #[derive(Default, Clone)]
    struct TestLifecycle(LifecycleInner);

    #[lifecycle(TestAgent)]
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
        .lane_event(&agent, "value")
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

    #[lifecycle(TestAgent)]
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
        .lane_event(&agent, "value")
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

    #[lifecycle(TestAgent)]
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
        .lane_event(&agent, "value")
        .expect("Expected handler for lane.");
    run_handler(&agent, handler);

    let handler = lifecycle
        .lane_event(&agent, "value2")
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

    #[lifecycle(TestAgent)]
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

    let agent = TestAgent::from((0, 0, init_map()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map.clear();
    let handler = lifecycle
        .lane_event(&agent, "map")
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

    #[lifecycle(TestAgent)]
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

    let agent = TestAgent::from((0, 0, init_map()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map.remove(&K1);
    let handler = lifecycle
        .lane_event(&agent, "map")
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

    #[lifecycle(TestAgent)]
    impl TestLifecycle {
        #[on_update(map)]
        fn my_on_update(
            &self,
            context: HandlerContext<TestAgent>,
            map: &HashMap<i32, Text>,
            key: i32,
            prev: Option<Text>,
        ) -> impl EventHandler<TestAgent> + '_ {
            let map_state = map.clone();
            context.effect(move || {
                self.0
                    .push(Event::Map(MapEvent::Update(map_state, key, prev)));
            })
        }
    }

    let agent = TestAgent::from((0, 0, init_map()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    agent.map.update(K2, Text::new("changed"));
    let handler = lifecycle
        .lane_event(&agent, "map")
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

    #[lifecycle(TestAgent)]
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

    let agent = TestAgent::from((0, 0, init_map()));
    let template = TestLifecycle::default();

    let lifecycle = template.clone().into_lifecycle();

    //The tests primarily verfies that he lifecycle compiles so we just test one representative event.
    agent.value.set(TEST_VALUE);
    let handler = lifecycle
        .lane_event(&agent, "value")
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
