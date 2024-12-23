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

use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use swimos_api::agent::AgentConfig;
use swimos_model::Text;
use swimos_utilities::routing::RouteUri;

use crate::{
    agent_lifecycle::item_event::{tests::run_handler, HLeaf, ItemEvent},
    event_handler::{ActionContext, HandlerAction, StepResult},
    lanes::map::{
        lifecycle::{on_clear::OnClear, on_remove::OnRemove, on_update::OnUpdate},
        MapLane, MapLaneEvent,
    },
    meta::AgentMetadata,
};

use super::MapLikeBranch;

type MapLeaf<Context, K, V, M, LC> = MapBranch<Context, K, V, M, LC, HLeaf, HLeaf>;
type MapBranch<Context, K, V, M, LC, L, R> =
    MapLikeBranch<Context, K, V, M, MapLane<K, V, M>, LC, L, R>;

struct TestAgent {
    first: MapLane<i32, i32>,
    second: MapLane<i32, Text>,
    third: MapLane<i32, bool>,
}

const K1: i32 = 5;
const V1: i32 = 67;
const V2: &str = "hello";
const V3: bool = false;

impl TestAgent {
    fn with_content() -> Self {
        let mut map1 = HashMap::new();
        let mut map2 = HashMap::new();
        let mut map3 = HashMap::new();
        map1.insert(K1, V1);
        map2.insert(K1, Text::new(V2));
        map3.insert(K1, V3);
        Self {
            first: MapLane::new(LANE_ID1, map1),
            second: MapLane::new(LANE_ID2, map2),
            third: MapLane::new(LANE_ID3, map3),
        }
    }
}

const LANE_ID1: u64 = 0;
const LANE_ID2: u64 = 1;
const LANE_ID3: u64 = 2;

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            first: MapLane::new(LANE_ID1, Default::default()),
            second: MapLane::new(LANE_ID2, Default::default()),
            third: MapLane::new(LANE_ID3, Default::default()),
        }
    }
}

impl TestAgent {
    const FIRST: fn(&TestAgent) -> &MapLane<i32, i32> = |agent| &agent.first;
    const SECOND: fn(&TestAgent) -> &MapLane<i32, Text> = |agent| &agent.second;
    const THIRD: fn(&TestAgent) -> &MapLane<i32, bool> = |agent| &agent.third;
}

const FIRST_NAME: &str = "first";
const SECOND_NAME: &str = "second";
const THIRD_NAME: &str = "third";

#[derive(Default, Debug, Clone)]
struct LifecycleState<K, V> {
    event: Option<Inner<K, V>>,
}

#[derive(Debug, Clone)]
struct Inner<K, V> {
    map: HashMap<K, V>,
    event: MapLaneEvent<K, V>,
}

impl<K, V> Inner<K, V> {
    fn new(map: HashMap<K, V>, event: MapLaneEvent<K, V>) -> Self {
        Inner { map, event }
    }
}

struct OnUpdateHandler<K, V> {
    map: HashMap<K, V>,
    key: K,
    previous: Option<V>,
    state: Arc<Mutex<LifecycleState<K, V>>>,
    done: bool,
}

impl<K: Clone, V: Clone> HandlerAction<TestAgent> for OnUpdateHandler<K, V> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let OnUpdateHandler {
            map,
            key,
            previous,
            state,
            done,
            ..
        } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            let mut guard = state.lock();
            guard.event = Some(Inner::new(
                map.clone(),
                MapLaneEvent::Update(key.clone(), previous.clone()),
            ));
            StepResult::done(())
        }
    }
}

struct OnRemoveHandler<K, V> {
    map: HashMap<K, V>,
    key: K,
    previous: V,
    state: Arc<Mutex<LifecycleState<K, V>>>,
    done: bool,
}

impl<K: Clone, V: Clone> HandlerAction<TestAgent> for OnRemoveHandler<K, V> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let OnRemoveHandler {
            map,
            key,
            previous,
            state,
            done,
        } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            let mut guard = state.lock();
            guard.event = Some(Inner::new(
                map.clone(),
                MapLaneEvent::Remove(key.clone(), previous.clone()),
            ));
            StepResult::done(())
        }
    }
}

struct OnClearHandler<K, V> {
    previous: HashMap<K, V>,
    state: Arc<Mutex<LifecycleState<K, V>>>,
    done: bool,
}

impl<K: Clone, V: Clone> HandlerAction<TestAgent> for OnClearHandler<K, V> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let OnClearHandler {
            previous,
            state,
            done,
        } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            let mut guard = state.lock();
            guard.event = Some(Inner::new(
                HashMap::new(),
                MapLaneEvent::Clear(previous.clone()),
            ));
            StepResult::done(())
        }
    }
}

#[derive(Default, Debug, Clone)]
struct FakeLifecycle<K, V> {
    state: Arc<Mutex<LifecycleState<K, V>>>,
}

impl<K, V> FakeLifecycle<K, V> {
    fn on_update_handler(
        &self,
        map: HashMap<K, V>,
        key: K,
        previous: Option<V>,
    ) -> OnUpdateHandler<K, V> {
        OnUpdateHandler {
            map,
            key,
            previous,
            state: self.state.clone(),
            done: false,
        }
    }

    fn on_remove_handler(&self, map: HashMap<K, V>, key: K, previous: V) -> OnRemoveHandler<K, V> {
        OnRemoveHandler {
            map,
            key,
            previous,
            state: self.state.clone(),
            done: false,
        }
    }

    fn on_clear_handler(&self, previous: HashMap<K, V>) -> OnClearHandler<K, V> {
        OnClearHandler {
            previous,
            state: self.state.clone(),
            done: false,
        }
    }
}

impl<K, V> OnUpdate<K, V, HashMap<K, V>, TestAgent> for FakeLifecycle<K, V>
where
    K: Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    type OnUpdateHandler<'a> = OnUpdateHandler<K, V>
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        self.on_update_handler(map.clone(), key, prev_value)
    }
}

impl<K, V> OnRemove<K, V, HashMap<K, V>, TestAgent> for FakeLifecycle<K, V>
where
    K: Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    type OnRemoveHandler<'a> = OnRemoveHandler<K, V>
    where
        Self: 'a;

    fn on_remove<'a>(
        &'a self,
        map: &HashMap<K, V>,
        key: K,
        prev_value: V,
    ) -> Self::OnRemoveHandler<'a> {
        self.on_remove_handler(map.clone(), key, prev_value)
    }
}

impl<K, V> OnClear<HashMap<K, V>, TestAgent> for FakeLifecycle<K, V>
where
    K: Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    type OnClearHandler<'a> = OnClearHandler<K, V>
    where
        Self: 'a;

    fn on_clear(&self, before: HashMap<K, V>) -> Self::OnClearHandler<'_> {
        self.on_clear_handler(before)
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

#[test]
fn map_lane_leaf() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::default();

    agent.first.update(56, -1);

    let lifecycle = FakeLifecycle::<i32, i32>::default();
    let leaf = MapLeaf::leaf(FIRST_NAME, TestAgent::FIRST, lifecycle.clone());

    assert!(leaf.item_event(&agent, "other").is_none());

    if let Some(handler) = leaf.item_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);
        let guard = lifecycle.state.lock();
        let LifecycleState { event } = guard.clone();

        let Inner { map, event } = event.expect("No event.");
        let mut expected = HashMap::new();
        expected.insert(56, -1);
        assert_eq!(map, expected);
        assert_eq!(event, MapLaneEvent::Update(56, None));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
fn map_lane_left_branch() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::with_content();

    let first_lifecycle = FakeLifecycle::<i32, i32>::default();
    let second_lifecycle = FakeLifecycle::<i32, Text>::default();
    let leaf = MapLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());

    let branch = MapBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        leaf,
        HLeaf,
    );

    assert!(branch.item_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.item_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.item_event(&agent, "u").is_none()); //After second lane.

    agent.first.remove(&K1);
    agent.second.clear();

    if let Some(handler) = branch.item_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);

        let guard = first_lifecycle.state.lock();
        let LifecycleState { event: inner } = guard.clone();

        let Inner { map, event } = inner.expect("No event.");

        assert!(map.is_empty());
        assert_eq!(event, MapLaneEvent::Remove(K1, V1));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);

        let guard = second_lifecycle.state.lock();
        let LifecycleState { event: inner } = guard.clone();

        let Inner { map, event } = inner.expect("No event.");

        assert!(map.is_empty());
        let mut before = HashMap::new();
        before.insert(K1, Text::new(V2));
        assert_eq!(event, MapLaneEvent::Clear(before));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
fn map_lane_right_branch() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::with_content();

    let first_lifecycle = FakeLifecycle::<i32, i32>::default();
    let second_lifecycle = FakeLifecycle::<i32, Text>::default();
    let leaf = MapLeaf::leaf(SECOND_NAME, TestAgent::SECOND, second_lifecycle.clone());

    let branch = MapBranch::new(
        FIRST_NAME,
        TestAgent::FIRST,
        first_lifecycle.clone(),
        HLeaf,
        leaf,
    );

    assert!(branch.item_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.item_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.item_event(&agent, "u").is_none()); //After second lane.

    agent.first.remove(&K1);
    agent.second.clear();

    if let Some(handler) = branch.item_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);

        let guard = first_lifecycle.state.lock();
        let LifecycleState { event: inner } = guard.clone();

        let Inner { map, event } = inner.expect("No event.");

        assert!(map.is_empty());
        assert_eq!(event, MapLaneEvent::Remove(K1, V1));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);

        let guard = second_lifecycle.state.lock();
        let LifecycleState { event: inner } = guard.clone();

        let Inner { map, event } = inner.expect("No event.");

        assert!(map.is_empty());
        let mut before = HashMap::new();
        before.insert(K1, Text::new(V2));
        assert_eq!(event, MapLaneEvent::Clear(before));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
fn map_lane_two_branches() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::with_content();

    let first_lifecycle = FakeLifecycle::<i32, i32>::default();
    let second_lifecycle = FakeLifecycle::<i32, Text>::default();
    let third_lifecycle = FakeLifecycle::<i32, bool>::default();
    let leaf_left = MapLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());
    let leaf_right = MapLeaf::leaf(THIRD_NAME, TestAgent::THIRD, third_lifecycle.clone());
    let branch = MapBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        leaf_left,
        leaf_right,
    );

    assert!(branch.item_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.item_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.item_event(&agent, "sf").is_none()); //Between second and third lanes.
    assert!(branch.item_event(&agent, "u").is_none()); //After third lane.

    agent.first.remove(&K1);
    agent.second.clear();
    agent.third.update(67, true);

    if let Some(handler) = branch.item_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);

        let guard = first_lifecycle.state.lock();
        let LifecycleState { event: inner } = guard.clone();

        let Inner { map, event } = inner.expect("No event.");

        assert!(map.is_empty());
        assert_eq!(event, MapLaneEvent::Remove(K1, V1));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);

        let guard = second_lifecycle.state.lock();
        let LifecycleState { event: inner } = guard.clone();

        let Inner { map, event } = inner.expect("No event.");

        assert!(map.is_empty());
        let mut before = HashMap::new();
        before.insert(K1, Text::new(V2));
        assert_eq!(event, MapLaneEvent::Clear(before));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&agent, THIRD_NAME) {
        run_handler(meta, &agent, handler);

        let guard = third_lifecycle.state.lock();
        let LifecycleState { event: inner } = guard.clone();

        let Inner { map, event } = inner.expect("No event.");

        let mut expected = HashMap::new();
        expected.insert(K1, V3);
        expected.insert(67, true);

        assert_eq!(map, expected);
        assert_eq!(event, MapLaneEvent::Update(67, None));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
#[should_panic]
fn fail_out_of_order_labels_right() {
    let first_lifecycle = FakeLifecycle::<i32, i32>::default();
    let second_lifecycle = FakeLifecycle::<i32, Text>::default();
    let leaf = MapLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle);

    MapBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle,
        HLeaf,
        leaf,
    );
}

#[test]
#[should_panic]
fn fail_out_of_order_labels_left() {
    let first_lifecycle = FakeLifecycle::<i32, i32>::default();
    let second_lifecycle = FakeLifecycle::<i32, Text>::default();
    let leaf = MapLeaf::leaf(SECOND_NAME, TestAgent::SECOND, second_lifecycle);

    MapBranch::new(FIRST_NAME, TestAgent::FIRST, first_lifecycle, leaf, HLeaf);
}

#[test]
#[should_panic]
fn fail_equal_labels() {
    let first_lifecycle = FakeLifecycle::<i32, i32>::default();
    let leaf = MapLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());

    MapBranch::new(FIRST_NAME, TestAgent::FIRST, first_lifecycle, HLeaf, leaf);
}
