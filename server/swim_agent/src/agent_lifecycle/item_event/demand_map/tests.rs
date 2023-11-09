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
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
};

use bytes::BytesMut;
use parking_lot::Mutex;
use swim_api::{
    agent::AgentConfig,
    protocol::{
        agent::{LaneResponse, LaneResponseDecoder},
        map::{MapOperation, MapOperationDecoder},
    },
};
use swim_form::Form;
use swim_model::Text;
use swim_utilities::routing::route_uri::RouteUri;
use tokio_util::codec::Decoder;
use uuid::Uuid;

use crate::{
    agent_lifecycle::item_event::{DemandMapBranch, DemandMapLeaf, HLeaf, ItemEvent},
    agent_model::WriteResult,
    event_handler::{ActionContext, HandlerAction, Modification, ModificationFlags, StepResult},
    lanes::{
        demand_map::lifecycle::{keys::Keys, on_cue_key::OnCueKey},
        DemandMapLane, LaneItem,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

struct TestAgent {
    first: DemandMapLane<i32, i32>,
    second: DemandMapLane<i32, Text>,
    third: DemandMapLane<i32, bool>,
}

const LANE_ID1: u64 = 0;
const LANE_ID2: u64 = 1;
const LANE_ID3: u64 = 2;

const SYNC_ID: Uuid = Uuid::from_u128(8484);

impl Default for TestAgent {
    fn default() -> Self {
        TestAgent {
            first: DemandMapLane::new(LANE_ID1),
            second: DemandMapLane::new(LANE_ID2),
            third: DemandMapLane::new(LANE_ID3),
        }
    }
}

impl TestAgent {
    const FIRST: fn(&TestAgent) -> &DemandMapLane<i32, i32> = |agent| &agent.first;
    const SECOND: fn(&TestAgent) -> &DemandMapLane<i32, Text> = |agent| &agent.second;
    const THIRD: fn(&TestAgent) -> &DemandMapLane<i32, bool> = |agent| &agent.third;
}

const FIRST_NAME: &str = "first";
const SECOND_NAME: &str = "second";
const THIRD_NAME: &str = "third";

#[derive(Debug, Clone, Copy)]
struct LifecycleState {
    keys_used: bool,
    cue_used: bool,
}

#[derive(Debug, Clone)]
struct FakeLifecycle<K, V> {
    content: HashMap<K, V>,
    state: Arc<Mutex<LifecycleState>>,
}

impl<K, V> FakeLifecycle<K, V>
where
    K: Eq + Hash + Clone,
{
    fn new(it: impl IntoIterator<Item = (K, V)>) -> Self {
        FakeLifecycle {
            content: it.into_iter().collect(),
            state: Arc::new(Mutex::new(LifecycleState {
                keys_used: false,
                cue_used: false,
            })),
        }
    }
}

struct OnCueKeyHandler<V> {
    value: Option<Option<V>>,
    state: Arc<Mutex<LifecycleState>>,
}

struct KeysHandler<K> {
    keys: Option<HashSet<K>>,
    state: Arc<Mutex<LifecycleState>>,
}

impl<K> HandlerAction<TestAgent> for KeysHandler<K> {
    type Completion = HashSet<K>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let KeysHandler { keys, state } = self;
        if let Some(keys) = keys.take() {
            let mut guard = state.lock();
            assert!(!guard.keys_used);
            guard.keys_used = true;
            StepResult::done(keys)
        } else {
            StepResult::after_done()
        }
    }
}

impl<V> HandlerAction<TestAgent> for OnCueKeyHandler<V> {
    type Completion = Option<V>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let OnCueKeyHandler { value, state, .. } = self;
        if let Some(v) = value.take() {
            let mut guard = state.lock();
            assert!(!guard.cue_used);
            guard.cue_used = true;
            StepResult::done(v)
        } else {
            StepResult::after_done()
        }
    }
}

impl<K, V> Keys<K, TestAgent> for FakeLifecycle<K, V>
where
    K: Clone + Eq + Hash + Send,
    V: Send,
{
    type KeysHandler<'a> = KeysHandler<K>
    where
        Self: 'a;

    fn keys(&self) -> Self::KeysHandler<'_> {
        KeysHandler {
            keys: Some(self.content.keys().cloned().collect()),
            state: self.state.clone(),
        }
    }
}

impl<K, V> OnCueKey<K, V, TestAgent> for FakeLifecycle<K, V>
where
    K: Clone + Eq + Hash + Send,
    V: Clone + Send,
{
    type OnCueKeyHandler<'a> = OnCueKeyHandler<V>
    where
        Self: 'a;

    fn on_cue_key(&self, key: K) -> Self::OnCueKeyHandler<'_> {
        let FakeLifecycle { content, state } = self;
        let value = content.get(&key).cloned();
        OnCueKeyHandler {
            value: Some(value),
            state: state.clone(),
        }
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
fn demand_map_lane_leaf() {
    let agent = TestAgent::default();

    agent.first.sync(SYNC_ID);

    let lifecycle = FakeLifecycle::new([(1, 2), (2, 4), (3, 6)]);
    let leaf = DemandMapLeaf::leaf(FIRST_NAME, TestAgent::FIRST, lifecycle.clone());

    assert!(leaf.item_event(&agent, "other").is_none());

    check_interaction(
        &leaf,
        &agent,
        FIRST_NAME,
        LANE_ID1,
        &lifecycle,
        TestAgent::FIRST,
        [(1, 2), (2, 4), (3, 6)],
    );
}

pub fn run_handler<H, Agent>(
    meta: AgentMetadata<'_>,
    agent: &Agent,
    mut event_handler: H,
) -> Option<Modification>
where
    H: HandlerAction<Agent, Completion = ()>,
{
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();
    let modified = loop {
        match event_handler.step(
            &mut dummy_context(&mut join_lane_init, &mut ad_hoc_buffer),
            meta,
            agent,
        ) {
            StepResult::Continue { modified_item } => {
                assert!(modified_item.is_none());
            }
            StepResult::Fail(err) => {
                panic!("Event handler failed: {}", err);
            }
            StepResult::Complete { modified_item, .. } => {
                break modified_item;
            }
        }
    };
    assert!(join_lane_init.is_empty());
    assert!(ad_hoc_buffer.is_empty());
    modified
}

fn check_interaction<H, K, V>(
    item_handler: &H,
    agent: &TestAgent,
    name: &str,
    lane_id: u64,
    lifecycle: &FakeLifecycle<K, V>,
    projection: fn(&TestAgent) -> &DemandMapLane<K, V>,
    expected: impl IntoIterator<Item = (K, V)>,
) where
    K: Form + Eq + Hash + Clone + Debug,
    V: Form + Eq + Debug + Clone,
    H: ItemEvent<TestAgent>,
{
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let sync_handler = item_handler
        .item_event(agent, name)
        .expect("Expected an event handler.");

    let modified = run_handler(meta, agent, sync_handler);

    assert_eq!(
        modified,
        Some(Modification {
            item_id: lane_id,
            flags: ModificationFlags::all()
        })
    );
    assert!(lifecycle.state.lock().keys_used);

    let cue_handler = item_handler
        .item_event(agent, name)
        .expect("Expected an event handler.");
    let modified = run_handler(meta, agent, cue_handler);
    assert_eq!(
        modified,
        Some(Modification {
            item_id: lane_id,
            flags: ModificationFlags::DIRTY,
        })
    );
    assert!(lifecycle.state.lock().cue_used);

    check(projection(agent), true, expected);
}

fn check<K, V>(
    lane: &DemandMapLane<K, V>,
    requires_event: bool,
    expected: impl IntoIterator<Item = (K, V)>,
) where
    K: Form + Eq + Hash + Clone + Debug,
    V: Form + Eq + Clone + Debug,
{
    let mut buffer = BytesMut::new();
    let expected = expected.into_iter().collect::<HashMap<_, _>>();

    if requires_event {
        assert_eq!(
            lane.write_to_buffer(&mut buffer),
            WriteResult::RequiresEvent
        );
    } else {
        assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);
    }

    let mut decoder = LaneResponseDecoder::new(MapOperationDecoder::<K, V>::default());

    let msg = decoder
        .decode(&mut buffer)
        .expect("Decode failed.")
        .expect("Expected record.");
    match msg {
        LaneResponse::SyncEvent(id, MapOperation::Update { key, value }) => {
            assert_eq!(id, SYNC_ID);
            let maybe = expected.get(&key).expect("Unexpected key.");
            assert_eq!(maybe, &value);
        }
        ow => panic!("Unexpected message: {:?}", ow),
    }
}

#[test]
fn demand_map_lane_left_branch() {
    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::new([(1, 2), (2, 4), (3, 6)]);
    let second_lifecycle = FakeLifecycle::new([(0, Text::new("a")), (1, Text::new("b"))]);
    let leaf = DemandMapLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());

    let branch = DemandMapBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        leaf,
        HLeaf,
    );

    assert!(branch.item_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.item_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.item_event(&agent, "u").is_none()); //After second lane.

    agent.first.sync(SYNC_ID);
    agent.second.sync(SYNC_ID);

    check_interaction(
        &branch,
        &agent,
        FIRST_NAME,
        LANE_ID1,
        &first_lifecycle,
        TestAgent::FIRST,
        [(1, 2), (2, 4), (3, 6)],
    );
    check_interaction(
        &branch,
        &agent,
        SECOND_NAME,
        LANE_ID2,
        &second_lifecycle,
        TestAgent::SECOND,
        [(0, Text::new("a")), (1, Text::new("b"))],
    );
}

#[test]
fn demand_map_lane_right_branch() {
    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::new([(1, 2), (2, 4), (3, 6)]);
    let second_lifecycle = FakeLifecycle::new([(0, Text::new("a")), (1, Text::new("b"))]);
    let leaf = DemandMapLeaf::leaf(SECOND_NAME, TestAgent::SECOND, second_lifecycle.clone());

    let branch = DemandMapBranch::new(
        FIRST_NAME,
        TestAgent::FIRST,
        first_lifecycle.clone(),
        HLeaf,
        leaf,
    );

    assert!(branch.item_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.item_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.item_event(&agent, "u").is_none()); //After second lane.

    agent.first.sync(SYNC_ID);
    agent.second.sync(SYNC_ID);

    check_interaction(
        &branch,
        &agent,
        FIRST_NAME,
        LANE_ID1,
        &first_lifecycle,
        TestAgent::FIRST,
        [(1, 2), (2, 4), (3, 6)],
    );
    check_interaction(
        &branch,
        &agent,
        SECOND_NAME,
        LANE_ID2,
        &second_lifecycle,
        TestAgent::SECOND,
        [(0, Text::new("a")), (1, Text::new("b"))],
    );
}

#[test]
fn demand_lane_two_branches() {
    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::new([(1, 2), (2, 4), (3, 6)]);
    let second_lifecycle = FakeLifecycle::new([(0, Text::new("a")), (1, Text::new("b"))]);
    let third_lifecycle = FakeLifecycle::new([(1, false), (2, true)]);
    let leaf_left = DemandMapLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());
    let leaf_right = DemandMapLeaf::leaf(THIRD_NAME, TestAgent::THIRD, third_lifecycle.clone());

    let branch = DemandMapBranch::new(
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

    agent.first.sync(SYNC_ID);
    agent.second.sync(SYNC_ID);
    agent.third.sync(SYNC_ID);

    check_interaction(
        &branch,
        &agent,
        FIRST_NAME,
        LANE_ID1,
        &first_lifecycle,
        TestAgent::FIRST,
        [(1, 2), (2, 4), (3, 6)],
    );
    check_interaction(
        &branch,
        &agent,
        SECOND_NAME,
        LANE_ID2,
        &second_lifecycle,
        TestAgent::SECOND,
        [(0, Text::new("a")), (1, Text::new("b"))],
    );
    check_interaction(
        &branch,
        &agent,
        THIRD_NAME,
        LANE_ID3,
        &third_lifecycle,
        TestAgent::THIRD,
        [(1, false), (2, true)],
    );
}
