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

use std::{collections::HashMap, sync::Arc};

use bytes::BytesMut;
use parking_lot::Mutex;
use swimos_agent_protocol::{encoding::lane::RawValueLaneResponseDecoder, LaneResponse};
use swimos_api::agent::AgentConfig;
use swimos_form::write::StructuralWritable;
use swimos_model::Text;
use swimos_recon::print_recon_compact;
use swimos_utilities::routing::RouteUri;
use tokio_util::codec::Decoder;

use crate::{
    agent_lifecycle::item_event::{
        tests::run_handler_expect_mod, DemandBranch, DemandLeaf, HLeaf, ItemEvent,
    },
    agent_model::WriteResult,
    event_handler::{ActionContext, HandlerAction, Modification, StepResult},
    lanes::{
        demand::{lifecycle::on_cue::OnCue, DemandLane},
        LaneItem,
    },
    meta::AgentMetadata,
};

struct TestAgent {
    first: DemandLane<i32>,
    second: DemandLane<Text>,
    third: DemandLane<bool>,
}

const LANE_ID1: u64 = 0;
const LANE_ID2: u64 = 1;
const LANE_ID3: u64 = 2;

impl Default for TestAgent {
    fn default() -> Self {
        TestAgent {
            first: DemandLane::new(LANE_ID1),
            second: DemandLane::new(LANE_ID2),
            third: DemandLane::new(LANE_ID3),
        }
    }
}

impl TestAgent {
    const FIRST: fn(&TestAgent) -> &DemandLane<i32> = |agent| &agent.first;
    const SECOND: fn(&TestAgent) -> &DemandLane<Text> = |agent| &agent.second;
    const THIRD: fn(&TestAgent) -> &DemandLane<bool> = |agent| &agent.third;
}

const FIRST_NAME: &str = "first";
const SECOND_NAME: &str = "second";
const THIRD_NAME: &str = "third";

#[derive(Default, Debug, Clone, Copy)]
struct LifecycleState {
    cued: bool,
}

struct OnCueHandler<T> {
    value: Option<T>,
    state: Arc<Mutex<LifecycleState>>,
}

impl<T: Clone> HandlerAction<TestAgent> for OnCueHandler<T> {
    type Completion = T;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let OnCueHandler { value, state } = self;
        if let Some(v) = value.take() {
            let mut guard = state.lock();
            guard.cued = true;
            StepResult::done(v)
        } else {
            StepResult::after_done()
        }
    }
}

#[derive(Debug, Clone)]
struct FakeLifecycle<T> {
    value: T,
    state: Arc<Mutex<LifecycleState>>,
}

impl<T: Clone> FakeLifecycle<T> {
    fn new(value: T) -> Self {
        FakeLifecycle {
            value,
            state: Default::default(),
        }
    }

    fn on_cue_handler(&self) -> OnCueHandler<T> {
        OnCueHandler {
            value: Some(self.value.clone()),
            state: self.state.clone(),
        }
    }
}

impl<T: Clone + Send + 'static> OnCue<T, TestAgent> for FakeLifecycle<T> {
    type OnCueHandler<'a> = OnCueHandler<T>
    where
        Self: 'a;

    fn on_cue(&self) -> Self::OnCueHandler<'_> {
        self.on_cue_handler()
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
fn demand_lane_leaf() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::default();

    agent.first.cue();

    let lifecycle = FakeLifecycle::new(56);
    let leaf = DemandLeaf::leaf(FIRST_NAME, TestAgent::FIRST, lifecycle.clone());

    assert!(leaf.item_event(&agent, "other").is_none());

    if let Some(handler) = leaf.item_event(&agent, FIRST_NAME) {
        run_handler_expect_mod(
            meta,
            &agent,
            Some(Modification::no_trigger(LANE_ID1)),
            handler,
        );
        let guard = lifecycle.state.lock();
        let LifecycleState { cued } = *guard;
        assert!(cued);
        check(&agent.first, 56);
    } else {
        panic!("Expected an event handler.");
    }
}

fn check<T>(lane: &DemandLane<T>, expected: T)
where
    T: StructuralWritable + Eq,
{
    let mut buffer = BytesMut::new();
    assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);

    let mut decoder = RawValueLaneResponseDecoder::default();
    let msg = decoder
        .decode(&mut buffer)
        .expect("Decode failed.")
        .expect("Expected record.");
    match msg {
        LaneResponse::StandardEvent(body) => {
            let expected_recon = format!("{}", print_recon_compact(&expected));
            assert_eq!(body.as_ref(), expected_recon.as_bytes());
        }
        ow => panic!("Unexpected message: {:?}", ow),
    }
}

#[test]
fn demand_lane_left_branch() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::new(1);
    let second_lifecycle = FakeLifecycle::new(Text::new("name"));
    let leaf = DemandLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());

    let branch = DemandBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        leaf,
        HLeaf,
    );

    assert!(branch.item_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.item_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.item_event(&agent, "u").is_none()); //After second lane.

    agent.first.cue();
    agent.second.cue();

    if let Some(handler) = branch.item_event(&agent, FIRST_NAME) {
        run_handler_expect_mod(
            meta,
            &agent,
            Some(Modification::no_trigger(LANE_ID1)),
            handler,
        );

        let guard = first_lifecycle.state.lock();
        let LifecycleState { cued } = *guard;

        assert!(cued);
        check(&agent.first, 1);
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&agent, SECOND_NAME) {
        run_handler_expect_mod(
            meta,
            &agent,
            Some(Modification::no_trigger(LANE_ID2)),
            handler,
        );

        let guard = second_lifecycle.state.lock();
        let LifecycleState { cued } = *guard;

        assert!(cued);
        check(&agent.second, Text::new("name"));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
fn demand_lane_right_branch() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::new(67);
    let second_lifecycle = FakeLifecycle::new(Text::new("name"));
    let leaf = DemandLeaf::leaf(SECOND_NAME, TestAgent::SECOND, second_lifecycle.clone());

    let branch = DemandBranch::new(
        FIRST_NAME,
        TestAgent::FIRST,
        first_lifecycle.clone(),
        HLeaf,
        leaf,
    );

    assert!(branch.item_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.item_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.item_event(&agent, "u").is_none()); //After second lane.

    agent.first.cue();
    agent.second.cue();

    if let Some(handler) = branch.item_event(&agent, FIRST_NAME) {
        run_handler_expect_mod(
            meta,
            &agent,
            Some(Modification::no_trigger(LANE_ID1)),
            handler,
        );

        let guard = first_lifecycle.state.lock();
        let LifecycleState { cued } = *guard;

        assert!(cued);
        check(&agent.first, 67);
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&agent, SECOND_NAME) {
        run_handler_expect_mod(
            meta,
            &agent,
            Some(Modification::no_trigger(LANE_ID2)),
            handler,
        );

        let guard = second_lifecycle.state.lock();
        let LifecycleState { cued } = *guard;

        assert!(cued);
        check(&agent.second, Text::new("name"));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
fn demand_lane_two_branches() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::new(12);
    let second_lifecycle = FakeLifecycle::new(Text::new("name"));
    let third_lifecycle = FakeLifecycle::new(false);
    let leaf_left = DemandLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());
    let leaf_right = DemandLeaf::leaf(THIRD_NAME, TestAgent::THIRD, third_lifecycle.clone());

    let branch = DemandBranch::new(
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

    agent.first.cue();
    agent.second.cue();
    agent.third.cue();

    if let Some(handler) = branch.item_event(&agent, FIRST_NAME) {
        run_handler_expect_mod(
            meta,
            &agent,
            Some(Modification::no_trigger(LANE_ID1)),
            handler,
        );

        let guard = first_lifecycle.state.lock();
        let LifecycleState { cued } = *guard;

        assert!(cued);
        check(&agent.first, 12);
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&agent, SECOND_NAME) {
        run_handler_expect_mod(
            meta,
            &agent,
            Some(Modification::no_trigger(LANE_ID2)),
            handler,
        );

        let guard = second_lifecycle.state.lock();
        let LifecycleState { cued } = *guard;

        assert!(cued);
        check(&agent.second, Text::new("name"));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&agent, THIRD_NAME) {
        run_handler_expect_mod(
            meta,
            &agent,
            Some(Modification::no_trigger(LANE_ID3)),
            handler,
        );

        let guard = third_lifecycle.state.lock();
        let LifecycleState { cued } = *guard;

        assert!(cued);
        check(&agent.third, false);
    } else {
        panic!("Expected an event handler.");
    }
}
