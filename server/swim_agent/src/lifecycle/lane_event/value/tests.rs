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

use std::sync::Arc;

use parking_lot::Mutex;
use swim_api::agent::AgentConfig;
use swim_model::Text;
use swim_utilities::routing::uri::RelativeUri;

use crate::{
    event_handler::{HandlerAction, StepResult},
    lanes::value::{
        lifecycle::{on_event::OnEvent, on_set::OnSet},
        ValueLane,
    },
    lifecycle::lane_event::{tests::run_handler, HLeaf},
    meta::AgentMetadata,
};

use super::{LaneEvent, ValueBranch, ValueLeaf};

struct TestAgent {
    first: ValueLane<i32>,
    second: ValueLane<Text>,
    third: ValueLane<bool>,
}

const INIT_FIRST: i32 = 4;
const INIT_SECOND: &str = "";
const INIT_THIRD: bool = false;
const LANE_ID1: u64 = 0;
const LANE_ID2: u64 = 1;
const LANE_ID3: u64 = 2;

impl Default for TestAgent {
    fn default() -> Self {
        TestAgent {
            first: ValueLane::new(LANE_ID1, INIT_FIRST),
            second: ValueLane::new(LANE_ID2, Text::new(INIT_SECOND)),
            third: ValueLane::new(LANE_ID3, INIT_THIRD),
        }
    }
}

impl TestAgent {
    const FIRST: fn(&TestAgent) -> &ValueLane<i32> = |agent| &agent.first;
    const SECOND: fn(&TestAgent) -> &ValueLane<Text> = |agent| &agent.second;
    const THIRD: fn(&TestAgent) -> &ValueLane<bool> = |agent| &agent.third;
}

const FIRST_NAME: &str = "first";
const SECOND_NAME: &str = "second";
const THIRD_NAME: &str = "third";

#[derive(Default, Debug, Clone, Copy)]
struct LifecycleState<T> {
    on_event: Option<T>,
    on_set: Option<(Option<T>, T)>,
}

struct OnEventHandler<T> {
    value: T,
    state: Arc<Mutex<LifecycleState<T>>>,
    done: bool,
}

impl<T: Clone> HandlerAction<TestAgent> for OnEventHandler<T> {
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, _context: &TestAgent) -> StepResult<Self::Completion> {
        let OnEventHandler { value, state, done } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            let mut guard = state.lock();
            guard.on_event = Some(value.clone());
            StepResult::done(())
        }
    }
}

struct OnSetHandler<T> {
    prev: Option<T>,
    new_value: T,
    state: Arc<Mutex<LifecycleState<T>>>,
    done: bool,
}

impl<T: Clone> HandlerAction<TestAgent> for OnSetHandler<T> {
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, _context: &TestAgent) -> StepResult<Self::Completion> {
        let OnSetHandler {
            state,
            done,
            prev,
            new_value,
        } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            let mut guard = state.lock();
            guard.on_set = Some((prev.take(), new_value.clone()));
            StepResult::done(())
        }
    }
}

#[derive(Default, Debug, Clone)]
struct FakeLifecycle<T> {
    state: Arc<Mutex<LifecycleState<T>>>,
}

impl<T> FakeLifecycle<T> {
    fn on_event_handler(&self, value: T) -> OnEventHandler<T> {
        OnEventHandler {
            value,
            state: self.state.clone(),
            done: false,
        }
    }

    fn on_set_handler(&self, prev: Option<T>, new_value: T) -> OnSetHandler<T> {
        OnSetHandler {
            prev,
            new_value,
            state: self.state.clone(),
            done: false,
        }
    }
}

impl<'a, T: Clone + Send + 'static> OnEvent<'a, T, TestAgent> for FakeLifecycle<T> {
    type OnEventHandler = OnEventHandler<T>;

    fn on_event(&'a self, value: &T) -> Self::OnEventHandler {
        self.on_event_handler(value.clone())
    }
}

impl<'a, T: Clone + Send + 'static> OnSet<'a, T, TestAgent> for FakeLifecycle<T> {
    type OnSetHandler = OnSetHandler<T>;

    fn on_set(&'a self, existing: Option<T>, new_value: &T) -> Self::OnSetHandler {
        self.on_set_handler(existing, new_value.clone())
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
#[test]
fn value_lane_leaf() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let agent = TestAgent::default();

    agent.first.set(56);

    let lifecycle = FakeLifecycle::<i32>::default();
    let leaf = ValueLeaf::leaf(FIRST_NAME, TestAgent::FIRST, lifecycle.clone());

    assert!(leaf.lane_event(&agent, "other").is_none());

    if let Some(handler) = leaf.lane_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);
        let guard = lifecycle.state.lock();
        let LifecycleState { on_event, on_set } = *guard;

        assert_eq!(on_event, Some(56));
        assert_eq!(on_set, Some((Some(INIT_FIRST), 56)));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
fn value_lane_left_branch() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::<i32>::default();
    let second_lifecycle = FakeLifecycle::<Text>::default();
    let leaf = ValueLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());

    let branch = ValueBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        leaf,
        HLeaf,
    );

    assert!(branch.lane_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.lane_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.lane_event(&agent, "u").is_none()); //After second lane.

    agent.first.set(56);
    let hello = Text::new("Hello");
    agent.second.set(hello.clone());

    if let Some(handler) = branch.lane_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);

        let guard = first_lifecycle.state.lock();
        let LifecycleState { on_event, on_set } = *guard;

        assert_eq!(on_event, Some(56));
        assert_eq!(on_set, Some((Some(INIT_FIRST), 56)));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.lane_event(&agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);

        let guard = second_lifecycle.state.lock();
        let LifecycleState { on_event, on_set } = guard.clone();

        assert_eq!(on_event, Some(hello.clone()));
        assert_eq!(on_set, Some((Some(Text::new(INIT_SECOND)), hello)));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
fn value_lane_right_branch() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::<i32>::default();
    let second_lifecycle = FakeLifecycle::<Text>::default();
    let leaf = ValueLeaf::leaf(SECOND_NAME, TestAgent::SECOND, second_lifecycle.clone());

    let branch = ValueBranch::new(
        FIRST_NAME,
        TestAgent::FIRST,
        first_lifecycle.clone(),
        HLeaf,
        leaf,
    );

    assert!(branch.lane_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.lane_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.lane_event(&agent, "u").is_none()); //After second lane.

    agent.first.set(56);
    let hello = Text::new("Hello");
    agent.second.set(hello.clone());

    if let Some(handler) = branch.lane_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);

        let guard = first_lifecycle.state.lock();
        let LifecycleState { on_event, on_set } = *guard;

        assert_eq!(on_event, Some(56));
        assert_eq!(on_set, Some((Some(INIT_FIRST), 56)));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.lane_event(&agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);

        let guard = second_lifecycle.state.lock();
        let LifecycleState { on_event, on_set } = guard.clone();

        assert_eq!(on_event, Some(hello.clone()));
        assert_eq!(on_set, Some((Some(Text::new(INIT_SECOND)), hello)));
    } else {
        panic!("Expected an event handler.");
    }
}
#[test]
fn value_lane_two_branches() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::<i32>::default();
    let second_lifecycle = FakeLifecycle::<Text>::default();
    let third_lifecycle = FakeLifecycle::<bool>::default();

    let left = ValueLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());
    let right = ValueLeaf::leaf(THIRD_NAME, TestAgent::THIRD, third_lifecycle.clone());

    let branch = ValueBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        left,
        right,
    );

    assert!(branch.lane_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.lane_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.lane_event(&agent, "sf").is_none()); //Between second and third lanes.
    assert!(branch.lane_event(&agent, "u").is_none()); //After third lane.

    agent.first.set(56);
    let hello = Text::new("Hello");
    agent.second.set(hello.clone());
    agent.third.set(true);

    if let Some(handler) = branch.lane_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);

        let guard = first_lifecycle.state.lock();
        let LifecycleState { on_event, on_set } = *guard;

        assert_eq!(on_event, Some(56));
        assert_eq!(on_set, Some((Some(INIT_FIRST), 56)));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.lane_event(&agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);

        let guard = second_lifecycle.state.lock();
        let LifecycleState { on_event, on_set } = guard.clone();

        assert_eq!(on_event, Some(hello.clone()));
        assert_eq!(on_set, Some((Some(Text::new(INIT_SECOND)), hello)));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.lane_event(&agent, THIRD_NAME) {
        run_handler(meta, &agent, handler);

        let guard = third_lifecycle.state.lock();
        let LifecycleState { on_event, on_set } = *guard;

        assert_eq!(on_event, Some(true));
        assert_eq!(on_set, Some((Some(INIT_THIRD), true)));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
#[should_panic]
fn fail_out_of_order_labels_right() {
    let first_lifecycle = FakeLifecycle::<i32>::default();
    let second_lifecycle = FakeLifecycle::<Text>::default();
    let leaf = ValueLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle);

    ValueBranch::new(
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
    let first_lifecycle = FakeLifecycle::<i32>::default();
    let second_lifecycle = FakeLifecycle::<Text>::default();
    let leaf = ValueLeaf::leaf(SECOND_NAME, TestAgent::SECOND, second_lifecycle);

    ValueBranch::new(FIRST_NAME, TestAgent::FIRST, first_lifecycle, leaf, HLeaf);
}

#[test]
#[should_panic]
fn fail_equal_labels() {
    let first_lifecycle = FakeLifecycle::<i32>::default();
    let leaf = ValueLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());

    ValueBranch::new(FIRST_NAME, TestAgent::FIRST, first_lifecycle, HLeaf, leaf);
}
