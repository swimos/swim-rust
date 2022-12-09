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
    agent_lifecycle::lane_event::{
        tests::run_handler, CommandBranch, CommandLeaf, HLeaf, LaneEvent,
    },
    event_handler::{HandlerAction, Spawner, StepResult},
    lanes::command::{lifecycle::on_command::OnCommand, CommandLane},
    meta::AgentMetadata,
};

struct TestAgent {
    first: CommandLane<i32>,
    second: CommandLane<Text>,
    third: CommandLane<bool>,
}

const LANE_ID1: u64 = 0;
const LANE_ID2: u64 = 1;
const LANE_ID3: u64 = 2;

impl Default for TestAgent {
    fn default() -> Self {
        TestAgent {
            first: CommandLane::new(LANE_ID1),
            second: CommandLane::new(LANE_ID2),
            third: CommandLane::new(LANE_ID3),
        }
    }
}

impl TestAgent {
    const FIRST: fn(&TestAgent) -> &CommandLane<i32> = |agent| &agent.first;
    const SECOND: fn(&TestAgent) -> &CommandLane<Text> = |agent| &agent.second;
    const THIRD: fn(&TestAgent) -> &CommandLane<bool> = |agent| &agent.third;
}

const FIRST_NAME: &str = "first";
const SECOND_NAME: &str = "second";
const THIRD_NAME: &str = "third";

#[derive(Default, Debug, Clone, Copy)]
struct LifecycleState<T> {
    on_command: Option<T>,
}

struct OnCommandHandler<T> {
    value: T,
    state: Arc<Mutex<LifecycleState<T>>>,
    done: bool,
}

impl<T: Clone> HandlerAction<TestAgent> for OnCommandHandler<T> {
    type Completion = ();

    fn step(
        &mut self,
        _suspend: &dyn Spawner<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let OnCommandHandler { value, state, done } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            let mut guard = state.lock();
            guard.on_command = Some(value.clone());
            StepResult::done(())
        }
    }
}

#[derive(Default, Debug, Clone)]
struct FakeLifecycle<T> {
    state: Arc<Mutex<LifecycleState<T>>>,
}

impl<T> FakeLifecycle<T> {
    fn on_command_handler(&self, value: T) -> OnCommandHandler<T> {
        OnCommandHandler {
            value,
            state: self.state.clone(),
            done: false,
        }
    }
}

impl<'a, T: Clone + Send + 'static> OnCommand<'a, T, TestAgent> for FakeLifecycle<T> {
    type OnCommandHandler = OnCommandHandler<T>;

    fn on_command(&'a self, value: &T) -> Self::OnCommandHandler {
        self.on_command_handler(value.clone())
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
fn command_lane_leaf() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let agent = TestAgent::default();

    agent.first.command(56);

    let lifecycle = FakeLifecycle::<i32>::default();
    let leaf = CommandLeaf::leaf(FIRST_NAME, TestAgent::FIRST, lifecycle.clone());

    assert!(leaf.lane_event(&agent, "other").is_none());

    if let Some(handler) = leaf.lane_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);
        let guard = lifecycle.state.lock();
        let LifecycleState { on_command } = *guard;

        assert_eq!(on_command, Some(56));
    } else {
        panic!("Expected an event handler.");
    }
}
#[test]
fn command_lane_left_branch() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::<i32>::default();
    let second_lifecycle = FakeLifecycle::<Text>::default();
    let leaf = CommandLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());

    let branch = CommandBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        leaf,
        HLeaf,
    );

    assert!(branch.lane_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.lane_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.lane_event(&agent, "u").is_none()); //After second lane.

    agent.first.command(56);
    let hello = Text::new("Hello");
    agent.second.command(hello.clone());

    if let Some(handler) = branch.lane_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);

        let guard = first_lifecycle.state.lock();
        let LifecycleState { on_command } = *guard;

        assert_eq!(on_command, Some(56));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.lane_event(&agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);

        let guard = second_lifecycle.state.lock();
        let LifecycleState { on_command } = guard.clone();

        assert_eq!(on_command, Some(hello));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
fn command_lane_right_branch() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::<i32>::default();
    let second_lifecycle = FakeLifecycle::<Text>::default();
    let leaf = CommandLeaf::leaf(SECOND_NAME, TestAgent::SECOND, second_lifecycle.clone());

    let branch = CommandBranch::new(
        FIRST_NAME,
        TestAgent::FIRST,
        first_lifecycle.clone(),
        HLeaf,
        leaf,
    );

    assert!(branch.lane_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.lane_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.lane_event(&agent, "u").is_none()); //After second lane.

    agent.first.command(56);
    let hello = Text::new("Hello");
    agent.second.command(hello.clone());

    if let Some(handler) = branch.lane_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);

        let guard = first_lifecycle.state.lock();
        let LifecycleState { on_command } = *guard;

        assert_eq!(on_command, Some(56));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.lane_event(&agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);

        let guard = second_lifecycle.state.lock();
        let LifecycleState { on_command } = guard.clone();

        assert_eq!(on_command, Some(hello));
    } else {
        panic!("Expected an event handler.");
    }
}

#[test]
fn command_lane_two_branches() {
    let uri = make_uri();
    let meta = make_meta(&uri);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::<i32>::default();
    let second_lifecycle = FakeLifecycle::<Text>::default();
    let third_lifecycle = FakeLifecycle::<bool>::default();
    let leaf_left = CommandLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());
    let leaf_right = CommandLeaf::leaf(THIRD_NAME, TestAgent::THIRD, third_lifecycle.clone());

    let branch = CommandBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        leaf_left,
        leaf_right,
    );

    assert!(branch.lane_event(&agent, "a").is_none()); //Before first lane.
    assert!(branch.lane_event(&agent, "g").is_none()); //Between first and second lanes.
    assert!(branch.lane_event(&agent, "sf").is_none()); //Between second and third lanes.
    assert!(branch.lane_event(&agent, "u").is_none()); //After third lane.

    agent.first.command(56);
    let hello = Text::new("Hello");
    agent.second.command(hello.clone());
    agent.third.command(true);

    if let Some(handler) = branch.lane_event(&agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);

        let guard = first_lifecycle.state.lock();
        let LifecycleState { on_command } = *guard;

        assert_eq!(on_command, Some(56));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.lane_event(&agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);

        let guard = second_lifecycle.state.lock();
        let LifecycleState { on_command } = guard.clone();

        assert_eq!(on_command, Some(hello));
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.lane_event(&agent, THIRD_NAME) {
        run_handler(meta, &agent, handler);

        let guard = third_lifecycle.state.lock();
        let LifecycleState { on_command } = *guard;

        assert_eq!(on_command, Some(true));
    } else {
        panic!("Expected an event handler.");
    }
}
