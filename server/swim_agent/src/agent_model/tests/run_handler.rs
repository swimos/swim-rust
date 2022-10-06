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

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use parking_lot::Mutex;
use swim_api::agent::AgentConfig;
use swim_model::Text;
use swim_utilities::routing::uri::RelativeUri;

use crate::{
    agent_lifecycle::lane_event::LaneEvent,
    agent_model::run_handler,
    event_handler::{
        ActionContext, HandlerAction, HandlerFuture, Modification, Spawner, StepResult,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

struct NoSpawn;

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Lane {
    A,
    B,
    C,
}

impl Lane {
    fn id(&self) -> u64 {
        match self {
            Lane::A => 0,
            Lane::B => 1,
            Lane::C => 2,
        }
    }

    fn name(&self) -> &str {
        match self {
            Lane::A => "a",
            Lane::B => "b",
            Lane::C => "c",
        }
    }
}

#[derive(Default)]
struct TestAgent {
    inner: Arc<Mutex<Inner>>,
}

impl TestAgent {
    fn check_lanes(&self, expected: Vec<Lane>) {
        let guard = self.inner.lock();
        let Inner { events } = &*guard;
        assert_eq!(events, &expected);
    }
}

#[derive(Default)]
struct Inner {
    events: Vec<Lane>,
}

#[derive(Debug, Clone)]
enum HandlerInner {
    NoTargets(bool),
    WithTargets(VecDeque<Lane>),
}

impl Handler {
    fn no_targets(label: Lane) -> Self {
        Handler {
            lane: Some(label),
            inner: HandlerInner::NoTargets(false),
        }
    }

    fn with_targets<I: IntoIterator<Item = Lane>>(label: Lane, it: I) -> Self {
        Handler {
            lane: Some(label),
            inner: HandlerInner::WithTargets(it.into_iter().collect()),
        }
    }
}

#[derive(Debug, Clone)]
struct Handler {
    lane: Option<Lane>,
    inner: HandlerInner,
}

impl HandlerAction<TestAgent> for Handler {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: ActionContext<TestAgent>,
        _meta: AgentMetadata,
        context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let Handler { lane, inner } = self;
        if let Some(lane) = lane.take() {
            let mut guard = context.inner.lock();
            guard.events.push(lane);
        }
        match inner {
            HandlerInner::NoTargets(done) => {
                if *done {
                    StepResult::after_done()
                } else {
                    *done = true;
                    StepResult::done(())
                }
            }
            HandlerInner::WithTargets(targets) => {
                if let Some(lane) = targets.pop_front() {
                    if targets.is_empty() {
                        StepResult::Complete {
                            modified_lane: Some(Modification::of(lane.id())),
                            result: (),
                        }
                    } else {
                        StepResult::Continue {
                            modified_lane: Some(Modification::of(lane.id())),
                        }
                    }
                } else {
                    StepResult::after_done()
                }
            }
        }
    }
}

struct TestLifecycle {
    template: HashMap<Lane, Handler>,
}

impl TestLifecycle {
    fn new(template: HashMap<Lane, Handler>) -> Self {
        TestLifecycle { template }
    }
}

fn chose_lane(lane_name: &str) -> Option<Lane> {
    match lane_name {
        "a" => Some(Lane::A),
        "b" => Some(Lane::B),
        "c" => Some(Lane::C),
        _ => None,
    }
}

impl<'a> LaneEvent<'a, TestAgent> for TestLifecycle {
    type LaneEventHandler = Handler;

    fn lane_event(
        &'a self,
        _context: &TestAgent,
        lane_name: &str,
    ) -> Option<Self::LaneEventHandler> {
        let TestLifecycle { template } = self;
        chose_lane(lane_name)
            .and_then(|lane| template.get(&lane))
            .cloned()
    }
}

const NODE_URI: &str = "/node";
const CONFIG: AgentConfig = AgentConfig::DEFAULT;

fn make_uri() -> RelativeUri {
    RelativeUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta(uri: &RelativeUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
}

fn run_test_handler(agent: &TestAgent, lifecycle: TestLifecycle, start_with: Lane) -> HashSet<u64> {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let mut lanes = HashMap::new();
    lanes.insert(Lane::A.id(), Text::new(Lane::A.name()));
    lanes.insert(Lane::B.id(), Text::new(Lane::B.name()));
    lanes.insert(Lane::C.id(), Text::new(Lane::C.name()));

    let mut collector = HashSet::new();

    if let Some(handler) = lifecycle.lane_event(agent, start_with.name()) {
        let result = run_handler(
            dummy_context(),
            meta,
            agent,
            &lifecycle,
            handler,
            &lanes,
            &mut collector,
        );
        assert!(result.is_ok());
    }

    collector
}

#[test]
fn trivial_handler() {
    let mut template = HashMap::new();
    template.insert(Lane::A, Handler::no_targets(Lane::A));
    let lifecycle = TestLifecycle::new(template);

    let agent = TestAgent::default();
    let ids = run_test_handler(&agent, lifecycle, Lane::A);

    let expected_lanes = vec![Lane::A];

    assert!(ids.is_empty());
    agent.check_lanes(expected_lanes);
}

#[test]
fn simple_event_trigger() {
    let mut template = HashMap::new();
    template.insert(Lane::A, Handler::with_targets(Lane::A, [Lane::B]));
    template.insert(Lane::B, Handler::no_targets(Lane::B));
    let lifecycle = TestLifecycle::new(template);

    let agent = TestAgent::default();
    let ids = run_test_handler(&agent, lifecycle, Lane::A);

    let expected_ids = [Lane::B.id()].into_iter().collect();
    let expected_lanes = vec![Lane::A, Lane::B];

    assert_eq!(ids, expected_ids);
    agent.check_lanes(expected_lanes);
}

#[test]
fn compound_event_trigger() {
    let mut template = HashMap::new();
    template.insert(Lane::A, Handler::with_targets(Lane::A, [Lane::B, Lane::C]));
    template.insert(Lane::B, Handler::no_targets(Lane::B));
    template.insert(Lane::C, Handler::no_targets(Lane::C));
    let lifecycle = TestLifecycle::new(template);

    let agent = TestAgent::default();
    let ids = run_test_handler(&agent, lifecycle, Lane::A);

    let expected_ids = [Lane::B.id(), Lane::C.id()].into_iter().collect();
    let expected_lanes = vec![Lane::A, Lane::B, Lane::C];

    assert_eq!(ids, expected_ids);
    agent.check_lanes(expected_lanes);
}

#[test]
fn multi_hop_event_trigger() {
    let mut template = HashMap::new();
    template.insert(Lane::A, Handler::with_targets(Lane::A, [Lane::B]));
    template.insert(Lane::B, Handler::with_targets(Lane::B, [Lane::C]));
    template.insert(Lane::C, Handler::no_targets(Lane::C));
    let lifecycle = TestLifecycle::new(template);

    let agent = TestAgent::default();
    let ids = run_test_handler(&agent, lifecycle, Lane::A);

    let expected_ids = [Lane::B.id(), Lane::C.id()].into_iter().collect();
    let expected_lanes = vec![Lane::A, Lane::B, Lane::C];

    assert_eq!(ids, expected_ids);
    agent.check_lanes(expected_lanes);
}
