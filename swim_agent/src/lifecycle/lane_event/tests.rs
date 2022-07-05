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

use crate::{
    event_handler::{EventHandler, StepResult},
    lifecycle::lane_event::{HLeaf, LaneEvent},
    meta::AgentMetadata,
};

#[test]
fn hleaf_lane_event() {
    let leaf = HLeaf;

    let agent = ();
    assert!(leaf.lane_event(&agent, "lane").is_none());
}

pub fn run_handler<H, Agent>(meta: AgentMetadata<'_>, agent: &Agent, mut event_handler: H)
where
    H: EventHandler<Agent, Completion = ()>,
{
    loop {
        match event_handler.step(meta, agent) {
            StepResult::Continue { modified_lane } => {
                assert!(modified_lane.is_none());
            }
            StepResult::Fail(err) => {
                panic!("Event handler failed: {}", err);
            }
            StepResult::Complete { modified_lane, .. } => {
                assert!(modified_lane.is_none());
                break;
            }
        }
    }
}
