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

use std::collections::HashMap;

use crate::{
    agent_lifecycle::item_event::{HLeaf, ItemEvent},
    event_handler::{HandlerAction, StepResult},
    meta::AgentMetadata,
    test_context::dummy_context,
};

#[test]
fn hleaf_lane_event() {
    let leaf = HLeaf;

    assert!(leaf.item_event(&(), "lane").is_none());
}

pub fn run_handler<H, Agent>(meta: AgentMetadata<'_>, agent: &Agent, mut event_handler: H)
where
    H: HandlerAction<Agent, Completion = ()>,
{
    let mut join_value_init = HashMap::new();
    loop {
        match event_handler.step(&mut dummy_context(&mut join_value_init), meta, agent) {
            StepResult::Continue { modified_item } => {
                assert!(modified_item.is_none());
            }
            StepResult::Fail(err) => {
                panic!("Event handler failed: {}", err);
            }
            StepResult::Complete { modified_item, .. } => {
                assert!(modified_item.is_none());
                break;
            }
        }
    }
    assert!(join_value_init.is_empty());
}
