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

use swimos::agent::lanes::ValueLane;
use swimos::agent::{lifecycle, projections, AgentLaneModel};
use swimos_form::Form;

#[derive(Debug, Form)]
#[form(fields_convention = "camel")]
pub struct Config {
    min_ripples: usize,
    max_ripples: usize,
    ripple_duration: usize,
    ripple_spread: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            min_ripples: 2,
            max_ripples: 5,
            ripple_duration: 5000,
            ripple_spread: 3000,
        }
    }
}

#[derive(AgentLaneModel)]
#[projections]
pub struct MirrorAgent {
    mode: ValueLane<Config>,
}

#[derive(Clone)]
pub struct MirrorLifecycle;

#[lifecycle(MirrorAgent)]
impl MirrorLifecycle {}
