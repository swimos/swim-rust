// Copyright 2015-2020 SWIM.AI inc.
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
use std::fmt::Debug;
use swim_server::agent::lane::lifecycle::LaneLifecycle;
use swim_server::agent::lane::model::value::{ValueLane, ValueLaneEvent};
use swim_server::agent::SwimAgent;
use swim_server::agent::{value_lifecycle, AgentContext};

#[derive(Debug, SwimAgent)]
pub struct UnitAgent {
    #[lifecycle(name = "InfoLifecycle")]
    pub info: ValueLane<String>,
}

#[value_lifecycle(agent = "UnitAgent", event_type = "String", on_event)]
struct InfoLifecycle {
    count: i32,
}

impl InfoLifecycle {
    async fn on_event<Context>(
        &mut self,
        _event: &ValueLaneEvent<String>,
        _model: &ValueLane<String>,
        _context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        self.count += 1;
        println!("Count: {}", self.count)
    }
}

impl LaneLifecycle<()> for InfoLifecycle {
    fn create(_config: &()) -> Self {
        InfoLifecycle { count: 0 }
    }
}
