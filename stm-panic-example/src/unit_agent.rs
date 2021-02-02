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
use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
use swim_server::agent::map_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;

#[derive(Debug, SwimAgent)]
pub struct UnitAgent {
    #[lifecycle(name = "ShoppingCartLifecycle")]
    pub shopping_cart: MapLane<String, i32>,
}

#[map_lifecycle(agent = "UnitAgent", key_type = "String", value_type = "i32", on_event)]
struct ShoppingCartLifecycle;

impl ShoppingCartLifecycle {
    async fn on_event<Context>(
        &mut self,
        event: &MapLaneEvent<String, i32>,
        _model: &MapLane<String, i32>,
        _context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        println!("{:?}", event);
    }
}
