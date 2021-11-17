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

use swim_server::agent::lane::model::action::ActionLane;
use swim_server::agent::lane::model::command::CommandLane;
use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
use swim_server::agent::lane::model::value::{ValueLane, ValueLaneEvent};
use swim_server::agent::AgentContext;
use swim_server::{
    action_lifecycle, agent_lifecycle, command_lifecycle, map_lifecycle, value_lifecycle, SwimAgent,
};

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    // ----------------------- Agent derivation -----------------------

    #[derive(Debug, SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    pub struct TestAgent {
        #[lifecycle(name = "CommandLifecycle")]
        pub command: CommandLane<String>,
        #[lifecycle(name = "ActionLifecycle")]
        action: ActionLane<String, i32>,
        #[lifecycle(name = "ValueLifecycle")]
        value: ValueLane<i32>,
        #[lifecycle(name = "MapLifecycle")]
        map: MapLane<String, i32>,
    }

    #[derive(Debug)]
    pub struct TestAgentConfig;

    // ----------------------- Agent Lifecycle -----------------------

    #[agent_lifecycle(agent = "TestAgent", on_start = "agent_on_start")]
    struct TestAgentLifecycle;

    impl TestAgentLifecycle {
        async fn agent_on_start<Context>(&self, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }
    }

    // ----------------------- Command Lifecycle -----------------------

    #[command_lifecycle(
        agent = "TestAgent",
        command_type = "String",
        on_command = "on_command"
    )]
    struct CommandLifecycle;

    impl CommandLifecycle {
        async fn on_command<Context>(
            &self,
            _command: &String,
            _model: &CommandLane<String>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    // ----------------------- Action Lifecycle -----------------------

    #[action_lifecycle(
        agent = "TestAgent",
        command_type = "String",
        response_type = "i32",
        on_command
    )]
    struct ActionLifecycle;

    impl ActionLifecycle {
        async fn on_command<Context>(
            &self,
            _command: String,
            _model: &ActionLane<String, i32>,
            _context: &Context,
        ) -> i32
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    // ----------------------- Value Lifecycle -----------------------

    #[value_lifecycle(agent = "TestAgent", event_type = "i32", on_start, on_event)]
    struct ValueLifecycle;

    impl ValueLifecycle {
        async fn on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }

        async fn on_event<Context>(
            &self,
            _event: &ValueLaneEvent<i32>,
            _model: &ValueLane<i32>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    // ----------------------- Map Lifecycle -----------------------

    #[map_lifecycle(
        agent = "TestAgent",
        key_type = "String",
        value_type = "i32",
        on_start,
        on_event
    )]
    struct MapLifecycle;

    impl MapLifecycle {
        async fn on_start<Context>(&self, _model: &MapLane<String, i32>, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }

        async fn on_event<Context>(
            &self,
            _event: &MapLaneEvent<String, i32>,
            _model: &MapLane<String, i32>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }
}
