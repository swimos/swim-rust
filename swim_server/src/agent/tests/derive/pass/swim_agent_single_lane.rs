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

use swim_server::agent::lane::model::command::CommandLane;
use swim_server::agent::AgentContext;
use swim_server::{command_lifecycle, SwimAgent};

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[command_lifecycle(agent = "TestAgent", command_type = "i32", on_command)]
    struct CommandLifecycle;

    impl CommandLifecycle {
        async fn on_command<Context>(
            &self,
            _command: i32,
            _model: &CommandLane<i32>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    #[derive(Debug, SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    pub struct TestAgent {
        #[lifecycle(name = "CommandLifecycle")]
        pub command: CommandLane<i32>,
    }
}
