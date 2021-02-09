// Copyright 2015-2021 SWIM.AI inc.
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

use swim_server::action_lifecycle;
use swim_server::agent::lane::model::action::ActionLane;
use swim_server::agent::AgentContext;

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    struct TestAgent;

    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[action_lifecycle(
        agent = "TestAgent",
        command_type = "f32",
        response_type = "i32",
        on_command
    )]
    struct ActionLifecycle;

    impl ActionLifecycle {
        async fn on_command<Context>(
            &self,
            _command: f32,
            _model: &ActionLane<f32, i32>,
            _context: &Context,
        ) -> i32
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }
}
