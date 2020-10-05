use std::num::NonZeroUsize;
use swim_server::agent::lane::lifecycle::LaneLifecycle;
use swim_server::agent::lane::model::action::CommandLane;
use swim_server::agent::{AgentConfig, AgentContext};
use swim_server::{command_lifecycle, SwimAgent};

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    #[derive(Debug)]
    pub struct TestAgentConfig;

    impl AgentConfig for TestAgentConfig {
        fn get_buffer_size(&self) -> NonZeroUsize {
            unimplemented!()
        }
    }

    #[command_lifecycle(agent = "TestAgent", command_type = "i32")]
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

    impl LaneLifecycle<TestAgentConfig> for CommandLifecycle {
        fn create(_config: &TestAgentConfig) -> Self {
            CommandLifecycle {}
        }
    }

    #[derive(Debug, SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    pub struct TestAgent {
        #[lifecycle(public, name = "CommandLifecycle")]
        command: CommandLane<i32>,
    }
}
