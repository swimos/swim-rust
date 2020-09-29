use std::num::NonZeroUsize;
use swim_server::agent::lane::model::action::ActionLane;
use swim_server::agent::{AgentConfig, AgentContext};
use swim_server::command_lifecycle;

fn main() {
    struct TestAgent {}

    #[derive(Debug)]
    pub struct TestAgentConfig {}

    impl AgentConfig for TestAgentConfig {
        fn get_buffer_size(&self) -> NonZeroUsize {
            NonZeroUsize::new(5).unwrap()
        }
    }

    #[command_lifecycle(agent = "TestAgent", command_type = "i32")]
    struct CommandLifecycle {}

    impl CommandLifecycle {
        async fn on_command<Context>(
            &self,
            _command: i32,
            _model: &ActionLane<i32, ()>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }
}
