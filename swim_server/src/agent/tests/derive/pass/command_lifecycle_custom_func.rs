use swim_server::agent::lane::model::action::CommandLane;
use swim_server::agent::AgentContext;
use swim_server::command_lifecycle;

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    struct TestAgent;

    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[command_lifecycle(
        agent = "TestAgent",
        command_type = "i32",
        on_command = "custom_function"
    )]
    struct CommandLifecycle;

    impl CommandLifecycle {
        async fn custom_function<Context>(
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
}
