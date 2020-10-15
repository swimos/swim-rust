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
        on_command = "action_command"
    )]
    struct ActionLifecycle;

    impl ActionLifecycle {
        async fn action_command<Context>(
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
