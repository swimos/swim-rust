use swim_server::agent::AgentContext;
use swim_server::agent_lifecycle;

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    struct TestAgent;

    #[agent_lifecycle(agent = "TestAgent")]
    struct TestAgentLifecycle;

    impl TestAgentLifecycle {
        async fn on_start<Context>(&self, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }
    }
}
