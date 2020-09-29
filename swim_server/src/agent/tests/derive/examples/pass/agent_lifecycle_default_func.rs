use swim_server::agent::AgentContext;
use swim_server::agent_lifecycle;

fn main() {
    struct TestAgent {}

    #[agent_lifecycle(agent = "TestAgent")]
    struct TestAgentLifecycle {}

    impl TestAgentLifecycle {
        async fn on_start<Context>(&self, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }
    }
}
