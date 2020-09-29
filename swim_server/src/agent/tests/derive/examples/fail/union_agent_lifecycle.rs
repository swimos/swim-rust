use swim_server::agent_lifecycle;

fn main() {
    struct TestAgent {}

    #[derive(Debug)]
    pub struct TestAgentConfig {}

    #[agent_lifecycle(agent = "TestAgent")]
    union TestAgentLifecycle {
        field: i32,
    }
}
