use swim_server::SwimAgent;

fn main() {
    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[derive(Debug, SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    pub struct TestAgent<S> {
        field: S,
    }
}
