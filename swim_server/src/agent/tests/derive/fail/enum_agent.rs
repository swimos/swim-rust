use swim_server::SwimAgent;

fn main() {
    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[derive(Debug, SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    enum TestAgent {}
}
