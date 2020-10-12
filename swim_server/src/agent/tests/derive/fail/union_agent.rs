use swim_server::SwimAgent;

fn main() {
    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[derive(SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    union TestAgent {
        field: i32,
    }
}
