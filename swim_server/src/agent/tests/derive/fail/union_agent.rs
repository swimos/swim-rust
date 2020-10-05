use std::num::NonZeroUsize;
use swim_server::agent::AgentConfig;
use swim_server::SwimAgent;

fn main() {
    #[derive(Debug)]
    pub struct TestAgentConfig;

    impl AgentConfig for TestAgentConfig {
        fn get_buffer_size(&self) -> NonZeroUsize {
            unimplemented!()
        }
    }

    #[derive(SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    union TestAgent {
        field: i32,
    }
}
