use std::num::NonZeroUsize;
use swim_server::agent::AgentConfig;
use swim_server::SwimAgent;

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    #[derive(Debug)]
    pub struct TestAgentConfig {}

    impl AgentConfig for TestAgentConfig {
        fn get_buffer_size(&self) -> NonZeroUsize {
            unimplemented!()
        }
    }

    #[derive(Debug, SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    pub struct TestAgent {}
}
