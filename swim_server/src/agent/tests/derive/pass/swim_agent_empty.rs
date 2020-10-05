use swim_server::SwimAgent;

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[derive(Debug, SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    pub struct TestAgent;
}
