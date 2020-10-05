use swim_server::value_lifecycle;

fn main() {
    struct TestAgent;

    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[value_lifecycle(agent = "TestAgent", event_type = "i32")]
    enum ValueLifecycle {}
}
