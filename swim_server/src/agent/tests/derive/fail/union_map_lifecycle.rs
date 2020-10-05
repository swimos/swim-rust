use swim_server::map_lifecycle;

fn main() {
    struct TestAgent;

    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[map_lifecycle(agent = "TestAgent", key_type = "String", value_type = "i32")]
    union MapLifecycle {
        field: i32,
    }
}
