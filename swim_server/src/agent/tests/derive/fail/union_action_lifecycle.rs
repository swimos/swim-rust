use swim_server::action_lifecycle;

fn main() {
    struct TestAgent;

    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[action_lifecycle(agent = "TestAgent", command_type = "f32", response_type = "i32")]
    union ActionLifecycle {
        field: i32,
    }
}
