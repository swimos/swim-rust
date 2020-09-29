use swim_server::command_lifecycle;

fn main() {
    struct TestAgent {}

    #[derive(Debug)]
    pub struct TestAgentConfig {}

    #[command_lifecycle(agent = "TestAgent", command_type = "i32")]
    enum CommandLifecycle<T> {}
}
