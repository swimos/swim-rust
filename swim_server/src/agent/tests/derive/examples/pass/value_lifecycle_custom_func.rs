use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_server::agent::lane::model::value::ValueLane;
use swim_server::agent::{AgentConfig, AgentContext};
use swim_server::value_lifecycle;

fn main() {
    struct TestAgent {}

    #[derive(Debug)]
    pub struct TestAgentConfig {}

    impl AgentConfig for TestAgentConfig {
        fn get_buffer_size(&self) -> NonZeroUsize {
            NonZeroUsize::new(5).unwrap()
        }
    }

    #[value_lifecycle(
        agent = "TestAgent",
        event_type = "i32",
        on_start = "custom_on_start",
        on_event = "custom_on_event"
    )]
    struct ValueLifecycle {}

    impl ValueLifecycle {
        async fn custom_on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }

        async fn custom_on_event<Context>(
            &self,
            _event: &Arc<i32>,
            _model: &ValueLane<i32>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }
}
