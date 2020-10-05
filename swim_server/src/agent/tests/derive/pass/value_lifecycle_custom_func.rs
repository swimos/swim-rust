use std::sync::Arc;
use swim_server::agent::lane::model::value::ValueLane;
use swim_server::agent::AgentContext;
use swim_server::value_lifecycle;

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    struct TestAgent;

    #[derive(Debug)]
    pub struct TestAgentConfig;

    #[value_lifecycle(
        agent = "TestAgent",
        event_type = "i32",
        on_start = "custom_on_start",
        on_event = "custom_on_event"
    )]
    struct ValueLifecycle;

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
