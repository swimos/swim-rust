use std::num::NonZeroUsize;
use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
use swim_server::agent::{AgentConfig, AgentContext};
use swim_server::map_lifecycle;

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    struct TestAgent;

    #[derive(Debug)]
    pub struct TestAgentConfig;

    impl AgentConfig for TestAgentConfig {
        fn get_buffer_size(&self) -> NonZeroUsize {
            NonZeroUsize::new(5).unwrap()
        }
    }

    #[map_lifecycle(
        agent = "TestAgent",
        key_type = "String",
        value_type = "i32",
        on_start = "map_on_start",
        on_event = "map_on_event"
    )]
    struct MapLifecycle;

    impl MapLifecycle {
        async fn map_on_start<Context>(&self, _model: &MapLane<String, i32>, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }

        async fn map_on_event<Context>(
            &self,
            _event: &MapLaneEvent<String, i32>,
            _model: &MapLane<String, i32>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }
}
