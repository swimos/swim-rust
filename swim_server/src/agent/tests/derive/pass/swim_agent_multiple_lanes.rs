use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_server::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
use swim_server::agent::lane::model::action::{ActionLane, CommandLane};
use swim_server::agent::lane::model::map::{MapLane, MapLaneEvent};
use swim_server::agent::lane::model::value::ValueLane;
use swim_server::agent::lane::strategy::Queue;
use swim_server::agent::{AgentConfig, AgentContext};
use swim_server::{
    action_lifecycle, agent_lifecycle, command_lifecycle, map_lifecycle, value_lifecycle, SwimAgent,
};

mod swim_server {
    pub use crate::*;
}

#[test]
fn main() {
    // ----------------------- Agent derivation -----------------------

    #[derive(Debug, SwimAgent)]
    #[agent(config = "TestAgentConfig")]
    pub struct TestAgent {
        #[lifecycle(public, name = "CommandLifecycle")]
        command: CommandLane<String>,
        #[lifecycle(name = "ActionLifecycle")]
        action: ActionLane<String, i32>,
        #[lifecycle(name = "ValueLifecycle")]
        value: ValueLane<i32>,
        #[lifecycle(name = "MapLifecycle")]
        map: MapLane<String, i32>,
    }

    #[derive(Debug)]
    pub struct TestAgentConfig {}

    impl AgentConfig for TestAgentConfig {
        fn get_buffer_size(&self) -> NonZeroUsize {
            NonZeroUsize::new(5).unwrap()
        }
    }

    // ----------------------- Agent Lifecycle -----------------------

    #[agent_lifecycle(agent = "TestAgent", on_start = "agent_on_start")]
    struct TestAgentLifecycle {}

    impl TestAgentLifecycle {
        async fn agent_on_start<Context>(&self, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }
    }

    // ----------------------- Command Lifecycle -----------------------

    #[command_lifecycle(
        agent = "TestAgent",
        command_type = "String",
        on_command = "on_command"
    )]
    struct CommandLifecycle {}

    impl CommandLifecycle {
        async fn on_command<Context>(
            &self,
            _command: String,
            _model: &CommandLane<String>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    impl LaneLifecycle<TestAgentConfig> for CommandLifecycle {
        fn create(_config: &TestAgentConfig) -> Self {
            CommandLifecycle {}
        }
    }

    // ----------------------- Action Lifecycle -----------------------

    #[action_lifecycle(agent = "TestAgent", command_type = "String", response_type = "i32")]
    struct ActionLifecycle {}

    impl ActionLifecycle {
        async fn on_command<Context>(
            &self,
            _command: String,
            _model: &ActionLane<String, i32>,
            _context: &Context,
        ) -> i32
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    impl LaneLifecycle<TestAgentConfig> for ActionLifecycle {
        fn create(_config: &TestAgentConfig) -> Self {
            ActionLifecycle {}
        }
    }

    // ----------------------- Value Lifecycle -----------------------

    #[value_lifecycle(agent = "TestAgent", event_type = "i32")]
    struct ValueLifecycle {}

    impl ValueLifecycle {
        async fn on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }

        async fn on_event<Context>(
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

    impl LaneLifecycle<TestAgentConfig> for ValueLifecycle {
        fn create(_config: &TestAgentConfig) -> Self {
            ValueLifecycle {}
        }
    }

    impl StatefulLaneLifecycleBase for ValueLifecycle {
        type WatchStrategy = Queue;

        fn create_strategy(&self) -> Self::WatchStrategy {
            Queue::default()
        }
    }

    // ----------------------- Map Lifecycle -----------------------

    #[map_lifecycle(agent = "TestAgent", key_type = "String", value_type = "i32")]
    struct MapLifecycle {}

    impl MapLifecycle {
        async fn on_start<Context>(&self, _model: &MapLane<String, i32>, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }

        async fn on_event<Context>(
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

    impl LaneLifecycle<TestAgentConfig> for MapLifecycle {
        fn create(_config: &TestAgentConfig) -> Self {
            MapLifecycle {}
        }
    }

    impl StatefulLaneLifecycleBase for MapLifecycle {
        type WatchStrategy = Queue;

        fn create_strategy(&self) -> Self::WatchStrategy {
            Queue::default()
        }
    }
}
