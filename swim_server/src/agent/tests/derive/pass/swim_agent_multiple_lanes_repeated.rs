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
        #[lifecycle(public, name = "CommandLifecycle1")]
        command_1: CommandLane<String>,
        #[lifecycle(public, name = "CommandLifecycle2")]
        command_2: CommandLane<i32>,
        #[lifecycle(name = "ActionLifecycle1")]
        action_1: ActionLane<i32, i32>,
        #[lifecycle(name = "ActionLifecycle2")]
        action_2: ActionLane<i64, i64>,
        #[lifecycle(name = "ActionLifecycle3")]
        action_3: ActionLane<String, String>,
        #[lifecycle(public, name = "ValueLifecycle1")]
        value_1: ValueLane<i32>,
        #[lifecycle(name = "ValueLifecycle2")]
        value_2: ValueLane<String>,
        #[lifecycle(public, name = "MapLifecycle1")]
        map_1: MapLane<String, i32>,
        #[lifecycle(name = "MapLifecycle2")]
        map_2: MapLane<i32, String>,
        #[lifecycle(name = "MapLifecycle2")]
        map_3: MapLane<i32, String>,
        #[lifecycle(public, name = "MapLifecycle1")]
        map_4: MapLane<String, i32>,
    }

    #[derive(Debug)]
    pub struct TestAgentConfig;

    impl AgentConfig for TestAgentConfig {
        fn get_buffer_size(&self) -> NonZeroUsize {
            NonZeroUsize::new(5).unwrap()
        }
    }

    // ----------------------- Agent Lifecycle -----------------------

    #[agent_lifecycle(agent = "TestAgent", on_start = "agent_on_start")]
    struct TestAgentLifecycle;

    impl TestAgentLifecycle {
        async fn agent_on_start<Context>(&self, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }
    }

    // ----------------------- Command Lifecycle 1 -----------------------

    #[command_lifecycle(
        agent = "TestAgent",
        command_type = "String",
        on_command = "on_command"
    )]
    struct CommandLifecycle1;

    impl CommandLifecycle1 {
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

    impl LaneLifecycle<TestAgentConfig> for CommandLifecycle1 {
        fn create(_config: &TestAgentConfig) -> Self {
            CommandLifecycle1 {}
        }
    }

    // ----------------------- Command Lifecycle 2 -----------------------

    #[command_lifecycle(agent = "TestAgent", command_type = "i32", on_command = "on_command")]
    struct CommandLifecycle2;

    impl CommandLifecycle2 {
        async fn on_command<Context>(
            &self,
            _command: i32,
            _model: &CommandLane<i32>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    impl LaneLifecycle<TestAgentConfig> for CommandLifecycle2 {
        fn create(_config: &TestAgentConfig) -> Self {
            CommandLifecycle2 {}
        }
    }

    // ----------------------- Action Lifecycle 1 -----------------------

    #[action_lifecycle(agent = "TestAgent", command_type = "i32", response_type = "i32")]
    struct ActionLifecycle1;

    impl ActionLifecycle1 {
        async fn on_command<Context>(
            &self,
            _command: i32,
            _model: &ActionLane<i32, i32>,
            _context: &Context,
        ) -> i32
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    impl LaneLifecycle<TestAgentConfig> for ActionLifecycle1 {
        fn create(_config: &TestAgentConfig) -> Self {
            ActionLifecycle1 {}
        }
    }

    // ----------------------- Action Lifecycle 2 -----------------------

    #[action_lifecycle(agent = "TestAgent", command_type = "i64", response_type = "i64")]
    struct ActionLifecycle2;

    impl ActionLifecycle2 {
        async fn on_command<Context>(
            &self,
            _command: i64,
            _model: &ActionLane<i64, i64>,
            _context: &Context,
        ) -> i64
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    impl LaneLifecycle<TestAgentConfig> for ActionLifecycle2 {
        fn create(_config: &TestAgentConfig) -> Self {
            ActionLifecycle2 {}
        }
    }

    // ----------------------- Action Lifecycle 2 -----------------------

    #[action_lifecycle(agent = "TestAgent", command_type = "String", response_type = "String")]
    struct ActionLifecycle3;

    impl ActionLifecycle3 {
        async fn on_command<Context>(
            &self,
            _command: String,
            _model: &ActionLane<String, String>,
            _context: &Context,
        ) -> String
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    impl LaneLifecycle<TestAgentConfig> for ActionLifecycle3 {
        fn create(_config: &TestAgentConfig) -> Self {
            ActionLifecycle3 {}
        }
    }

    // ----------------------- Value Lifecycle 1 -----------------------

    #[value_lifecycle(agent = "TestAgent", event_type = "i32")]
    struct ValueLifecycle1;

    impl ValueLifecycle1 {
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

    impl LaneLifecycle<TestAgentConfig> for ValueLifecycle1 {
        fn create(_config: &TestAgentConfig) -> Self {
            ValueLifecycle1 {}
        }
    }

    impl StatefulLaneLifecycleBase for ValueLifecycle1 {
        type WatchStrategy = Queue;

        fn create_strategy(&self) -> Self::WatchStrategy {
            Queue::default()
        }
    }

    // ----------------------- Value Lifecycle 2 -----------------------

    #[value_lifecycle(agent = "TestAgent", event_type = "String")]
    struct ValueLifecycle2;

    impl ValueLifecycle2 {
        async fn on_start<Context>(&self, _model: &ValueLane<String>, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }

        async fn on_event<Context>(
            &self,
            _event: &Arc<String>,
            _model: &ValueLane<String>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    impl LaneLifecycle<TestAgentConfig> for ValueLifecycle2 {
        fn create(_config: &TestAgentConfig) -> Self {
            ValueLifecycle2 {}
        }
    }

    impl StatefulLaneLifecycleBase for ValueLifecycle2 {
        type WatchStrategy = Queue;

        fn create_strategy(&self) -> Self::WatchStrategy {
            Queue::default()
        }
    }

    // ----------------------- Map Lifecycle 1 -----------------------

    #[map_lifecycle(agent = "TestAgent", key_type = "String", value_type = "i32")]
    struct MapLifecycle1;

    impl MapLifecycle1 {
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

    impl LaneLifecycle<TestAgentConfig> for MapLifecycle1 {
        fn create(_config: &TestAgentConfig) -> Self {
            MapLifecycle1 {}
        }
    }

    impl StatefulLaneLifecycleBase for MapLifecycle1 {
        type WatchStrategy = Queue;

        fn create_strategy(&self) -> Self::WatchStrategy {
            Queue::default()
        }
    }

    // ----------------------- Map Lifecycle 1 -----------------------

    #[map_lifecycle(agent = "TestAgent", key_type = "i32", value_type = "String")]
    struct MapLifecycle2;

    impl MapLifecycle2 {
        async fn on_start<Context>(&self, _model: &MapLane<i32, String>, _context: &Context)
        where
            Context: AgentContext<TestAgent> + Sized + Send + Sync,
        {
            unimplemented!()
        }

        async fn on_event<Context>(
            &self,
            _event: &MapLaneEvent<i32, String>,
            _model: &MapLane<i32, String>,
            _context: &Context,
        ) where
            Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
        {
            unimplemented!()
        }
    }

    impl LaneLifecycle<TestAgentConfig> for MapLifecycle2 {
        fn create(_config: &TestAgentConfig) -> Self {
            MapLifecycle2 {}
        }
    }

    impl StatefulLaneLifecycleBase for MapLifecycle2 {
        type WatchStrategy = Queue;

        fn create_strategy(&self) -> Self::WatchStrategy {
            Queue::default()
        }
    }
}
