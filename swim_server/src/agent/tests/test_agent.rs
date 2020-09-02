use crate::agent;
use crate::agent::context::ContextImpl;
use crate::agent::lane::model;
use crate::agent::lane::model::action::{ActionLane, CommandLane};
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::{
    AgentContext, CommandLifecycleTasks, Lane, LaneTasks, LifecycleTasks, SwimAgent,
};
use futures::future::BoxFuture;
use futures_util::core_reexport::time::Duration;
use std::num::NonZeroUsize;
use swim_runtime::time::clock::Clock;
use swim_runtime::time::delay;
use tokio::sync::mpsc;
use url::Url;
use utilities::sync::trigger;

struct TestAgent {
    action: CommandLane<String>,
}

struct ActionLifecycle {}

impl Lane for ActionLifecycle {
    fn name(&self) -> &str {
        "action"
    }
}

impl LaneTasks<TestAgent, ContextImpl<TestAgent, TestClock>> for ActionLifecycle {
    fn start<'a>(&'a self, context: &'a ContextImpl<TestAgent, TestClock>) -> BoxFuture<'a, ()> {
        unimplemented!()
    }

    fn events(
        self: Box<Self>,
        context: ContextImpl<TestAgent, TestClock>,
    ) -> BoxFuture<'static, ()> {
        unimplemented!()
    }
}

struct TestAgentConfig {}

impl SwimAgent<TestAgentConfig> for TestAgent {
    fn instantiate<Context>(
        _configuration: &TestAgentConfig,
    ) -> (Self, Vec<Box<dyn LaneTasks<Self, Context>>>)
    where
        Context: AgentContext<Self> + Send + Sync + 'static,
    {
        let buffer_size = NonZeroUsize::new(5).unwrap();
        let name = "action";
        let projection = |agent: &TestAgent| &agent.action;

        let lifecycle = ActionLifecycle {};

        let (action, event_stream) = model::action::make_lane_model(buffer_size);

        let action_tasks = CommandLifecycleTasks(LifecycleTasks {
            name: name.into(),
            lifecycle,
            event_stream,
            projection,
        });

        // let (action, action_tasks) = agent::make_command_lane(
        //     "action",
        //     ActionLifecycle {},
        //     |agent: &TestAgent| &agent.action,
        //     NonZeroUsize::new(5).unwrap(),
        // );

        let agent = TestAgent { action };

        let tasks = vec![action_tasks.boxed()];
        (agent, tasks)
    }
}

#[derive(Clone, Debug)]
struct TestClock {}

impl Clock for TestClock {
    type DelayFuture = delay::Delay;

    fn delay(&self, duration: Duration) -> Self::DelayFuture {
        delay::delay_for(duration)
    }
}

struct TestAgentLifecycle {}

//This needs to be implemented using LaneTasks
impl AgentLifecycle<TestAgent> for TestAgentLifecycle {
    fn on_start<'a, C>(&'a self, context: &'a C) -> BoxFuture<'a, ()>
    where
        C: AgentContext<TestAgent> + Send + Sync + 'a,
    {
        unimplemented!()
    }
}

#[tokio::test]
async fn test_agent() {
    let config = TestAgentConfig {};
    let lifecycle = TestAgentLifecycle {};
    let url = Url::parse("ws://127.0.0.1:9001/").unwrap();
    let buff_size = NonZeroUsize::new(5).unwrap();
    let clock = TestClock {};
    let (_stop, stop_sig) = trigger::trigger();

    super::super::run_agent(config, lifecycle, url, buff_size, clock, stop_sig);
}
