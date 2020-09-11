




use crate::agent::lifecycle::AgentLifecycle;

use crate::agent::{
    AgentContext, CommandLifecycleTasks, Lane, LaneTasks, LifecycleTasks, SwimAgent,
};
use futures::future::{BoxFuture};
use futures::{FutureExt, StreamExt};
use futures_util::core_reexport::time::Duration;


use std::num::NonZeroUsize;
use swim_runtime::time::clock::Clock;
use swim_runtime::time::delay;



use url::Url;

use utilities::sync::trigger;


struct TestAgent {}
struct TestAgentConfig {}

impl SwimAgent<TestAgentConfig> for TestAgent {
    fn instantiate<Context>(
        _configuration: &TestAgentConfig,
    ) -> (Self, Vec<Box<dyn LaneTasks<Self, Context>>>)
    where
        Context: AgentContext<Self> + Send + Sync + 'static,
    {
        let agent = TestAgent {};

        let tasks = vec![];
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

async fn custom_on_start<Context>(model: &TestAgentLifecycle, context: &Context)
where
    Context: AgentContext<TestAgent> + Sized + Send + Sync,
{
    unimplemented!()
}

impl AgentLifecycle<TestAgent> for TestAgentLifecycle {
    fn on_start<'a, C>(&'a self, context: &'a C) -> BoxFuture<'a, ()>
    where
        C: AgentContext<TestAgent> + Send + Sync + 'a,
    {
        custom_on_start(self, context).boxed()
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

    super::super::super::run_agent(config, lifecycle, url, buff_size, clock, stop_sig);
}
