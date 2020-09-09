use crate::agent;
use crate::agent::context::ContextImpl;
use crate::agent::lane::lifecycle::ActionLaneLifecycle;
use crate::agent::lane::model;
use crate::agent::lane::model::action::{ActionLane, CommandLane};
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::tests::TestContext;
use crate::agent::{
    AgentContext, CommandLifecycleTasks, Lane, LaneTasks, LifecycleTasks, SwimAgent,
};
use futures::future::{ready, BoxFuture};
use futures::{FutureExt, Stream, StreamExt};
use futures_util::core_reexport::time::Duration;
use pin_utils::pin_mut;
use std::future::Future;
use std::num::NonZeroUsize;
use swim_runtime::time::clock::Clock;
use swim_runtime::time::delay;
use tokio::sync::mpsc;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use url::Url;
use utilities::future::SwimStreamExt;
use utilities::sync::trigger;
use utilities::sync::trigger::Receiver;

const COMMANDED: &str = "Command received";
const ON_COMMAND: &str = "On command handler";
const ACTION_RESULT: &str = "Action result";

struct TestAgent {
    action: ActionLane<String, usize>,
}

#[action_lifecycle(
    agent = "TestAgent",
    command_type = "String",
    response_type = "usize",
    on_command = "custom_on_command"
)]
struct ActionLifecycle {}

async fn custom_on_command<Context, Agent, Config>(
    command: String,
    model: &ActionLane<String, usize>,
    context: &Context,
) -> usize
where
    Agent: SwimAgent<Config> + Send + Sync + 'static,
    Context: AgentContext<Agent> + Sized + Send + Sync + 'static,
{
    unimplemented!()
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

        let (tx, event_stream) = mpsc::channel(buffer_size.get());
        let action = ActionLane::new(tx);

        let agent = TestAgent { action };

        let task = ActionLifecycle {
            name: name.into(),
            event_stream,
            projection: |agent: &TestAgent| &agent.action,
        };

        let tasks = vec![task.boxed()];
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

    super::super::super::run_agent(config, lifecycle, url, buff_size, clock, stop_sig);
}
