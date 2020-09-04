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

struct TestAgent {
    action: CommandLane<String>,
}

struct ActionLifecycle<T>
where
    T: Fn(&TestAgent) -> &CommandLane<String> + Send + Sync + 'static,
{
    name: String,
    event_stream: mpsc::Receiver<String>,
    projection: T,
}

async fn custom_on_command<Context>(
    command: String,
    model: &ActionLane<String, ()>,
    context: &Context,
) where
    Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
{
    unimplemented!()
}

impl<T: Fn(&TestAgent) -> &CommandLane<String> + Send + Sync + 'static> Lane
    for ActionLifecycle<T>
{
    fn name(&self) -> &str {
        &self.name
    }
}

impl<Context, T> LaneTasks<TestAgent, Context> for ActionLifecycle<T>
where
    Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
    T: Fn(&TestAgent) -> &CommandLane<String> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let ActionLifecycle {
                name,
                event_stream,
                projection,
            } = *self;

            let model = projection(context.agent()).clone();
            let mut events = event_stream.take_until_completes(context.agent_stop_event());
            pin_mut!(events);
            while let Some(command) = events.next().await {
                event!(Level::TRACE, COMMANDED, ?command);
                custom_on_command(command, &model, &context)
                    .instrument(span!(Level::TRACE, ON_COMMAND))
                    .await;
            }
        }
        .boxed()
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
