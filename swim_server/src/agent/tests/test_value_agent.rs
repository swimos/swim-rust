use crate::agent;
use crate::agent::context::ContextImpl;
use crate::agent::lane::lifecycle::ActionLaneLifecycle;
use crate::agent::lane::model;
use crate::agent::lane::model::action::{ActionLane, CommandLane};
use crate::agent::lane::model::value::{ValueLane, ValueLaneWatch};
use crate::agent::lane::strategy::Queue;
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
use std::sync::Arc;
use stm::var::TVar;
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
const ON_EVENT: &str = "On event handler";

struct TestAgent {
    value: ValueLane<i32>,
}

struct ValueLifecycle<T>
where
    T: Fn(&TestAgent) -> &ValueLane<i32> + Send + Sync + 'static,
{
    name: String,
    event_stream: mpsc::Receiver<Arc<i32>>,
    projection: T,
}

async fn custom_on_start<Context>(model: &ValueLane<i32>, context: &Context)
where
    Context: AgentContext<TestAgent> + Sized + Send + Sync,
{
    unimplemented!()
}

async fn custom_on_event<Context>(event: &Arc<i32>, model: &ValueLane<i32>, context: &Context)
where
    Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
{
    unimplemented!()
}

impl<T: Fn(&TestAgent) -> &ValueLane<i32> + Send + Sync + 'static> Lane for ValueLifecycle<T> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<Context, T> LaneTasks<TestAgent, Context> for ValueLifecycle<T>
where
    Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
    T: Fn(&TestAgent) -> &ValueLane<i32> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
        let ValueLifecycle { projection, .. } = self;

        let model = projection(context.agent());
        custom_on_start(model, context).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let ValueLifecycle {
                name,
                event_stream,
                projection,
            } = *self;

            let model = projection(context.agent()).clone();
            let mut events = event_stream.take_until_completes(context.agent_stop_event());
            pin_mut!(events);
            while let Some(event) = events.next().await {
                custom_on_event(&event, &model, &context)
                    .instrument(span!(Level::TRACE, ON_EVENT, ?event))
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
        let init = 0;

        let value = Arc::new(init);
        let (observer, event_stream) = Queue(buffer_size).make_watch(&value);
        let var = TVar::from_arc_with_observer(value, observer);
        let value = ValueLane::from_tvar(var);

        let agent = TestAgent { value };

        let task = ValueLifecycle {
            name: name.into(),
            event_stream,
            projection: |agent: &TestAgent| &agent.value,
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

    super::super::run_agent(config, lifecycle, url, buff_size, clock, stop_sig);
}
