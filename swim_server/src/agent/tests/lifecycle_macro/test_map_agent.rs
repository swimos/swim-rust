use crate::agent;




use crate::agent::lane::model::map::{MapLane, MapLaneEvent};

use crate::agent::lane::strategy::Queue;
use crate::agent::lifecycle::AgentLifecycle;

use crate::agent::{
    AgentContext, CommandLifecycleTasks, Lane, LaneTasks, LifecycleTasks, SwimAgent,
};
use futures::future::{BoxFuture};
use futures::{FutureExt, Stream, StreamExt};
use futures_util::core_reexport::time::Duration;
use pin_utils::pin_mut;


use std::num::NonZeroUsize;


use stm::var::TVar;
use swim_runtime::time::clock::Clock;
use swim_runtime::time::delay;

use tracing::{span, Level};
use tracing_futures::Instrument;
use url::Url;
use utilities::future::SwimStreamExt;
use utilities::sync::trigger;


const COMMANDED: &str = "Command received";
const ON_EVENT: &str = "On event handler";

struct TestAgent {
    map: MapLane<i64, bool>,
}

struct MapLifecycle<T, S>
where
    T: Fn(&TestAgent) -> &MapLane<i64, bool> + Send + Sync + 'static,
    S: Stream<Item = MapLaneEvent<i64, bool>> + Send + Sync + 'static,
{
    name: String,
    event_stream: S,
    projection: T,
}

async fn custom_on_start<Context>(model: &MapLane<i64, bool>, context: &Context)
where
    Context: AgentContext<TestAgent> + Sized + Send + Sync,
{
    unimplemented!()
}

async fn custom_on_event<Context>(
    event: &MapLaneEvent<i64, bool>,
    model: &MapLane<i64, bool>,
    context: &Context,
) where
    Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
{
    unimplemented!()
}

impl<T, S> Lane for MapLifecycle<T, S>
where
    T: Fn(&TestAgent) -> &MapLane<i64, bool> + Send + Sync + 'static,
    S: Stream<Item = MapLaneEvent<i64, bool>> + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }
}

impl<Context, T, S> LaneTasks<TestAgent, Context> for MapLifecycle<T, S>
where
    Context: AgentContext<TestAgent> + Sized + Send + Sync + 'static,
    S: Stream<Item = MapLaneEvent<i64, bool>> + Send + Sync + 'static,
    T: Fn(&TestAgent) -> &MapLane<i64, bool> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
        let MapLifecycle { projection, .. } = self;

        let model = projection(context.agent());
        custom_on_start(model, context).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let MapLifecycle {
                name: _,
                event_stream,
                projection,
            } = *self;

            let model = projection(context.agent()).clone();
            let events = event_stream.take_until_completes(context.agent_stop_event());
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
        let name = "map";
        let (observer, event_stream) =
            agent::lane::model::map::MapLaneWatch::make_watch(&Queue::default());
        let summary = TVar::new_with_observer(Default::default(), observer);
        let map = MapLane::from_summary(summary);

        let agent = TestAgent { map };

        let task = MapLifecycle {
            name: name.into(),
            event_stream,
            projection: |agent: &TestAgent| &agent.map,
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
