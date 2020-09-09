use crate::agent;
use crate::agent::context::ContextImpl;
use crate::agent::lane::lifecycle::ActionLaneLifecycle;
use crate::agent::lane::model;
use crate::agent::lane::model::action::{ActionLane, CommandLane};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent, MapLaneWatch};
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
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use stm::local::TLocal;
use stm::var::TVar;
use swim_runtime::time::clock::Clock;
use swim_runtime::time::delay;
use tokio::sync::{mpsc, watch};
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use url::Url;
use utilities::future::SwimStreamExt;
use utilities::sync::trigger;
use utilities::sync::trigger::Receiver;

const COMMANDED: &str = "Command received";
const ON_EVENT: &str = "On event handler";

struct TestAgent {
    map: MapLane<i64, bool>,
}

#[map_lifecycle(
    agent = "TestAgent",
    key_type = "i64",
    value_type = "bool",
    on_start = "custom_on_start",
    on_event = "custom_on_event"
)]
struct MapLifecycle {}

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
