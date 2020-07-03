// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::agent::context::ContextImpl;
use crate::agent::lane::lifecycle::{ActionLaneLifecycle, StatefulLaneLifecycle};
use crate::agent::lane::model;
use crate::agent::lane::model::action::{ActionLane, CommandLane};
use crate::agent::lane::model::map::MapLaneEvent;
use crate::agent::lane::model::map::{MapLane, MapLaneWatch};
use crate::agent::lane::model::value::{ValueLane, ValueLaneWatch};
use crate::agent::lifecycle::AgentLifecycle;
use futures::future::{ready, BoxFuture};
use futures::sink::drain;
use futures::stream::{once, repeat, unfold, BoxStream};
use futures::{FutureExt, Stream, StreamExt};
use futures_util::stream::FuturesUnordered;
use pin_utils::pin_mut;
use std::any::Any;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use swim_form::Form;
use tokio::sync::mpsc;
use url::Url;
use utilities::future::SwimStreamExt;

mod context;
pub mod lane;
pub mod lifecycle;

/// Trait that must be implemented for any agent. This is essentially just boilerplate and will
/// eventually be implemented using a derive macro.
/// TODO Write derive macro for SwimAgent.
pub trait SwimAgent: Sized {
    /// Create an instance of the agent and life-cycle handles for each of its lanes.
    fn instantiate<Context: AgentContext<Self>>() -> (Self, Vec<Box<dyn LaneTasks<Self, Context>>>);
}

/// Creates a single, asynchronous task that manages the lifecycle of an agent, all of its lanes
/// and any events that are scheduled within it.
///
/// #Arguments
///
/// * `lifecycle` - Life-cycle event handler for the agent.
/// * `url` - The node URL for the agent instance.
/// * `schedule_buffer_size` - The buffer size for the MPSC channel used by the agent to schedule
/// events.
pub async fn run_agent<Agent, L>(lifecycle: L, url: Url, scheduler_buffer_size: NonZeroUsize)
where
    Agent: SwimAgent + Send + Sync + 'static,
    L: AgentLifecycle<Agent>,
{
    let (agent, tasks) = Agent::instantiate::<ContextImpl<Agent>>();
    let agent_ref = Arc::new(agent);
    let (tx, rx) = mpsc::channel(scheduler_buffer_size.get());
    let context = ContextImpl::new(agent_ref, url, tx);

    lifecycle.on_start(&context).await;

    for lane_task in tasks.iter() {
        (**lane_task).start(&context).await;
    }

    let task_manager: FuturesUnordered<Eff> = FuturesUnordered::new();

    task_manager.push(rx.for_each_concurrent(None, |eff| eff).boxed());

    for lane_task in tasks.into_iter() {
        task_manager.push(lane_task.events(context.clone()));
    }

    task_manager
        .never_error()
        .forward(drain())
        .map(|_| ()) //Never is an empty type so we can discard the errors.
        .await
}

pub type Eff = BoxFuture<'static, ()>;
pub type EffStream = BoxStream<'static, ()>;

/// A context for a running instance of an agent. This provides access to the agent instance in
/// agent and lane life-cycle events and allows events to be scheduled within the task that
/// is running the agent.
pub trait AgentContext<Agent> {
    /// Schedule events to be executed on a provided schedule. The events will be executed within
    /// the task that runs the agent and so should not block.
    ///
    /// #Type Parameters
    ///
    /// * `Effect` - The type of the events to schedule.
    /// * `Str` - The type of the stream of events.
    /// * `Sch` - The type of the stream of [`Duration`]s defining the schedule.
    ///
    /// #Arguments
    ///
    /// * `effects` - A stream of events to be executed.
    /// * `schedule` - A stream of [`Duration`]s describing the schedule on which the effects
    /// should be run.
    fn schedule<Effect, Str, Sch>(&self, effects: Str, schedule: Sch) -> BoxFuture<()>
    where
        Effect: Future<Output = ()> + Send + 'static,
        Str: Stream<Item = Effect> + Send + 'static,
        Sch: Stream<Item = Duration> + Send + 'static;

    /// Schedule an event to be run on a fixed schedule.
    ///
    /// #Type Parameters
    ///
    /// * `Fut` - Type of the event to schedule.
    /// * `F` - Event factory closure type.
    ///
    /// #Arguments
    ///
    /// * `effect` - Factory closure to generate the events.
    /// * `interval` - The fixed interval on which to generate the events.
    /// * `max_periods` - The maximum number of times that the effect will run. `Some(0)` is
    /// treated as `None`.
    fn periodically<Fut, F>(
        &self,
        mut effect: F,
        interval: Duration,
        max_periods: Option<usize>,
    ) -> BoxFuture<()>
    where
        Fut: Future<Output = ()> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
    {
        let sch = repeat(interval);
        match max_periods {
            Some(n) if n > 0 => {
                let effects = unfold(0, move |i| {
                    if i < n {
                        ready(Some((effect(), i + 1)))
                    } else {
                        ready(None)
                    }
                });
                self.schedule(effects, sch)
            }
            _ => {
                let effects = unfold((), move |_| ready(Some((effect(), ()))));
                self.schedule(effects, sch)
            }
        }
    }

    /// Schedule a single event to run after a fixed delay.
    ///
    /// #Type Parameters
    ///
    /// * `Fut` - Type of the event to schedule.
    ///
    /// #Arguments
    ///
    /// * `effect` - The single event.
    /// * `duration` - The delay before executing the event.
    fn defer<Fut>(&self, effect: Fut, duration: Duration) -> BoxFuture<()>
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.schedule(once(ready(effect)), once(ready(duration)))
    }

    /// Access the agent instance.
    fn agent(&self) -> &Agent;

    /// Get the node URL of the agent instance.
    fn node_url(&self) -> &Url;
}

/// Provides an abstraction over the different types of lane to allow the lane life-cycles to be
/// managed uniformly by the agent. Eventually this trait will be made private and instances will
/// be generated by the derive macro for [`SwimAgent`].
///
/// TODO Make this trait private after the derive macro has been written for SwimAgent.
pub trait LaneTasks<Agent, Context: AgentContext<Agent> + Sized + Send + Sync + 'static> {
    /// Perform any required work for the lane when the agent starts.
    fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()>;

    /// Handle the stream of events produced by the lane.
    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()>;
}

struct LifecycleTasks<L, S, P> {
    lifecycle: L,
    event_stream: S,
    projection: P,
}

struct ValueLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);
struct MapLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);
struct ActionLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);
struct CommandLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);

impl<Agent, Context, T, L, S, P> LaneTasks<Agent, Context> for ValueLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Arc<T>> + Send + Sync + 'static,
    T: Any + Send + Sync,
    L: for<'l> StatefulLaneLifecycle<'l, ValueLane<T>, Agent>,
    L::WatchStrategy: ValueLaneWatch<T, View = S>,
    P: Fn(&Agent) -> &ValueLane<T> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
        let ValueLifecycleTasks(LifecycleTasks {
            lifecycle,
            projection,
            ..
        }) = self;
        let model = projection(context.agent());
        lifecycle.on_start(model, context).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let ValueLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
            }) = *self;
            let model = projection(context.agent());
            pin_mut!(event_stream);
            while let Some(event) = event_stream.next().await {
                lifecycle.on_event(&event, &model, &context).await
            }
        }
        .boxed()
    }
}

/// Create a value lane instance along with its life-cycle.
///
/// #Arguments
///
/// * `init` - The initial value of the lane.
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
pub fn make_value_lane<Agent, Context, T, L>(
    init: T,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &ValueLane<T> + Send + Sync + 'static,
) -> (ValueLane<T>, impl LaneTasks<Agent, Context>)
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    T: Any + Send + Sync,
    L: for<'l> StatefulLaneLifecycle<'l, ValueLane<T>, Agent>,
    L::WatchStrategy: ValueLaneWatch<T>,
{
    let (lane, event_stream) = model::value::make_lane_model(init, lifecycle.create_strategy());

    let tasks = ValueLifecycleTasks(LifecycleTasks {
        lifecycle,
        event_stream,
        projection,
    });
    (lane, tasks)
}

impl<Agent, Context, K, V, L, S, P> LaneTasks<Agent, Context> for MapLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = MapLaneEvent<K, V>> + Send + Sync + 'static,
    K: Any + Form + Send + Sync,
    V: Any + Send + Sync,
    L: for<'l> StatefulLaneLifecycle<'l, MapLane<K, V>, Agent>,
    L::WatchStrategy: MapLaneWatch<K, V, View = S>,
    P: Fn(&Agent) -> &MapLane<K, V> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
        let MapLifecycleTasks(LifecycleTasks {
            lifecycle,
            projection,
            ..
        }) = self;
        let model = projection(context.agent());
        lifecycle.on_start(model, context).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let MapLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
            }) = *self;
            let model = projection(context.agent()).clone();
            pin_mut!(event_stream);
            while let Some(event) = event_stream.next().await {
                lifecycle.on_event(&event, &model, &context).await
            }
        }
        .boxed()
    }
}

/// Create a map lane instance along with its life-cycle.
///
/// #Arguments
///
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
pub fn make_map_lane<Agent, Context, K, V, L, P>(
    lifecycle: L,
    projection: impl Fn(&Agent) -> &MapLane<K, V> + Send + Sync + 'static,
) -> (MapLane<K, V>, impl LaneTasks<Agent, Context>)
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    K: Any + Form + Send + Sync,
    V: Any + Send + Sync,
    L: for<'l> StatefulLaneLifecycle<'l, MapLane<K, V>, Agent>,
    L::WatchStrategy: MapLaneWatch<K, V>,
    P: Fn(&Agent) -> &MapLane<K, V>,
{
    let (lane, event_stream) = model::map::make_lane_model(lifecycle.create_strategy());

    let tasks = MapLifecycleTasks(LifecycleTasks {
        lifecycle,
        event_stream,
        projection,
    });
    (lane, tasks)
}

impl<Agent, Context, Command, Response, L, S, P> LaneTasks<Agent, Context>
    for ActionLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Command> + Send + Sync + 'static,
    Command: Any + Send + Sync,
    Response: Any + Send + Sync,
    L: for<'l> ActionLaneLifecycle<'l, Command, Response, Agent>,
    P: Fn(&Agent) -> &ActionLane<Command, Response> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let ActionLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
            }) = *self;
            let model = projection(context.agent()).clone();
            pin_mut!(event_stream);
            while let Some(command) = event_stream.next().await {
                //TODO After agents are connected to web-sockets the response will have somewhere to go.
                let _response = lifecycle.on_command(&command, &model, &context).await;
            }
        }
        .boxed()
    }
}

impl<Agent, Context, Command, L, S, P> LaneTasks<Agent, Context> for CommandLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Command> + Send + Sync + 'static,
    Command: Any + Send + Sync,
    L: for<'l> ActionLaneLifecycle<'l, Command, (), Agent>,
    P: Fn(&Agent) -> &CommandLane<Command> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let CommandLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
            }) = *self;
            let model = projection(context.agent()).clone();
            pin_mut!(event_stream);
            while let Some(command) = event_stream.next().await {
                lifecycle.on_command(&command, &model, &context).await;
            }
        }
        .boxed()
    }
}

/// Create an action lane from a lifecycle.
///
/// #Arguments
///
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
/// * `buffer_size` - Buffer size for the MPSC channel accepting the commands.
pub fn make_action_lane<Agent, Context, Command, Response, L, S, P>(
    lifecycle: L,
    projection: impl Fn(&Agent) -> &ActionLane<Command, Response> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
) -> (
    ActionLane<Command, Response>,
    impl LaneTasks<Agent, Context>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Command> + Send + Sync + 'static,
    Command: Any + Send + Sync,
    Response: Any + Send + Sync,
    L: for<'l> ActionLaneLifecycle<'l, Command, Response, Agent>,
    P: Fn(&Agent) -> &ActionLane<Command, Response> + Send + Sync + 'static,
{
    let (lane, event_stream) = model::action::make_lane_model(buffer_size);

    let tasks = ActionLifecycleTasks(LifecycleTasks {
        lifecycle,
        event_stream,
        projection,
    });
    (lane, tasks)
}

/// Create a command lane from a lifecycle.
///
/// #Arguments
///
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
/// * `buffer_size` - Buffer size for the MPSC channel accepting the commands.
pub fn make_command_lane<Agent, Context, Command, L, S, P>(
    lifecycle: L,
    projection: impl Fn(&Agent) -> &CommandLane<Command> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
) -> (CommandLane<Command>, impl LaneTasks<Agent, Context>)
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Command> + Send + Sync + 'static,
    Command: Any + Send + Sync,
    L: for<'l> ActionLaneLifecycle<'l, Command, (), Agent>,
    P: Fn(&Agent) -> &CommandLane<Command> + Send + Sync + 'static,
{
    let (lane, event_stream) = model::action::make_lane_model(buffer_size);

    let tasks = CommandLifecycleTasks(LifecycleTasks {
        lifecycle,
        event_stream,
        projection,
    });
    (lane, tasks)
}
