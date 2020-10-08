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

pub(crate) mod context;
pub mod dispatch;
pub mod lane;
pub mod lifecycle;
#[cfg(test)]
mod tests;

use crate::agent::context::{AgentExecutionContext, ContextImpl};
use crate::agent::dispatch::error::DispatcherErrors;
use crate::agent::dispatch::AgentDispatcher;
use crate::agent::lane::channels::task::{run_supply_lane_io, LaneIoError, MapLaneMessageHandler};
use crate::agent::lane::channels::update::StmRetryStrategy;
use crate::agent::lane::channels::uplink::spawn::{SpawnerUplinkFactory, UplinkErrorReport};
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::lifecycle::{
    ActionLaneLifecycle, DemandLaneLifecycle, StatefulLaneLifecycle,
};
use crate::agent::lane::model;
use crate::agent::lane::model::action::{Action, ActionLane, CommandLane};
use crate::agent::lane::model::demand::DemandLane;
use crate::agent::lane::model::map::MapLaneEvent;
use crate::agent::lane::model::map::{MapLane, MapLaneWatch};
use crate::agent::lane::model::supply::{make_lane_model, SupplyLane};
use crate::agent::lane::model::value::{ValueLane, ValueLaneWatch};
use crate::agent::lane::model::DeferredLaneView;
use crate::agent::lifecycle::AgentLifecycle;
use crate::routing::{ServerRouter, TaggedClientEnvelope, TaggedEnvelope};
use futures::future::{ready, BoxFuture};
use futures::sink::drain;
use futures::stream::{once, repeat, unfold, BoxStream, FuturesUnordered};
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use swim_runtime::time::clock::Clock;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tracing::{event, span, Level};
use tracing_futures::{Instrument, Instrumented};
use utilities::future::SwimStreamExt;
use utilities::sync::trigger;

/// Trait that must be implemented for any agent. This is essentially just boilerplate and will
/// eventually be implemented using a derive macro.
/// TODO Write derive macro for SwimAgent.
pub trait SwimAgent<Config>: Any + Send + Sync + Sized {
    /// Create an instance of the agent and life-cycle handles for each of its lanes.
    fn instantiate<Context>(
        configuration: &Config,
    ) -> (
        Self,
        DynamicLaneTasks<Self, Context>,
        DynamicAgentIo<Context>,
    )
    where
        Context: AgentContext<Self> + AgentExecutionContext + Send + Sync + 'static;
}

pub type DynamicLaneTasks<Agent, Context> = Vec<Box<dyn LaneTasks<Agent, Context>>>;
pub type DynamicAgentIo<Context> = HashMap<String, Box<dyn LaneIo<Context>>>;

const AGENT_TASK: &str = "Agent task";
const AGENT_START: &str = "Agent start";
const LANE_START: &str = "Lane start";
const SCHEDULER_TASK: &str = "Agent scheduler";
const ROOT_DISPATCHER_TASK: &str = "Agent envelope dispatcher.";
const LANE_EVENTS: &str = "Lane events";
const ON_EVENT: &str = "On event handler";
const COMMANDED: &str = "Command received";
const ON_COMMAND: &str = "On command handler";
const ACTION_RESULT: &str = "Action result";
const RESPONSE_IGNORED: &str = "Response requested from action lane but ignored.";

#[derive(Debug)]
pub struct AgentResult {
    pub route: String,
    pub dispatcher_errors: DispatcherErrors,
    pub failed: bool,
}

impl AgentResult {
    fn from(
        route: String,
        result: Result<Result<DispatcherErrors, DispatcherErrors>, oneshot::error::RecvError>,
    ) -> Self {
        let (errs, failed) = match result {
            Ok(Ok(errs)) => (errs, false),
            Ok(Err(errs)) => (errs, true),
            _ => (Default::default(), true),
        };
        AgentResult {
            route,
            dispatcher_errors: errs,
            failed,
        }
    }
}

#[derive(Debug)]
pub struct AgentParameters<Config> {
    agent_config: Config,
    execution_config: AgentExecutionConfig,
    uri: String,
    parameters: HashMap<String, String>,
}

impl<Config> AgentParameters<Config> {
    pub fn new(
        agent_config: Config,
        execution_config: AgentExecutionConfig,
        uri: String,
        parameters: HashMap<String, String>,
    ) -> Self {
        AgentParameters {
            agent_config,
            execution_config,
            uri,
            parameters,
        }
    }
}

/// Creates a single, asynchronous task that manages the lifecycle of an agent, all of its lanes
/// and any events that are scheduled within it.
///
/// #Arguments
///
/// * `lifecycle` - Life-cycle event handler for the agent.
/// * `url` - The node URL for the agent instance.
/// * `clock` - Clock for timing asynchronous events.
/// * `stop_trigger` - External trigger to cleanly stop the agent.
/// * `parameters` - Parameters extracted from the agent node route pattern.
/// * `incoming_envelopes` - The stream of envelopes routed to the agent.
pub fn run_agent<Config, Clk, Agent, L, Router>(
    lifecycle: L,
    clock: Clk,
    parameters: AgentParameters<Config>,
    incoming_envelopes: impl Stream<Item = TaggedEnvelope> + Send + 'static,
    router: Router,
) -> (
    Arc<Agent>,
    impl Future<Output = AgentResult> + Send + 'static,
)
where
    Clk: Clock,
    Agent: SwimAgent<Config> + Send + Sync + 'static,
    L: AgentLifecycle<Agent> + Send + Sync + 'static,
    Router: ServerRouter + Clone + 'static,
{
    let AgentParameters {
        agent_config,
        execution_config,
        uri,
        parameters,
    } = parameters;

    let span = span!(Level::INFO, AGENT_TASK, %uri);
    let (tripwire, stop_trigger) = trigger::trigger();
    let (agent, tasks, io_providers) =
        Agent::instantiate::<ContextImpl<Agent, Clk, Router>>(&agent_config);
    let agent_ref = Arc::new(agent);
    let agent_cpy = agent_ref.clone();
    let task = async move {
        let (tx, rx) = mpsc::channel(execution_config.scheduler_buffer.get());
        let context = ContextImpl::new(
            agent_ref,
            uri.clone(),
            tx,
            clock,
            stop_trigger.clone(),
            router,
            parameters,
        );

        lifecycle
            .on_start(&context)
            .instrument(span!(Level::DEBUG, AGENT_START))
            .await;

        for lane_task in tasks.iter() {
            let lane_name = lane_task.name();
            (**lane_task)
                .start(&context)
                .instrument(span!(Level::DEBUG, LANE_START, name = lane_name))
                .await;
        }

        let task_manager: FuturesUnordered<Instrumented<Eff>> = FuturesUnordered::new();

        let scheduler_task = rx
            .take_until(stop_trigger)
            .for_each_concurrent(None, |eff| eff)
            .boxed()
            .instrument(span!(Level::TRACE, SCHEDULER_TASK));
        task_manager.push(scheduler_task);

        for lane_task in tasks.into_iter() {
            let lane_name = lane_task.name().to_string();
            task_manager.push(
                lane_task
                    .events(context.clone())
                    .instrument(span!(Level::DEBUG, LANE_EVENTS, name = %lane_name)),
            );
        }

        let dispatcher =
            AgentDispatcher::new(uri.clone(), execution_config, context.clone(), io_providers);

        let (result_tx, result_rx) = oneshot::channel();

        let dispatch_task = async move {
            let tripwire = tripwire;
            let result = dispatcher.run(incoming_envelopes).await;
            tripwire.trigger();
            let _ = result_tx.send(result);
        }
        .boxed()
        .instrument(span!(Level::INFO, ROOT_DISPATCHER_TASK));
        task_manager.push(dispatch_task);

        drop(context);

        task_manager
            .never_error()
            .forward(drain())
            .map(|_| ()) //Never is an empty type so we can discard the errors.
            .await;

        AgentResult::from(uri, result_rx.await)
    }
    .instrument(span);
    (agent_cpy, task)
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

    /// Get the node URI of the agent instance.
    fn node_uri(&self) -> &str;

    /// Get a future that will complete when the agent is stopping.
    fn agent_stop_event(&self) -> trigger::Receiver;

    /// Get the value of a parameter extracted from the agent node route.
    fn parameter(&self, key: &str) -> Option<&String>;

    /// Get a copy of all parameters extracted from the agent node route.
    fn parameters(&self) -> HashMap<String, String>;
}

pub trait Lane {
    /// The name of the lane.
    fn name(&self) -> &str;
}

/// Provides an abstraction over the different types of lane to allow the lane life-cycles to be
/// managed uniformly by the agent. Eventually this trait will be made private and instances will
/// be generated by the derive macro for [`SwimAgent`].
///
/// TODO Make this trait private after the derive macro has been written for SwimAgent.
pub trait LaneTasks<Agent, Context: AgentContext<Agent> + Sized + Send + Sync + 'static>:
    Lane + Send + Sync
{
    /// Perform any required work for the lane when the agent starts.
    fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()>;

    /// Handle the stream of events produced by the lane.
    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()>;

    fn boxed<'a>(self) -> Box<dyn LaneTasks<Agent, Context> + 'a>
    where
        Self: Sized + 'a,
    {
        Box::new(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachError {
    /// Could not attach to the lane as the agent hosting it is stopping.
    AgentStopping,
    /// The lane stopped reporting its state changes.
    LaneStoppedReporting,
    /// Failed to attach to the lane because it does not exist.
    LaneDoesNotExist(String),
}

impl Display for AttachError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AttachError::LaneStoppedReporting => write!(
                f,
                "Failed to attach as the lane stopped reporting its state."
            ),
            AttachError::LaneDoesNotExist(name) => {
                write!(f, "A lane named \"{}\" does not exist.", name)
            }
            AttachError::AgentStopping => {
                write!(f, "Could not attach to the lane as the agent is stoping.")
            }
        }
    }
}

impl Error for AttachError {}

/// Lazily initialized envelope IO for a lane.
pub trait LaneIo<Context: AgentExecutionContext + Sized + Send + Sync + 'static>:
    Send + Sync
{
    /// Attempt to attach the running lane to a stream of envelopes.
    fn attach(
        self,
        route: RelativePath,
        envelopes: mpsc::Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError>;

    fn attach_boxed(
        self: Box<Self>,
        route: RelativePath,
        envelopes: mpsc::Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError>;

    fn boxed(self) -> Box<dyn LaneIo<Context>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

struct ValueLaneIo<T, D> {
    lane: ValueLane<T>,
    deferred: D,
}

impl<T, D> ValueLaneIo<T, D>
where
    T: Any + Send + Sync + Form + Debug,
    D: DeferredLaneView<Arc<T>>,
{
    fn new(lane: ValueLane<T>, deferred: D) -> Self {
        ValueLaneIo { lane, deferred }
    }
}

impl<T, Context, D> LaneIo<Context> for ValueLaneIo<T, D>
where
    T: Any + Send + Sync + Form + Debug,
    D: DeferredLaneView<Arc<T>>,
    Context: AgentExecutionContext + Sized + Send + Sync + 'static,
{
    fn attach(
        self,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let ValueLaneIo { lane, deferred } = self;
        let uplink_factory = SpawnerUplinkFactory::new(config.clone());
        let topic = deferred.attach()?;
        Ok(lane::channels::task::run_lane_io(
            lane,
            uplink_factory,
            envelopes,
            topic,
            config,
            context,
            route,
        )
        .boxed())
    }

    fn attach_boxed(
        self: Box<Self>,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        (*self).attach(route, envelopes, config, context)
    }
}

struct MapLaneIo<K, V, D> {
    lane: MapLane<K, V>,
    deferred: D,
}

impl<K, V, D> MapLaneIo<K, V, D>
where
    K: Any + Send + Sync + Form + Clone + Debug,
    V: Any + Send + Sync + Form + Debug,
    D: DeferredLaneView<MapLaneEvent<K, V>>,
{
    fn new(lane: MapLane<K, V>, deferred: D) -> Self {
        MapLaneIo { lane, deferred }
    }
}

impl<K, V, Context, D> LaneIo<Context> for MapLaneIo<K, V, D>
where
    K: Any + Send + Sync + Form + Clone + Debug,
    V: Any + Send + Sync + Form + Debug,
    Context: AgentExecutionContext + Sized + Send + Sync + 'static,
    D: DeferredLaneView<MapLaneEvent<K, V>>,
{
    fn attach(
        self,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let MapLaneIo { lane, deferred } = self;

        let uplink_factory = SpawnerUplinkFactory::new(config.clone());

        let topic = deferred.attach()?;

        let retries = StmRetryStrategy::new(config.retry_strategy);

        let handler = MapLaneMessageHandler::new(lane, move || retries);

        Ok(lane::channels::task::run_lane_io(
            handler,
            uplink_factory,
            envelopes,
            topic,
            config,
            context,
            route,
        )
        .boxed())
    }

    fn attach_boxed(
        self: Box<Self>,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        (*self).attach(route, envelopes, config, context)
    }
}

struct ActionLaneIo<Command, Response> {
    lane: ActionLane<Command, Response>,
    feedback: bool,
}

impl<Command, Response> ActionLaneIo<Command, Response>
where
    Command: Send + Sync + Form + Debug + 'static,
    Response: Send + Sync + Form + Debug + 'static,
{
    fn new_action(lane: ActionLane<Command, Response>) -> Self {
        ActionLaneIo {
            lane,
            feedback: true,
        }
    }

    fn new_command(lane: ActionLane<Command, Response>) -> Self {
        ActionLaneIo {
            lane,
            feedback: false,
        }
    }
}

impl<Command, Response, Context> LaneIo<Context> for ActionLaneIo<Command, Response>
where
    Command: Send + Sync + Form + Debug + 'static,
    Response: Send + Sync + Form + Debug + 'static,
    Context: AgentExecutionContext + Sized + Send + Sync + 'static,
{
    fn attach(
        self,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let ActionLaneIo { lane, feedback } = self;

        Ok(lane::channels::task::run_action_lane_io(
            lane, feedback, envelopes, config, context, route,
        )
        .boxed())
    }

    fn attach_boxed(
        self: Box<Self>,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        (*self).attach(route, envelopes, config, context)
    }
}

struct LifecycleTasks<L, S, P> {
    name: String,
    lifecycle: L,
    event_stream: S,
    projection: P,
}

struct ValueLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);
struct MapLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);
struct ActionLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);
struct CommandLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);
struct DemandLifecycleTasks<L, S, P, Value> {
    tasks: LifecycleTasks<L, S, P>,
    response_tx: mpsc::Sender<Value>,
}

struct StatelessLifecycleTasks {
    name: String,
}

impl<L, S, P> Lane for ValueLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }
}

impl<Agent, Context, T, L, S, P> LaneTasks<Agent, Context> for ValueLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Arc<T>> + Send + Sync + 'static,
    T: Any + Send + Sync + Debug,
    L: for<'l> StatefulLaneLifecycle<'l, ValueLane<T>, Agent>,
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
                ..
            }) = *self;
            let model = projection(context.agent());
            let events = event_stream.take_until(context.agent_stop_event());
            pin_mut!(events);
            while let Some(event) = events.next().await {
                lifecycle
                    .on_event(&event, &model, &context)
                    .instrument(span!(Level::TRACE, ON_EVENT, ?event))
                    .await
            }
        }
        .boxed()
    }
}

/// Create a value lane instance along with its life-cycle.
///
/// #Arguments
///
/// * `name` - The name of the lane.
/// * `is_public` - Whether the lane is public (with respect to external message routing).
/// * `config` - Configuration parameters.
/// * `init` - The initial value of the lane.
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
pub fn make_value_lane<Agent, Context, T, L>(
    name: impl Into<String>,
    is_public: bool,
    config: &AgentExecutionConfig,
    init: T,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &ValueLane<T> + Send + Sync + 'static,
) -> (
    ValueLane<T>,
    impl LaneTasks<Agent, Context>,
    Option<impl LaneIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    T: Any + Send + Sync + Form + Debug,
    L: for<'l> StatefulLaneLifecycle<'l, ValueLane<T>, Agent>,
    L::WatchStrategy: ValueLaneWatch<T>,
{
    let (lane, event_stream, deferred) = if is_public {
        let (lane, event_stream, deferred) =
            model::value::make_lane_model_deferred(init, lifecycle.create_strategy(), config);

        (lane, event_stream, Some(deferred))
    } else {
        let (lane, event_stream) = model::value::make_lane_model(init, lifecycle.create_strategy());

        (lane, event_stream, None)
    };
    let tasks = ValueLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream,
        projection,
    });
    let lane_io = deferred.map(|d| ValueLaneIo::new(lane.clone(), d));
    (lane, tasks, lane_io)
}

impl<L, S, P> Lane for MapLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }
}

impl<Agent, Context, K, V, L, S, P> LaneTasks<Agent, Context> for MapLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = MapLaneEvent<K, V>> + Send + Sync + 'static,
    K: Any + Form + Send + Sync + Debug,
    V: Any + Send + Sync + Debug,
    L: for<'l> StatefulLaneLifecycle<'l, MapLane<K, V>, Agent>,
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

    fn events(self: Box<Self>, context: Context) -> Eff {
        async move {
            let MapLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
                ..
            }) = *self;
            let model = projection(context.agent()).clone();
            let events = event_stream.take_until(context.agent_stop_event());
            pin_mut!(events);
            while let Some(event) = events.next().await {
                lifecycle
                    .on_event(&event, &model, &context)
                    .instrument(span!(Level::TRACE, ON_EVENT, ?event))
                    .await
            }
        }
        .boxed()
    }
}

/// Create a map lane instance along with its life-cycle.
///
/// #Arguments
///
/// * `name` - The name of the lane.
/// * `is_public` - Whether the lane is public (with respect to external message routing).
/// * `config` - Configuration parameters.
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
pub fn make_map_lane<Agent, Context, K, V, L>(
    name: impl Into<String>,
    is_public: bool,
    config: &AgentExecutionConfig,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &MapLane<K, V> + Send + Sync + 'static,
) -> (
    MapLane<K, V>,
    impl LaneTasks<Agent, Context>,
    Option<impl LaneIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    K: Any + Form + Send + Sync + Clone + Debug,
    V: Any + Form + Send + Sync + Debug,
    L: for<'l> StatefulLaneLifecycle<'l, MapLane<K, V>, Agent>,
    L::WatchStrategy: MapLaneWatch<K, V>,
{
    let (lane, event_stream, deferred) = if is_public {
        let (lane, event_stream, deferred) =
            model::map::make_lane_model_deferred(lifecycle.create_strategy(), config);
        (lane, event_stream, Some(deferred))
    } else {
        let (lane, event_stream) = model::map::make_lane_model(lifecycle.create_strategy());
        (lane, event_stream, None)
    };

    let tasks = MapLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream,
        projection,
    });

    let lane_io = deferred.map(|d| MapLaneIo::new(lane.clone(), d));
    (lane, tasks, lane_io)
}

impl<L, S, P> Lane for ActionLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }
}

impl<Agent, Context, Command, Response, L, S, P> LaneTasks<Agent, Context>
    for ActionLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Action<Command, Response>> + Send + Sync + 'static,
    Command: Any + Send + Sync + Debug,
    Response: Any + Send + Sync + Debug,
    L: for<'l> ActionLaneLifecycle<'l, Command, Response, Agent>,
    P: Fn(&Agent) -> &ActionLane<Command, Response> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> Eff {
        async move {
            let ActionLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
                ..
            }) = *self;
            let model = projection(context.agent()).clone();
            let events = event_stream.take_until(context.agent_stop_event());
            pin_mut!(events);
            while let Some(Action { command, responder }) = events.next().await {
                event!(Level::TRACE, COMMANDED, ?command);
                //TODO After agents are connected to web-sockets the response will have somewhere to go.
                let response = lifecycle
                    .on_command(command, &model, &context)
                    .instrument(span!(Level::TRACE, ON_COMMAND))
                    .await;
                event!(Level::TRACE, ACTION_RESULT, ?response);
                if let Some(tx) = responder {
                    if tx.send(response).is_err() {
                        event!(Level::WARN, RESPONSE_IGNORED);
                    }
                }
            }
        }
        .boxed()
    }
}

impl<L, S, P> Lane for CommandLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }
}

impl<Agent, Context, Command, L, S, P> LaneTasks<Agent, Context> for CommandLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Action<Command, ()>> + Send + Sync + 'static,
    Command: Any + Send + Sync + Debug,
    L: for<'l> ActionLaneLifecycle<'l, Command, (), Agent>,
    P: Fn(&Agent) -> &CommandLane<Command> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> Eff {
        async move {
            let CommandLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
                ..
            }) = *self;
            let model = projection(context.agent()).clone();
            let events = event_stream.take_until(context.agent_stop_event());
            pin_mut!(events);
            while let Some(Action { command, responder }) = events.next().await {
                event!(Level::TRACE, COMMANDED, ?command);
                lifecycle
                    .on_command(command, &model, &context)
                    .instrument(span!(Level::TRACE, ON_COMMAND))
                    .await;
                if let Some(tx) = responder {
                    if tx.send(()).is_err() {
                        event!(Level::WARN, RESPONSE_IGNORED);
                    }
                }
            }
        }
        .boxed()
    }
}

/// Create an action lane from a lifecycle.
///
/// #Arguments
///
/// * `name`- The name of the lane.
/// * `is_public` - Whether the lane is public (with respect to external message routing).
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
/// * `buffer_size` - Buffer size for the MPSC channel accepting the commands.
pub fn make_action_lane<Agent, Context, Command, Response, L, S, P>(
    name: impl Into<String>,
    is_public: bool,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &ActionLane<Command, Response> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
) -> (
    ActionLane<Command, Response>,
    impl LaneTasks<Agent, Context>,
    Option<impl LaneIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    S: Stream<Item = Command> + Send + Sync + 'static,
    Command: Any + Send + Sync + Form + Debug,
    Response: Any + Send + Sync + Form + Debug,
    L: for<'l> ActionLaneLifecycle<'l, Command, Response, Agent>,
    P: Fn(&Agent) -> &ActionLane<Command, Response> + AgentExecutionContext + Send + Sync + 'static,
{
    let (lane, event_stream) = model::action::make_lane_model(buffer_size);

    let tasks = ActionLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream,
        projection,
    });

    let lane_io = if is_public {
        Some(ActionLaneIo::new_action(lane.clone()))
    } else {
        None
    };
    (lane, tasks, lane_io)
}

/// Create a command lane from a lifecycle.
///
/// #Arguments
///
/// * `name` - The name of the lane.
/// * `is_public` - Whether the lane is public (with respect to external message routing).
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
/// * `buffer_size` - Buffer size for the MPSC channel accepting the commands.
pub fn make_command_lane<Agent, Context, Command, L>(
    name: impl Into<String>,
    is_public: bool,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &CommandLane<Command> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
) -> (
    CommandLane<Command>,
    impl LaneTasks<Agent, Context>,
    Option<impl LaneIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    Command: Any + Send + Sync + Form + Debug,
    L: for<'l> ActionLaneLifecycle<'l, Command, (), Agent>,
{
    let (lane, event_stream) = model::action::make_lane_model(buffer_size);

    let tasks = CommandLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream,
        projection,
    });

    let lane_io = if is_public {
        Some(ActionLaneIo::new_command(lane.clone()))
    } else {
        None
    };

    (lane, tasks, lane_io)
}

/// Create a new supply lane.
///
/// # Arguments
///
/// * `name` - The name of the lane.
/// * `is_public` - Whether the lane is public (with respect to external message routing).
/// * `buffer_size` - Buffer size for the MPSC channel accepting the events.
pub fn make_supply_lane<Agent, Context, T>(
    name: impl Into<String>,
    is_public: bool,
    buffer_size: NonZeroUsize,
) -> (
    SupplyLane<T>,
    impl LaneTasks<Agent, Context>,
    Option<impl LaneIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    T: Any + Clone + Send + Sync + Form + Debug,
{
    let (lane, event_stream) = make_lane_model(buffer_size);

    let tasks = StatelessLifecycleTasks { name: name.into() };

    let lane_io = if is_public {
        Some(SupplyLaneIo::new(event_stream))
    } else {
        None
    };

    (lane, tasks, lane_io)
}

struct SupplyLaneIo<S> {
    stream: S,
}

impl<S> SupplyLaneIo<S> {
    fn new(stream: S) -> Self {
        SupplyLaneIo { stream }
    }
}

impl<S, Item, Context> LaneIo<Context> for SupplyLaneIo<S>
where
    S: Stream<Item = Item> + Send + Sync + 'static,
    Item: Send + Sync + Form + Debug + 'static,
    Context: AgentExecutionContext + Sized + Send + Sync + 'static,
{
    fn attach(
        self,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let SupplyLaneIo { stream } = self;

        Ok(run_supply_lane_io(envelopes, config, context, route, stream).boxed())
    }

    fn attach_boxed(
        self: Box<Self>,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        (*self).attach(route, envelopes, config, context)
    }
}

/// Create a new demand lane.
///
/// # Arguments
///
/// * `name` - The name of the lane.
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
/// * `buffer_size` - Buffer size for the MPSC channel accepting the commands.
pub fn make_demand_lane<Agent, Context, Value, L>(
    name: impl Into<String>,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &DemandLane<Value> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
) -> (
    DemandLane<Value>,
    impl LaneTasks<Agent, Context>,
    impl LaneIo<Context>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    Value: Any + Send + Sync + Form + Debug,
    L: for<'l> DemandLaneLifecycle<'l, Value, Agent>,
{
    let (lane, cue_stream) = model::demand::make_lane_model(buffer_size);
    let (response_tx, response_rx) = mpsc::channel(buffer_size.get());

    let tasks = DemandLifecycleTasks {
        tasks: LifecycleTasks {
            name: name.into(),
            lifecycle,
            event_stream: cue_stream,
            projection,
        },
        response_tx,
    };

    let lane_io = DemandLaneIo::new(response_rx);

    (lane, tasks, lane_io)
}

struct DemandLaneIo<Value> {
    response_rx: mpsc::Receiver<Value>,
}

impl<Value> DemandLaneIo<Value>
where
    Value: Send + Sync + 'static,
{
    fn new(response_rx: mpsc::Receiver<Value>) -> DemandLaneIo<Value> {
        DemandLaneIo { response_rx }
    }
}

impl<Value, Context> LaneIo<Context> for DemandLaneIo<Value>
where
    Value: Form + Send + Sync + 'static,
    Context: AgentExecutionContext + Sized + Send + Sync + 'static,
{
    fn attach(
        self,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let DemandLaneIo { response_rx } = self;

        Ok(
            lane::channels::task::run_demand_lane_io(
                envelopes,
                config,
                context,
                route,
                response_rx,
            )
            .boxed(),
        )
    }

    fn attach_boxed(
        self: Box<Self>,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        (*self).attach(route, envelopes, config, context)
    }
}

impl<L, S, P, Value> Lane for DemandLifecycleTasks<L, S, P, Value> {
    fn name(&self) -> &str {
        self.tasks.name.as_str()
    }
}

impl<Agent, Context, L, S, P, Value> LaneTasks<Agent, Context>
    for DemandLifecycleTasks<L, S, P, Value>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = ()> + Send + Sync + 'static,
    Value: Any + Send + Sync + Debug,
    L: for<'l> DemandLaneLifecycle<'l, Value, Agent>,
    P: Fn(&Agent) -> &DemandLane<Value> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let DemandLifecycleTasks {
                tasks:
                    LifecycleTasks {
                        lifecycle,
                        event_stream,
                        projection,
                        ..
                    },
                mut response_tx,
            } = *self;

            let model = projection(context.agent()).clone();
            let events = event_stream.take_until(context.agent_stop_event());

            pin_mut!(events);

            while events.next().await.is_some() {
                if let Some(value) = lifecycle.on_cue(&model, &context).await {
                    let _ = response_tx.send(value).await;
                }
            }
        }
        .boxed()
    }
}
