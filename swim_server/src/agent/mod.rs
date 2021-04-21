// Copyright 2015-2021 SWIM.AI inc.
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

pub mod context;
pub mod dispatch;
pub mod lane;
pub mod lifecycle;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub use tests::test_clock::TestClock;

use crate::agent::context::{AgentExecutionContext, ContextImpl, RoutingContext, SchedulerContext};
use crate::agent::dispatch::error::DispatcherErrors;
use crate::agent::dispatch::{AgentDispatcher, LaneIdentifier};
use crate::agent::lane::channels::task::{
    run_supply_lane_io, DemandMapLaneMessageHandler, LaneIoError, MapLaneMessageHandler,
    ValueLaneMessageHandler,
};
use crate::agent::lane::channels::update::StmRetryStrategy;
use crate::agent::lane::channels::uplink::spawn::{SpawnerUplinkFactory, UplinkErrorReport};
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::lifecycle::{
    ActionLaneLifecycle, CommandLaneLifecycle, DemandLaneLifecycle, DemandMapLaneLifecycle,
    StatefulLaneLifecycle,
};
pub use crate::agent::lane::model;
use crate::agent::lane::model::action::{Action, ActionLane};
use crate::agent::lane::model::command::{Command, CommandLane};
use crate::agent::lane::model::demand::DemandLane;
use crate::agent::lane::model::demand_map::{
    DemandMapLane, DemandMapLaneCommand, DemandMapLaneEvent,
};
use crate::agent::lane::model::map::MapLane;
use crate::agent::lane::model::map::{summaries_to_events, MapLaneEvent, MapSubscriber};
use crate::agent::lane::model::supply::{make_lane_model, SupplyLane};
use crate::agent::lane::model::value::{ValueLane, ValueLaneEvent};
use crate::agent::lane::model::DeferredSubscription;
use crate::agent::lifecycle::AgentLifecycle;
use crate::routing::{ServerRouter, TaggedClientEnvelope, TaggedEnvelope};
use futures::future::{join, ready, BoxFuture};
use futures::sink::drain;
use futures::stream::iter;
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
use utilities::sync::{topic, trigger};
use utilities::uri::RelativeUri;

use crate::agent::lane::store::task::{NodeStoreErrors, NodeStoreTask};
use crate::agent::lane::store::LaneNoStore;
pub use crate::agent::lane::store::StoreIo;
use crate::agent::model::value::value_store::ValueLaneStoreIo;
use crate::meta::info::{LaneInfo, LaneKind};
use crate::meta::log::NodeLogger;
use crate::meta::open_meta_lanes;
#[doc(hidden)]
#[allow(unused_imports)]
pub use agent_derive::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use store::NodeStore;
use tokio_stream::wrappers::ReceiverStream;

/// Trait that must be implemented for any agent. This is essentially just boilerplate and will
/// eventually be implemented using a derive macro.
pub trait SwimAgent<Config>: Any + Send + Sync + Sized {
    /// Create an instance of the agent and life-cycle handles for each of its lanes.
    fn instantiate<Context, Store>(
        configuration: &Config,
        exec_conf: &AgentExecutionConfig,
        store: &Store,
    ) -> (
        Self,
        DynamicLaneTasks<Self, Context>,
        DynamicAgentIo<Context, Store>,
    )
    where
        Context: AgentContext<Self> + AgentExecutionContext + Send + Sync + 'static,
        Store: NodeStore;
}

pub type DynamicLaneTasks<Agent, Context> = Vec<Box<dyn LaneTasks<Agent, Context>>>;
pub type DynamicAgentIo<Context, Store> =
    HashMap<String, LaneIo<Box<dyn RoutingIo<Context>>, Box<dyn StoreIo<Store>>>>;

pub const COMMANDED: &str = "Command received";
pub const ON_COMMAND: &str = "On command handler";
pub const RESPONSE_IGNORED: &str = "Response requested from action lane but ignored.";
pub const ON_EVENT: &str = "On event handler";
pub const ACTION_RESULT: &str = "Action result";
const AGENT_TASK: &str = "Agent task";
const AGENT_START: &str = "Agent start";
const LANE_START: &str = "Lane start";
const SCHEDULER_TASK: &str = "Agent scheduler";
const ROOT_DISPATCHER_TASK: &str = "Agent envelope dispatcher.";
const STORE_TASK: &str = "Lane store task.";
const LANE_EVENTS: &str = "Lane events";

type TaskIoResult<Err> = Result<Result<Err, Err>, oneshot::error::RecvError>;
type DispatchTaskResult = TaskIoResult<DispatcherErrors>;
type StoreTaskResult = TaskIoResult<NodeStoreErrors>;

#[derive(Debug, Default)]
pub struct AgentTaskResult<Err: Debug + Default> {
    pub errors: Err,
    pub failed: bool,
}

#[derive(Debug)]
pub struct AgentResult {
    pub route: RelativeUri,
    pub dispatcher_task: AgentTaskResult<DispatcherErrors>,
    pub store_task: AgentTaskResult<NodeStoreErrors>,
}

impl AgentResult {
    fn result_for<E: Default + Debug>(result: TaskIoResult<E>) -> AgentTaskResult<E> {
        match result {
            Ok(Ok(errs)) => AgentTaskResult {
                errors: errs,
                failed: false,
            },
            Ok(Err(errs)) => AgentTaskResult {
                errors: errs,
                failed: true,
            },
            _ => AgentTaskResult {
                errors: E::default(),
                failed: true,
            },
        }
    }

    fn from(
        route: RelativeUri,
        dispatcher_result: DispatchTaskResult,
        store_result: StoreTaskResult,
    ) -> Self {
        let dispatcher_task = Self::result_for(dispatcher_result);
        let store_task = Self::result_for(store_result);

        AgentResult {
            route,
            dispatcher_task,
            store_task,
        }
    }
}

#[derive(Debug)]
pub struct AgentParameters<Config> {
    agent_config: Config,
    execution_config: AgentExecutionConfig,
    uri: RelativeUri,
    parameters: HashMap<String, String>,
}

impl<Config> AgentParameters<Config> {
    pub fn new(
        agent_config: Config,
        execution_config: AgentExecutionConfig,
        uri: RelativeUri,
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
pub fn run_agent<Config, Clk, Agent, L, Router, Store>(
    lifecycle: L,
    clock: Clk,
    parameters: AgentParameters<Config>,
    incoming_envelopes: impl Stream<Item = TaggedEnvelope> + Send + 'static,
    router: Router,
    store: Store,
) -> (
    Arc<Agent>,
    impl Future<Output = AgentResult> + Send + 'static,
)
where
    Clk: Clock,
    Agent: SwimAgent<Config> + Send + Sync + 'static,
    L: AgentLifecycle<Agent> + Send + Sync + 'static,
    Router: ServerRouter + Clone + 'static,
    Store: NodeStore + Send + Sync + Clone,
{
    let AgentParameters {
        agent_config,
        execution_config,
        uri,
        parameters,
    } = parameters;

    let span = span!(Level::INFO, AGENT_TASK, %uri);
    let (tripwire, stop_trigger) = trigger::trigger();
    let (agent, mut tasks, io_providers) = Agent::instantiate::<
        ContextImpl<Agent, Clk, Router, Store>,
        Store,
    >(&agent_config, &execution_config, &store);
    let agent_ref = Arc::new(agent);
    let agent_cpy = agent_ref.clone();

    let lane_summary = tasks
        .iter()
        .fold(HashMap::with_capacity(tasks.len()), |mut map, lane| {
            let lane_name = lane.name().to_string();
            let lane_info = LaneInfo::new(lane_name.clone(), lane.kind());

            map.insert(lane_name, lane_info);
            map
        });

    let task = async move {
        let task_manager: FuturesUnordered<Instrumented<Eff>> = FuturesUnordered::new();

        let (meta_context, mut meta_tasks, meta_io) =
            open_meta_lanes::<Config, Agent, ContextImpl<Agent, Clk, Router, Store>>(
                uri.clone(),
                &execution_config,
                lane_summary,
                stop_trigger.clone(),
                &task_manager,
            );

        tasks.append(&mut meta_tasks);

        let (tx, rx) = mpsc::channel(execution_config.scheduler_buffer.get());
        let routing_context = RoutingContext::new(uri.clone(), router, parameters);
        let schedule_context = SchedulerContext::new(tx, clock, stop_trigger.clone());
        let context = ContextImpl::new(
            agent_ref,
            routing_context,
            schedule_context,
            meta_context,
            store.clone(),
        );

        lifecycle
            .starting(&context)
            .instrument(span!(Level::DEBUG, AGENT_START))
            .await;

        for lane_task in tasks.iter() {
            let lane_name = lane_task.name();
            (**lane_task)
                .start(&context)
                .instrument(span!(Level::DEBUG, LANE_START, name = lane_name))
                .await;
        }

        let scheduler_task = ReceiverStream::new(rx)
            .take_until(stop_trigger.clone())
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

        let (mut routing_io, persistence_io) = io_providers.into_iter().fold(
            (HashMap::new(), HashMap::new()),
            |(mut routing_io, mut persistence_io), (lane_uri, lane_io)| {
                let LaneIo {
                    routing,
                    persistence,
                } = lane_io;

                if let Some(routing) = routing {
                    let ident = LaneIdentifier::agent(lane_uri.clone());
                    routing_io.insert(ident, routing);
                }
                persistence_io.insert(lane_uri, persistence);
                (routing_io, persistence_io)
            },
        );

        routing_io.extend(meta_io);

        let (store_result_tx, store_result_rx) = oneshot::channel();
        let max_store_errors = execution_config.max_store_errors;
        let store_task = async move {
            let task = NodeStoreTask::new(stop_trigger.clone(), store);
            let result = task.run(persistence_io, max_store_errors).await;
            let _ = store_result_tx.send(result);
        }
        .boxed()
        .instrument(span!(Level::INFO, STORE_TASK));
        task_manager.push(store_task);

        let dispatcher =
            AgentDispatcher::new(uri.clone(), execution_config, context.clone(), routing_io);

        let (dispatch_result_tx, dispatch_result_rx) = oneshot::channel();

        let dispatch_task = async move {
            let tripwire = tripwire;
            let result = dispatcher.run(incoming_envelopes).await;
            tripwire.trigger();
            let _ = dispatch_result_tx.send(result);
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

        AgentResult::from(uri, dispatch_result_rx.await, store_result_rx.await)
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
    fn node_uri(&self) -> &RelativeUri;

    /// Get a future that will complete when the agent is stopping.
    fn agent_stop_event(&self) -> trigger::Receiver;

    /// Get the value of a parameter extracted from the agent node route.
    fn parameter(&self, key: &str) -> Option<&String>;

    /// Get a copy of all parameters extracted from the agent node route.
    fn parameters(&self) -> HashMap<String, String>;

    /// Return a handle to the logger for this node.
    fn logger(&self) -> NodeLogger;
}

pub trait Lane {
    /// The name of the lane.
    fn name(&self) -> &str;

    /// The type of the lane.
    fn kind(&self) -> LaneKind;
}

/// Provides an abstraction over the different types of lane to allow the lane life-cycles to be
/// managed uniformly by the agent. Eventually this trait will be made private and instances will
/// be generated by the derive macro for [`SwimAgent`].
///
pub trait LaneTasks<Agent, Context: AgentContext<Agent> + Sized + Send + 'static>:
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
                write!(f, "Could not attach to the lane as the agent is stopping.")
            }
        }
    }
}

impl Error for AttachError {}

/// Lazily initialized envelope IO for a lane.
pub trait RoutingIo<Context: AgentExecutionContext + Sized + Send + Sync + 'static>:
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

    fn boxed(self) -> Box<dyn RoutingIo<Context>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub struct ValueLaneIo<T, D> {
    lane: ValueLane<T>,
    deferred: D,
}

impl<T, D> ValueLaneIo<T, D>
where
    T: Any + Send + Sync + Form + Debug,
    D: DeferredSubscription<Arc<T>>,
{
    pub fn new(lane: ValueLane<T>, deferred: D) -> Self {
        ValueLaneIo { lane, deferred }
    }
}

impl<T, Context, D> RoutingIo<Context> for ValueLaneIo<T, D>
where
    T: Any + Send + Sync + Form + Debug,
    D: DeferredSubscription<Arc<T>>,
    Context: AgentExecutionContext + Send + Sync + 'static,
{
    fn attach(
        self,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let ValueLaneIo { lane, deferred } = self;

        let handler = ValueLaneMessageHandler::new(lane, config.value_lane_backpressure);

        let uplink_factory = SpawnerUplinkFactory::new(config.clone());
        Ok(lane::channels::task::run_lane_io(
            handler,
            uplink_factory,
            ReceiverStream::new(envelopes),
            deferred,
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

pub struct MapLaneIo<K, V, D> {
    lane: MapLane<K, V>,
    deferred: D,
}

impl<K, V, D> MapLaneIo<K, V, D>
where
    K: Any + Send + Sync + Form + Clone + Debug,
    V: Any + Send + Sync + Form + Debug,
    D: DeferredSubscription<MapLaneEvent<K, V>>,
{
    pub fn new(lane: MapLane<K, V>, deferred: D) -> Self {
        MapLaneIo { lane, deferred }
    }
}

impl<K, V, Context, D> RoutingIo<Context> for MapLaneIo<K, V, D>
where
    K: Any + Send + Sync + Form + Clone + Debug,
    V: Any + Send + Sync + Form + Debug,
    Context: AgentExecutionContext + Sized + Send + Sync + 'static,
    D: DeferredSubscription<MapLaneEvent<K, V>>,
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

        let retries = StmRetryStrategy::new(config.retry_strategy);

        let handler =
            MapLaneMessageHandler::new(lane, move || retries, config.map_lane_backpressure);

        Ok(lane::channels::task::run_lane_io(
            handler,
            uplink_factory,
            ReceiverStream::new(envelopes),
            deferred,
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

pub struct ActionLaneIo<Command, Response> {
    lane: ActionLane<Command, Response>,
}

impl<Command, Response> ActionLaneIo<Command, Response>
where
    Command: Send + Sync + Form + Debug + 'static,
    Response: Send + Sync + Form + Debug + 'static,
{
    pub fn new(lane: ActionLane<Command, Response>) -> Self {
        ActionLaneIo { lane }
    }
}

impl<Command, Response, Context> RoutingIo<Context> for ActionLaneIo<Command, Response>
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
        let ActionLaneIo { lane } = self;

        Ok(lane::channels::task::run_action_lane_io(
            lane,
            ReceiverStream::new(envelopes),
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

pub struct CommandLaneIo<T> {
    lane: CommandLane<T>,
}

impl<T> CommandLaneIo<T>
where
    T: Send + Sync + Form + Debug + 'static,
{
    pub fn new(lane: CommandLane<T>) -> Self {
        CommandLaneIo { lane }
    }
}

impl<T, Context> RoutingIo<Context> for CommandLaneIo<T>
where
    T: Send + Sync + Form + Debug + 'static,
    Context: AgentExecutionContext + Sized + Send + Sync + 'static,
{
    fn attach(
        self,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let CommandLaneIo { lane } = self;

        Ok(lane::channels::task::run_command_lane_io(
            lane,
            ReceiverStream::new(envelopes),
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
struct DemandMapLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);

struct DemandLifecycleTasks<L, S, P, Event> {
    name: String,
    lifecycle: L,
    event_stream: S,
    projection: P,
    response_tx: mpsc::Sender<Event>,
}

struct StatelessLifecycleTasks {
    name: String,
    kind: LaneKind,
}

impl<L, S, P> Lane for ValueLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }

    fn kind(&self) -> LaneKind {
        LaneKind::Value
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
                mut lifecycle,
                event_stream,
                projection,
                ..
            }) = *self;
            let model = projection(context.agent());
            let events = event_stream.take_until(context.agent_stop_event());

            let scan_stream = events.owning_scan(None, |prev_val, event| async move {
                Some((
                    Some(event.clone()),
                    ValueLaneEvent {
                        previous: prev_val,
                        current: event,
                    },
                ))
            });

            pin_mut!(scan_stream);
            while let Some(event) = scan_stream.next().await {
                lifecycle
                    .on_event(&event, &model, &context)
                    .instrument(span!(Level::TRACE, ON_EVENT, ?event))
                    .await
            }
        }
        .boxed()
    }
}

/// Lane IO pair consisting of routing IO and store IO.
pub struct LaneIo<Routing, Store> {
    routing: Option<Routing>,
    persistence: Store,
}

impl<Routing, Store> LaneIo<Routing, Store> {
    pub fn new(routing: Option<Routing>, persistence: Store) -> Self {
        LaneIo {
            routing,
            persistence,
        }
    }
}

/// Super trait for all required bounds on a stateful lane with persistence.
pub trait StatefulParam: Any + Send + Sync + Form + Default + Serialize + DeserializeOwned {}
impl<T> StatefulParam for T where
    T: Any + Send + Sync + Form + Default + Serialize + DeserializeOwned
{
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
pub fn make_value_lane<Agent, Context, T, L, Store, P>(
    name: String,
    is_public: bool,
    config: &AgentExecutionConfig,
    init: T,
    lifecycle: L,
    projection: P,
    transient: bool,
) -> (
    ValueLane<T>,
    impl LaneTasks<Agent, Context>,
    LaneIo<impl RoutingIo<Context>, Box<dyn StoreIo<Store>>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    T: StatefulParam + Debug,
    L: for<'l> StatefulLaneLifecycle<'l, ValueLane<T>, Agent>,
    Store: NodeStore,
    P: Fn(&Agent) -> &ValueLane<T> + Send + Sync + 'static,
{
    let (lane, observer) = ValueLane::observable(init, config.observation_buffer);

    let lane_io = if is_public {
        Some(ValueLaneIo::new(lane.clone(), observer.subscriber()))
    } else {
        None
    };

    let tasks = ValueLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream: observer.clone().into_stream(),
        projection,
    });

    let store_io: Box<dyn StoreIo<Store>> = if transient {
        Box::new(LaneNoStore)
    } else {
        Box::new(ValueLaneStoreIo::new(lane.clone(), observer.into_stream()))
    };

    let io = LaneIo {
        routing: lane_io,
        persistence: store_io,
    };

    (lane, tasks, io)
}

impl<L, S, P> Lane for MapLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }

    fn kind(&self) -> LaneKind {
        LaneKind::Map
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
                mut lifecycle,
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
    Option<impl RoutingIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    K: Any + Form + Send + Sync + Clone + Debug,
    V: Any + Form + Send + Sync + Debug,
    L: for<'l> StatefulLaneLifecycle<'l, MapLane<K, V>, Agent>,
{
    let (lane, observer) = MapLane::observable(config.observation_buffer);

    let lane_io = if is_public {
        Some(MapLaneIo::new(
            lane.clone(),
            MapSubscriber::new(observer.subscriber()),
        ))
    } else {
        None
    };

    let tasks = MapLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream: summaries_to_events(observer),
        projection,
    });

    (lane, tasks, lane_io)
}

impl<L, S, P> Lane for ActionLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }

    fn kind(&self) -> LaneKind {
        LaneKind::Action
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

    fn kind(&self) -> LaneKind {
        LaneKind::Command
    }
}

impl<Agent, Context, T, L, S, P> LaneTasks<Agent, Context> for CommandLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Command<T>> + Send + Sync + 'static,
    T: Any + Send + Sync + Debug + Clone,
    L: for<'l> CommandLaneLifecycle<'l, T, Agent>,
    P: Fn(&Agent) -> &CommandLane<T> + Send + Sync + 'static,
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
            while let Some(Command { command, responder }) = events.next().await {
                event!(Level::TRACE, COMMANDED, ?command);
                lifecycle
                    .on_command(&command, &model, &context)
                    .instrument(span!(Level::TRACE, ON_COMMAND))
                    .await;
                if let Some(tx) = responder {
                    if tx.send(command).is_err() {
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
    Option<impl RoutingIo<Context>>,
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
        Some(ActionLaneIo::new(lane.clone()))
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
pub fn make_command_lane<Agent, Context, T, L>(
    name: impl Into<String>,
    is_public: bool,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &CommandLane<T> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
) -> (
    CommandLane<T>,
    impl LaneTasks<Agent, Context>,
    Option<impl RoutingIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    T: Any + Send + Sync + Form + Debug + Clone,
    L: for<'l> CommandLaneLifecycle<'l, T, Agent>,
{
    let (lane, event_stream) = model::command::make_lane_model(buffer_size);

    let tasks = CommandLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream,
        projection,
    });

    let lane_io = if is_public {
        Some(CommandLaneIo::new(lane.clone()))
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
    Option<impl RoutingIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    T: Any + Clone + Send + Sync + Form + Debug,
{
    let (lane, view) = make_lane_model(buffer_size);

    let tasks = StatelessLifecycleTasks {
        name: name.into(),
        kind: LaneKind::Supply,
    };

    let lane_io = if is_public {
        Some(SupplyLaneIo::new(view))
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

impl<S, Item, Context> RoutingIo<Context> for SupplyLaneIo<S>
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

        Ok(run_supply_lane_io(
            ReceiverStream::new(envelopes),
            config,
            context,
            route,
            stream,
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

/// Create a new demand lane.
///
/// # Arguments
///
/// * `name` - The name of the lane.
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
/// * `buffer_size` - Buffer size for the MPSC channel accepting the commands.
pub fn make_demand_lane<Agent, Context, Event, L>(
    name: impl Into<String>,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &DemandLane<Event> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
) -> (
    DemandLane<Event>,
    impl LaneTasks<Agent, Context>,
    impl RoutingIo<Context>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    Event: Any + Send + Sync + Form + Debug,
    L: for<'l> DemandLaneLifecycle<'l, Event, Agent>,
{
    let (lane, cue_stream) = model::demand::make_lane_model(buffer_size);
    let (response_tx, response_rx) = mpsc::channel(buffer_size.get());

    let tasks = DemandLifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream: cue_stream,
        projection,
        response_tx,
    };

    let lane_io = DemandLaneIo::new(response_rx);

    (lane, tasks, lane_io)
}

pub struct DemandLaneIo<Event> {
    response_rx: mpsc::Receiver<Event>,
}

impl<Event> DemandLaneIo<Event>
where
    Event: Send + Sync + 'static,
{
    pub fn new(response_rx: mpsc::Receiver<Event>) -> DemandLaneIo<Event> {
        DemandLaneIo { response_rx }
    }
}

impl<Event, Context> RoutingIo<Context> for DemandLaneIo<Event>
where
    Event: Form + Send + Sync + 'static,
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

        Ok(lane::channels::task::run_demand_lane_io(
            ReceiverStream::new(envelopes),
            config,
            context,
            route,
            response_rx,
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

impl<L, S, P, Event> Lane for DemandLifecycleTasks<L, S, P, Event> {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn kind(&self) -> LaneKind {
        LaneKind::Demand
    }
}

impl<Agent, Context, L, S, P, Event> LaneTasks<Agent, Context>
    for DemandLifecycleTasks<L, S, P, Event>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = ()> + Send + Sync + 'static,
    Event: Any + Send + Sync + Debug,
    L: for<'l> DemandLaneLifecycle<'l, Event, Agent>,
    P: Fn(&Agent) -> &DemandLane<Event> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let DemandLifecycleTasks {
                lifecycle,
                event_stream,
                projection,
                response_tx,
                ..
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

/// Create a new demand map lane.
///
/// # Arguments
///
/// * `name` - The name of the lane.
/// * `is_public` - Whether the lane is public (with respect to external message routing).
/// * `lifecycle` - Life-cycle event handler for the lane.
/// * `projection` - A projection from the agent type to this lane.
/// * `buffer_size` - Buffer size for the MPSC channel accepting the commands.
pub fn make_demand_map_lane<Agent, Context, Key, Value, L>(
    name: impl Into<String>,
    is_public: bool,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &DemandMapLane<Key, Value> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
) -> (
    DemandMapLane<Key, Value>,
    impl LaneTasks<Agent, Context>,
    Option<impl RoutingIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    Key: Any + Send + Sync + Form + Clone + Debug,
    Value: Any + Send + Sync + Form + Clone + Debug,
    L: for<'l> DemandMapLaneLifecycle<'l, Key, Value, Agent>,
{
    let (lifecycle_tx, event_rx) = mpsc::channel(buffer_size.get());
    let (lane, topic) = model::demand_map::make_lane_model(buffer_size, lifecycle_tx);

    let tasks = DemandMapLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream: ReceiverStream::new(event_rx),
        projection,
    });

    let lane_io = if is_public {
        Some(DemandMapLaneIo::new(lane.clone(), topic))
    } else {
        None
    };

    (lane, tasks, lane_io)
}

pub struct DemandMapLaneIo<Key, Value>
where
    Key: Debug + Form + Send + Sync + 'static,
    Value: Debug + Form + Send + Sync + 'static,
{
    lane: DemandMapLane<Key, Value>,
    rx: mpsc::Receiver<DemandMapLaneEvent<Key, Value>>,
}

impl<Key, Value> DemandMapLaneIo<Key, Value>
where
    Key: Debug + Form + Send + Sync + 'static,
    Value: Debug + Form + Send + Sync + 'static,
{
    pub fn new(
        lane: DemandMapLane<Key, Value>,
        rx: mpsc::Receiver<DemandMapLaneEvent<Key, Value>>,
    ) -> DemandMapLaneIo<Key, Value> {
        DemandMapLaneIo { lane, rx }
    }
}

impl<Key, Value, Context> RoutingIo<Context> for DemandMapLaneIo<Key, Value>
where
    Key: Any + Send + Sync + Form + Clone + Debug,
    Value: Any + Send + Sync + Form + Clone + Debug,
    Context: AgentExecutionContext + Sized + Send + Sync + 'static,
{
    fn attach(
        self,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: Context,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let DemandMapLaneIo { lane, mut rx, .. } = self;
        let uplink_factory = SpawnerUplinkFactory::new(config.clone());
        let message_handler = DemandMapLaneMessageHandler::new(lane);

        let (mut topic_tx, topic_rx) = topic::channel(config.observation_buffer);

        let pump = async move {
            while let Some(update) = rx.recv().await {
                if topic_tx.discarding_send(update).await.is_err() {
                    break;
                }
            }
        };

        let lane_task = lane::channels::task::run_lane_io(
            message_handler,
            uplink_factory,
            ReceiverStream::new(envelopes),
            topic_rx.subscriber(),
            config,
            context,
            route,
        );

        Ok(join(pump, lane_task).map(|(_, r)| r).boxed())
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

impl<L, S, P> Lane for DemandMapLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }

    fn kind(&self) -> LaneKind {
        LaneKind::DemandMap
    }
}

impl<Agent, Context, L, S, P, Key, Value> LaneTasks<Agent, Context>
    for DemandMapLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = DemandMapLaneCommand<Key, Value>> + Send + Sync + 'static,
    Key: Any + Clone + Form + Send + Sync + Debug,
    Value: Any + Clone + Form + Send + Sync + Debug,
    L: for<'l> DemandMapLaneLifecycle<'l, Key, Value, Agent>,
    P: Fn(&Agent) -> &DemandMapLane<Key, Value> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let DemandMapLifecycleTasks(LifecycleTasks {
                mut lifecycle,
                event_stream,
                projection,
                ..
            }) = *self;

            let model = projection(context.agent()).clone();
            let events = event_stream.take_until(context.agent_stop_event());

            pin_mut!(events);

            while let Some(event) = events.next().await {
                match event {
                    DemandMapLaneCommand::Sync(sender) => {
                        let keys: Vec<Key> = lifecycle.on_sync(&model, &context).await;
                        let keys_len = keys.len();

                        let mut values = iter(keys)
                            .fold(Vec::with_capacity(keys_len), |mut results, key| async {
                                if let Some(value) =
                                    lifecycle.on_cue(&model, &context, key.clone()).await
                                {
                                    results.push(DemandMapLaneEvent::update(key, value));
                                }

                                results
                            })
                            .await;

                        values.shrink_to_fit();

                        let _ = sender.send(values);
                    }
                    DemandMapLaneCommand::Cue(sender, key) => {
                        let value = lifecycle.on_cue(&model, &context, key).await;
                        let _ = sender.send(value);
                    }
                    DemandMapLaneCommand::Remove(key) => {
                        lifecycle.on_remove(&model, &context, key).await;
                    }
                }
            }
        }
        .boxed()
    }
}
