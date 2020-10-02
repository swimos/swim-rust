use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::supply::SupplyLaneUplinks;
use crate::agent::lane::channels::uplink::{UplinkError, UplinkStateMachine};
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::LaneModel;
use crate::agent::{AgentContext, AttachError, Eff, Lane, LaneIo, LaneTasks, LifecycleTasks};
use crate::routing::TaggedClientEnvelope;
use futures::future::BoxFuture;
use futures::future::{join3, ready};
use futures::stream::{BoxStream, FusedStream};
use futures::{FutureExt, SinkExt};
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;
use stm::var::observer::join;
use swim_common::form::Form;
use swim_common::model::Value;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, Mutex};
use tracing::{event, span, Level};
use tracing_futures::{Instrument, Instrumented};
use utilities::sync::trigger;

const LANE_IO_TASK: &str = "Lane IO task.";
const UPDATE_TASK: &str = "Lane update task.";
const UPLINK_SPAWN_TASK: &str = "Uplink spawn task.";
const DISPATCH_ACTION: &str = "Dispatching uplink action.";
const DISPATCH_COMMAND: &str = "Dispatching lane command.";
const UPLINK_FAILED: &str = "An uplink failed with a non-fatal error.";
const UPLINK_FATAL: &str = "An uplink failed with a fatal error.";
const TOO_MANY_FAILURES: &str = "Terminating after too many failed uplinks.";

pub fn make_supply_lane<Agent, Context, T, L, S>(
    name: impl Into<String>,
    is_public: bool,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &SupplyLane<T> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
    event_stream: S,
) -> (
    SupplyLane<T>,
    impl LaneTasks<Agent, Context>,
    Option<impl LaneIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    T: Any + Clone + Send + Sync + Form + Debug,
    L: for<'l> StatelessLaneLifecycle<'l, SupplyLane<T>, Agent>,
    S: Stream<Item = T>,
{
    let (lane, event_stream) = make_lane_model(buffer_size);

    let tasks = SupplyLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream,
        projection,
    });

    let lane_io = if is_public {
        Some(SupplyLaneIo::new(lane.clone()))
    } else {
        None
    };

    (lane, tasks, lane_io)
}

#[derive(Debug, Clone)]
pub struct SupplyLane<T> {
    sender: mpsc::Sender<T>,
    id: Arc<()>,
}

impl<T> SupplyLane<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(sender: mpsc::Sender<T>) -> Self {
        SupplyLane {
            sender,
            id: Default::default(),
        }
    }

    pub fn supplier(&self) -> mpsc::Sender<T> {
        self.sender.clone()
    }
}

impl<T> LaneModel for SupplyLane<T> {
    type Event = T;

    fn same_lane(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.id, &other.id)
    }
}

struct SupplyLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);

struct SupplyLaneIo<Item> {
    lane: SupplyLane<Item>,
}

impl<Item> SupplyLaneIo<Item> {
    fn new(lane: SupplyLane<Item>) -> Self {
        SupplyLaneIo { lane }
    }
}

impl<Item, Context> LaneIo<Context> for SupplyLaneIo<Item>
where
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
        let SupplyLaneIo { lane } = self;

        Ok(run_supply_lane_io(lane, envelopes, config, context, route).boxed())
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

pub fn make_lane_model<T>(
    buffer_size: NonZeroUsize,
) -> (SupplyLane<T>, impl Stream<Item = T> + Send + 'static)
where
    T: Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let lane = SupplyLane::new(tx);
    (lane, rx)
}

pub trait StatelessLaneLifecycleBase: Send + Sync + 'static {
    type WatchStrategy;

    fn create_strategy(&self) -> Self::WatchStrategy;
}

pub trait StatelessLaneLifecycle<'a, Model: LaneModel, Agent>: StatelessLaneLifecycleBase {
    type UplinkFuture: Future<Output = ()> + Send + 'a;
    type CueFuture: Future<Output = Model::Event> + Send + 'a;

    fn on_supply<C>(
        &'a self,
        event: Model::Event,
        model: &'a Model,
        context: &'a C,
    ) -> Self::CueFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

pub async fn run_supply_lane_io<Item>(
    lane: SupplyLane<Item>,
    envelopes: impl Stream<Item = TaggedClientEnvelope>,
    config: AgentExecutionConfig,
    context: impl AgentExecutionContext,
    route: RelativePath,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    Item: Send + Sync + Form + Debug + 'static,
{
    let span = span!(Level::INFO, LANE_IO_TASK, ?route);
    let _enter = span.enter();

    let producer = envelopes.map(|(envelope)| {});
    let (uplink_tx, uplink_rx) = mpsc::channel(config.action_buffer.get());
    let (err_tx, mut err_rx) = mpsc::channel(config.uplink_err_buffer.get());
    let uplinks = SupplyLaneUplinks::new(envelopes, route.clone());

    uplinks
        .run(uplink_rx, context.router_handle(), err_tx)
        .instrument(span!(Level::INFO, UPLINK_SPAWN_TASK, ?route))
        .await;

    match err_rx.next().await {
        Some(uplink_errs) => Err(LaneIoError::for_uplink_errors(route, vec![uplink_errs])),
        None => Ok(vec![]),
    }
}

impl<L, S, P> Lane for SupplyLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }
}

impl<Agent, Context, T, L, S, P> LaneTasks<Agent, Context> for SupplyLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = T> + Send + Sync + 'static,
    T: Any + Send + Sync + Debug,
    L: for<'l> StatelessLaneLifecycle<'l, SupplyLane<T>, Agent>,
    P: Fn(&Agent) -> &SupplyLane<T> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> Eff {
        async move {
            let SupplyLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
                ..
            }) = *self;

            let model = projection(context.agent()).clone();
            let events = event_stream.take_until(context.agent_stop_event());

            pin_mut!(events);

            while let Some(t) = events.next().await {
                lifecycle.on_supply(t, &model, &context);
            }
        }
        .boxed()
    }
}
