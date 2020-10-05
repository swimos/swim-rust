use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::task::{run_supply_lane_io, LaneIoError};
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::LaneModel;
use crate::agent::{AgentContext, AttachError, Eff, Lane, LaneIo, LaneTasks};
use crate::routing::TaggedClientEnvelope;
use futures::future::ready;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::Stream;
use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

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

    let tasks = SupplyLifecycleTasks(StatelessLifecycleTasks { name: name.into() });

    let lane_io = if is_public {
        Some(SupplyLaneIo::new(event_stream))
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

struct SupplyLifecycleTasks(StatelessLifecycleTasks);

struct StatelessLifecycleTasks {
    name: String,
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
        let SupplyLaneIo { stream, .. } = self;

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

impl Lane for SupplyLifecycleTasks {
    fn name(&self) -> &str {
        self.0.name.as_str()
    }
}

impl<Agent, Context> LaneTasks<Agent, Context> for SupplyLifecycleTasks
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, _context: Context) -> Eff {
        async { panic!() }.boxed()
    }
}
