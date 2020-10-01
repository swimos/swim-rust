use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::{UplinkError, UplinkStateMachine};
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::LaneModel;
use crate::agent::{AgentContext, AttachError, Eff, Lane, LaneIo, LaneTasks, LifecycleTasks};
use crate::routing::TaggedClientEnvelope;
use futures::future::ready;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, FusedStream};
use futures::FutureExt;
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;
use swim_common::form::Form;
use swim_common::model::Value;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, Mutex};
use tracing::{event, span, Level};
use tracing_futures::{Instrument, Instrumented};
use utilities::sync::trigger;

pub fn make_supply_lane<Agent, Context, T, L, S>(
    name: impl Into<String>,
    is_public: bool,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &SupplyLane<T> + Send + Sync + 'static,
    buffer_size: NonZeroUsize,
    producer: S,
) -> (
    SupplyLane<T>,
    impl LaneTasks<Agent, Context>,
    impl LaneIo<Context>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    T: Any + Clone + Send + Sync + Form + Debug,
    L: for<'l> StatelessLaneLifecycle<'l, SupplyLane<T>, Agent>,
    S: Stream<Item = T> + Send + Sync + 'static,
{
    let (lane, event_stream) = make_lane_model(buffer_size);

    let tasks = SupplyLifecycleTasks(LifecycleTasks {
        name: name.into(),
        lifecycle,
        event_stream,
        projection,
    });

    let lane_io = SupplyLaneIo::new(lane.clone(), producer);

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
}

impl<T> LaneModel for SupplyLane<T> {
    type Event = T;

    fn same_lane(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.id, &other.id)
    }
}

struct SupplyLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);

struct SupplyLaneIo<S, Item>
where
    S: Stream<Item = Item>,
{
    lane: SupplyLane<Item>,
    producer: S,
}

impl<S, Item> SupplyLaneIo<S, Item>
where
    S: Stream<Item = Item>,
{
    fn new(lane: SupplyLane<Item>, producer: S) -> Self {
        SupplyLaneIo { lane, producer }
    }
}

impl<S, Item, Context> LaneIo<Context> for SupplyLaneIo<S, Item>
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
        let SupplyLaneIo { lane, producer } = self;

        Ok(run_supply_lane_io(lane, producer, envelopes, config, context, route).boxed())
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

    fn on_uplink<C>(&'a self, model: &'a Model, context: &'a C) -> Self::UplinkFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'a;

    fn on_cue<C>(
        &'a self,
        event: Model::Event,
        model: &'a Model,
        context: &'a C,
    ) -> Self::CueFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

pub async fn run_supply_lane_io<S, Item>(
    _lane: SupplyLane<Item>,
    _producer: S,
    _envelopes: impl Stream<Item = TaggedClientEnvelope>,
    _config: AgentExecutionConfig,
    _context: impl AgentExecutionContext,
    _route: RelativePath,
) -> Result<Vec<UplinkErrorReport>, LaneIoError>
where
    S: Stream<Item = Item>,
    Item: Send + Sync + Form + Debug + 'static,
{
    // let span = span!(Level::INFO, LANE_IO_TASK, ?route);
    // let _enter = span.enter();
    //
    // let (update_tx, update_rx) = mpsc::channel(config.update_buffer.get());
    // let (upd_done_tx, upd_done_rx) = trigger::trigger();
    // let envelopes = envelopes.take_until(upd_done_rx);
    //
    // if feedback {
    //     let (feedback_tx, feedback_rx) = mpsc::channel(config.feedback_buffer.get());
    //     let (uplink_tx, uplink_rx) = mpsc::channel(config.action_buffer.get());
    //     let (err_tx, err_rx) = mpsc::channel(config.uplink_err_buffer.get());
    //
    //     let updater =
    //         ActionLaneUpdateTask::new(lane.clone(), Some(feedback_tx), config.cleanup_timeout);
    //
    //     let update_task = async move {
    //         let result = updater.run_update(update_rx).await;
    //         upd_done_tx.trigger();
    //         result
    //     }
    //     .instrument(span!(Level::INFO, UPDATE_TASK, ?route));
    //
    //     let uplinks = ActionLaneUplinks::new(feedback_rx, route.clone());
    //     let uplink_task = uplinks
    //         .run(uplink_rx, context.router_handle(), err_tx)
    //         .instrument(span!(Level::INFO, UPLINK_SPAWN_TASK, ?route));
    //
    //     let envelope_task = action_envelope_task_with_uplinks(
    //         route.clone(),
    //         config,
    //         envelopes,
    //         update_tx,
    //         uplink_tx,
    //         err_rx,
    //     );
    //     let (upd_result, _, (uplink_fatal, uplink_errs)) =
    //         join3(update_task, uplink_task, envelope_task).await;
    //     combine_results(route, upd_result.err(), uplink_fatal, uplink_errs)
    // } else {
    //     let updater = ActionLaneUpdateTask::new(lane.clone(), None, config.cleanup_timeout);
    //     let update_task = async move {
    //         let result = updater.run_update(update_rx).await;
    //         upd_done_tx.trigger();
    //         result
    //     }
    //     .instrument(span!(Level::INFO, UPDATE_TASK, ?route));
    //     let envelope_task = simple_action_envelope_task(envelopes, update_tx);
    //     let (upd_result, _) = join(update_task, envelope_task).await;
    //     combine_results(route, upd_result.err(), false, vec![])
    // }
    unimplemented!()
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
                // let response = lifecycle
                //     .on_cue(t, &model, &context)
                //     .instrument(span!(Level::TRACE, ON_CUE))
                //     .await;

                // event!(Level::TRACE, COMMANDED, ?command);
                // //TODO After agents are connected to web-sockets the response will have somewhere to go.
                // let response = lifecycle
                //     .on_command(command, &model, &context)
                //     .instrument(span!(Level::TRACE, ON_COMMAND))
                //     .await;
                // event!(Level::TRACE, ACTION_RESULT, ?response);
                // if let Some(tx) = responder {
                //     if tx.send(response).is_err() {
                //         event!(Level::WARN, RESPONSE_IGNORED);
                //     }
                // }
                unimplemented!()
            }
        }
        .boxed()
    }
}
