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

use crate::agent::lane::channels::update::map::MapLaneUpdateTask;
use crate::agent::lane::channels::update::value::ValueLaneUpdateTask;
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::channels::uplink::{
    MapLaneUplink, Uplink, UplinkAction, UplinkError, UplinkMessage, UplinkStateMachine,
    ValueLaneUplink,
};
use crate::agent::lane::channels::AgentExecutionContext;
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::ValueLane;
use crate::routing::{RoutingAddr, ServerRouter, TaggedEnvelope};
use common::model::Value;
use common::sink::item::ItemSender;
use common::topic::Topic;
use common::warp::envelope::{Envelope, EnvelopeHeader, OutgoingHeader};
use common::warp::path::RelativePath;
use either::Either;
use futures::future::{join3, join_all, BoxFuture};
use futures::{select, FutureExt, Stream, StreamExt};
use pin_utils::core_reexport::fmt::Formatter;
use pin_utils::core_reexport::num::NonZeroUsize;
use pin_utils::pin_mut;
use std::any::Any;
use std::collections::hash_map::{Entry, HashMap};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stm::transaction::RetryManager;
use swim_form::Form;
use tokio::sync::mpsc;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use utilities::sync::trigger;

pub trait LaneMessageHandler {
    type Event: Send;
    type Uplink: UplinkStateMachine<Self::Event> + Send + Sync + 'static;
    type Update: LaneUpdate;

    fn make_uplink(&self) -> Self::Uplink;

    fn make_update(&self) -> Self::Update;
}

#[derive(Debug)]
pub struct TaggedAction(RoutingAddr, UplinkAction);

struct UplinkSpawner<Handler, Top> {
    handler: Arc<Handler>,
    topic: Top,
    actions: mpsc::Receiver<TaggedAction>,
    action_buffer_size: NonZeroUsize,
    route: RelativePath,
}

type OutputMessage<Handler> = <<Handler as LaneMessageHandler>::Uplink as UplinkStateMachine<
    <Handler as LaneMessageHandler>::Event,
>>::Msg;

type InputMessage<Handler> = <<Handler as LaneMessageHandler>::Update as LaneUpdate>::Msg;

#[derive(Debug)]
pub struct UplinkErrorReport {
    pub error: UplinkError,
    pub addr: RoutingAddr,
}

impl UplinkErrorReport {
    fn new(error: UplinkError, addr: RoutingAddr) -> Self {
        UplinkErrorReport { error, addr }
    }
}

impl Display for UplinkErrorReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Uplink to {} failed: {}", &self.addr, &self.error)
    }
}

const FAILED_ERR_REPORT: &str = "Failed to send error report.";
const UPLINK_TERMINATED: &str = "An uplink terminated uncleanly.";

struct UplinkHandle {
    sender: mpsc::Sender<UplinkAction>,
    wait_on_cleanup: trigger::Receiver,
}

impl UplinkHandle {
    fn new(sender: mpsc::Sender<UplinkAction>, wait_on_cleanup: trigger::Receiver) -> Self {
        UplinkHandle {
            sender,
            wait_on_cleanup,
        }
    }

    async fn send(
        &mut self,
        action: UplinkAction,
    ) -> Result<(), mpsc::error::SendError<UplinkAction>> {
        self.sender.send(action).await
    }

    async fn cleanup(self) -> bool {
        let UplinkHandle {
            sender,
            wait_on_cleanup,
        } = self;
        drop(sender);
        wait_on_cleanup.await.is_ok()
    }
}

impl<Handler, Top> UplinkSpawner<Handler, Top>
where
    Handler: LaneMessageHandler,
    OutputMessage<Handler>: Into<Value>,
    Top: Topic<Handler::Event>,
{
    fn new(
        handler: Arc<Handler>,
        topic: Top,
        rx: mpsc::Receiver<TaggedAction>,
        action_buffer_size: NonZeroUsize,
        route: RelativePath,
    ) -> Self {
        UplinkSpawner {
            handler,
            topic,
            actions: rx,
            action_buffer_size,
            route,
        }
    }

    async fn run<Router>(
        self,
        mut router: Router,
        mut spawn_tx: mpsc::Sender<BoxFuture<'static, ()>>,
        error_collector: mpsc::Sender<UplinkErrorReport>,
    ) where
        Router: ServerRouter,
    {
        let UplinkSpawner {
            handler,
            mut topic,
            mut actions,
            action_buffer_size,
            route,
        } = self;
        let mut uplink_senders: HashMap<RoutingAddr, UplinkHandle> = HashMap::new();

        'outer: while let Some(TaggedAction(addr, action)) = actions.recv().await {
            let mut action = Some(action);
            while let Some(act) = action.take() {
                let sender = match uplink_senders.entry(addr) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        let (tx, rx) = mpsc::channel(action_buffer_size.get());
                        let (cleanup_tx, cleanup_rx) = trigger::trigger();
                        let state_machine = handler.make_uplink();
                        let updates = if let Ok(sub) = topic.subscribe().await {
                            sub.fuse()
                        } else {
                            break 'outer;
                        };
                        let uplink = Uplink::new(state_machine, rx.fuse(), updates);

                        let route_cpy = route.clone();

                        let sink = if let Ok(sender) = router.get_sender(addr) {
                            sender.comap(
                                move |msg: UplinkMessage<OutputMessage<Handler>>| match msg {
                                    UplinkMessage::Linked => {
                                        Envelope::linked(&route_cpy.node, &route_cpy.lane)
                                    }
                                    UplinkMessage::Synced => {
                                        Envelope::synced(&route_cpy.node, &route_cpy.lane)
                                    }
                                    UplinkMessage::Unlinked => {
                                        Envelope::unlinked(&route_cpy.node, &route_cpy.lane)
                                    }
                                    UplinkMessage::Event(ev) => Envelope::make_event(
                                        &route_cpy.node,
                                        &route_cpy.lane,
                                        Some(ev.into()),
                                    ),
                                },
                            )
                        } else {
                            break 'outer;
                        };
                        let mut err_tx_cpy = error_collector.clone();
                        let ul_task = async move {
                            if let Err(err) = uplink.run_uplink(sink).await {
                                let report = UplinkErrorReport::new(err, addr);
                                if let Err(mpsc::error::SendError(report)) =
                                    err_tx_cpy.send(report).await
                                {
                                    event!(Level::ERROR, message = FAILED_ERR_REPORT, ?report);
                                }
                                cleanup_tx.trigger();
                            }
                        }
                        .instrument(span!(
                            Level::INFO,
                            "Lane uplink.",
                            ?route,
                            ?addr
                        ));
                        if spawn_tx.send(ul_task.boxed()).await.is_err() {
                            break 'outer;
                        }
                        entry.insert(UplinkHandle::new(tx, cleanup_rx))
                    }
                };
                if let Err(mpsc::error::SendError(act)) = sender.send(act).await {
                    if let Some(handle) = uplink_senders.remove(&addr) {
                        if !handle.cleanup().await {
                            event!(Level::ERROR, message = UPLINK_TERMINATED, ?route, ?addr);
                        }
                    }
                    action = Some(act);
                }
            }
        }
        join_all(uplink_senders.into_iter().map(|(_, h)| h.cleanup())).await;
    }
}

#[derive(Debug)]
pub struct LaneIoError {
    pub route: RelativePath,
    pub update_error: Option<UpdateError>,
    pub uplink_errors: Vec<UplinkErrorReport>,
}

impl LaneIoError {
    pub fn for_update_err(route: RelativePath, err: UpdateError) -> Self {
        LaneIoError {
            route,
            update_error: Some(err),
            uplink_errors: vec![],
        }
    }

    pub fn for_uplink_errors(route: RelativePath, errs: Vec<UplinkErrorReport>) -> Self {
        assert!(!errs.is_empty());
        LaneIoError {
            route,
            update_error: None,
            uplink_errors: errs,
        }
    }

    pub fn new(route: RelativePath, upd: UpdateError, upl: Vec<UplinkErrorReport>) -> Self {
        LaneIoError {
            route,
            update_error: Some(upd),
            uplink_errors: upl,
        }
    }
}

impl Display for LaneIoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let LaneIoError {
            route,
            update_error,
            uplink_errors,
        } = self;
        writeln!(f, "IO tasks failed for lane: \"{}\".", route)?;
        if let Some(upd) = update_error {
            writeln!(f, "    update_error = {}", upd)?;
        }

        if !uplink_errors.is_empty() {
            writeln!(f, "    uplink_errors =")?;
            for err in uplink_errors.iter() {
                writeln!(f, "        {},", err)?;
            }
        }
        Ok(())
    }
}

impl Error for LaneIoError {}

pub async fn run_lane_io<Handler>(
    message_handler: Handler,
    envelopes: impl Stream<Item = TaggedEnvelope>,
    events: impl Topic<Handler::Event>,
    context: &impl AgentExecutionContext,
    route: RelativePath,
) -> Result<(), LaneIoError>
where
    Handler: LaneMessageHandler,
    OutputMessage<Handler>: Into<Value>,
    InputMessage<Handler>: Debug + Form,
{
    let envelopes = envelopes.fuse();
    let arc_handler = Arc::new(message_handler);

    let act_buffer_size = context.configuration().action_buffer;
    let upd_buffer_size = context.configuration().update_buffer;
    let max_fatal = context.configuration().max_fatal_uplink_errors;

    let (mut act_tx, act_rx) = mpsc::channel(act_buffer_size.get());
    let (mut upd_tx, upd_rx) = mpsc::channel(upd_buffer_size.get());

    let spawner = UplinkSpawner::new(
        arc_handler.clone(),
        events,
        act_rx,
        act_buffer_size,
        route.clone(),
    );

    let update_task = arc_handler.make_update().run_update(upd_rx);

    let (err_tx, err_rx) = mpsc::channel(5);

    let uplink_spawn_task = spawner.run(context.router_handle(), context.spawner(), err_tx);

    let mut err_rx = err_rx.fuse();

    let envelope_task = async move {
        pin_mut!(envelopes);

        let mut uplink_errors = vec![];
        let mut num_fatal: usize = 0;

        let failed: bool = loop {
            let envelope_or_err: Option<Either<TaggedEnvelope, UplinkErrorReport>> = select! {
                maybe_env = envelopes.next() => maybe_env.map(Either::Left),
                maybe_err = err_rx.next() => maybe_err.map(Either::Right),
            };

            match envelope_or_err {
                Some(Either::Left(envelope)) => {
                    let TaggedEnvelope(addr, Envelope { header, body }) = envelope;
                    let action = match header {
                        EnvelopeHeader::OutgoingLink(OutgoingHeader::Link(_), _) => {
                            Some(Either::Left(UplinkAction::Link))
                        }
                        EnvelopeHeader::OutgoingLink(OutgoingHeader::Sync(_), _) => {
                            Some(Either::Left(UplinkAction::Sync))
                        }
                        EnvelopeHeader::OutgoingLink(OutgoingHeader::Unlink, _) => {
                            Some(Either::Left(UplinkAction::Unlink))
                        }
                        EnvelopeHeader::OutgoingLink(OutgoingHeader::Command, _) => {
                            Some(Either::Right(body.unwrap_or(Value::Extant)))
                        }
                        _ => None,
                    };
                    match action {
                        Some(Either::Left(uplink_action)) => {
                            if act_tx
                                .send(TaggedAction(addr, uplink_action))
                                .await
                                .is_err()
                            {
                                break false;
                            }
                        }
                        Some(Either::Right(command)) => {
                            if upd_tx.send(Form::try_convert(command)).await.is_err() {
                                break false;
                            }
                        }
                        _ => {
                            //TODO handle incoming messages to support server side downlinks.
                        }
                    }
                }
                Some(Either::Right(error)) => {
                    if error.error.is_fatal() {
                        num_fatal += 1;
                    }
                    uplink_errors.push(error);
                    if num_fatal > max_fatal {
                        break true;
                    }
                }
                _ => {
                    break false;
                }
            }
        };
        (failed, uplink_errors)
    };

    match join3(uplink_spawn_task, update_task, envelope_task).await {
        (_, Err(upd_err), (_, upl_errs)) => Err(LaneIoError::new(route, upd_err, upl_errs)),
        (_, _, (true, upl_errs)) if !upl_errs.is_empty() => {
            Err(LaneIoError::for_uplink_errors(route, upl_errs))
        }
        _ => Ok(()),
    }
}

impl<T> LaneMessageHandler for ValueLane<T>
where
    T: Any + Send + Sync,
{
    type Event = Arc<T>;
    type Uplink = ValueLaneUplink<T>;
    type Update = ValueLaneUpdateTask<T>;

    fn make_uplink(&self) -> Self::Uplink {
        ValueLaneUplink::new((*self).clone())
    }

    fn make_update(&self) -> Self::Update {
        ValueLaneUpdateTask::new((*self).clone())
    }
}

pub struct MapLaneMessageHandler<K, V, F> {
    lane: MapLane<K, V>,
    uplink_counter: AtomicU64,
    retries: F,
}

impl<K, V, F, Ret> MapLaneMessageHandler<K, V, F>
where
    F: Fn() -> Ret + Clone + Send + Sync + 'static,
    Ret: RetryManager + Send + Sync + 'static,
{
    pub fn new(lane: MapLane<K, V>, retries: F) -> Self {
        MapLaneMessageHandler {
            lane,
            uplink_counter: AtomicU64::new(1),
            retries,
        }
    }
}

impl<K, V, F, Ret> LaneMessageHandler for MapLaneMessageHandler<K, V, F>
where
    K: Any + Form + Send + Sync + Debug,
    V: Any + Send + Sync + Debug,
    F: Fn() -> Ret + Clone + Send + Sync + 'static,
    Ret: RetryManager + Send + Sync + 'static,
{
    type Event = MapLaneEvent<K, V>;
    type Uplink = MapLaneUplink<K, V, F>;
    type Update = MapLaneUpdateTask<K, V, F>;

    fn make_uplink(&self) -> Self::Uplink {
        let MapLaneMessageHandler {
            lane,
            uplink_counter,
            retries,
        } = self;
        let i = uplink_counter.fetch_add(1, Ordering::Relaxed);
        MapLaneUplink::new((*lane).clone(), i, retries.clone())
    }

    fn make_update(&self) -> Self::Update {
        let MapLaneMessageHandler { lane, retries, .. } = self;
        MapLaneUpdateTask::new((*lane).clone(), (*retries).clone())
    }
}
