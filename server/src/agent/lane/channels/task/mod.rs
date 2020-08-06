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
use crate::agent::lane::channels::uplink::{MapLaneUplink, Uplink, UplinkAction, UplinkMessage, UplinkStateMachine, ValueLaneUplink, UplinkError};
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
use futures::future::{join3, BoxFuture};
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::core_reexport::num::NonZeroUsize;
use pin_utils::pin_mut;
use std::any::Any;
use std::collections::hash_map::{Entry, HashMap};
use std::fmt::{Debug, Display};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stm::transaction::RetryManager;
use swim_form::Form;
use tokio::sync::mpsc;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use pin_utils::core_reexport::fmt::Formatter;
use std::error::Error;

pub trait LaneMessageHandler {
    type Event: Send;
    type Uplink: UplinkStateMachine<Self::Event> + Send + Sync + 'static;
    type Update: LaneUpdate;

    fn make_uplink(&self) -> Self::Uplink;

    fn make_update(&self) -> Self::Update;
}

#[derive(Debug)]
pub struct TaggedAction(RoutingAddr, UplinkAction);

struct UplinkSpawner<Handler, Top, Router> {
    handler: Arc<Handler>,
    topic: Top,
    router: Router,
    actions: mpsc::Receiver<TaggedAction>,
    action_buffer_size: NonZeroUsize,
    route: RelativePath,
}

type OutputMessage<Handler> = <<Handler as LaneMessageHandler>::Uplink as UplinkStateMachine<
    <Handler as LaneMessageHandler>::Event,
>>::Msg;

type InputMessage<Handler> = <<Handler as LaneMessageHandler>::Update as LaneUpdate>::Msg;

impl<Handler, Top, Router> UplinkSpawner<Handler, Top, Router>
where
    Handler: LaneMessageHandler,
    OutputMessage<Handler>: Into<Value>,
    Top: Topic<Handler::Event>,
    Router: ServerRouter,
{
    fn new(
        handler: Arc<Handler>,
        topic: Top,
        router: Router,
        rx: mpsc::Receiver<TaggedAction>,
        action_buffer_size: NonZeroUsize,
        route: RelativePath,
    ) -> Self {
        UplinkSpawner {
            handler,
            topic,
            router,
            actions: rx,
            action_buffer_size,
            route,
        }
    }

    async fn run(self, mut spawn_tx: mpsc::Sender<BoxFuture<'static, ()>>) {
        let UplinkSpawner {
            handler,
            mut topic,
            mut router,
            mut actions,
            action_buffer_size,
            route,
        } = self;
        let mut uplink_senders: HashMap<RoutingAddr, mpsc::Sender<UplinkAction>> = HashMap::new();
        'outer: while let Some(TaggedAction(addr, action)) = actions.recv().await {
            let mut action = Some(action);
            while let Some(act) = action.take() {
                let sender = match uplink_senders.entry(addr) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        let (tx, rx) = mpsc::channel(action_buffer_size.get());
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
                        let route_cpy = route.clone();
                        let ul_task = async move {
                            if let Err(err) = uplink.run_uplink(sink).await {
                                event!(Level::ERROR, %err, ?addr, route = ?route_cpy);
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
                        entry.insert(tx)
                    }
                };
                if let Err(mpsc::error::SendError(act)) = sender.send(act).await {
                    uplink_senders.remove(&addr);
                    action = Some(act);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct LaneIoError {
    route: RelativePath,
    update_error: Option<UpdateError>,
    uplink_errors: Vec<UplinkError>,
}

impl LaneIoError {

    pub fn for_update_err(route: RelativePath, err: UpdateError) -> Self {
        LaneIoError {
            route,
            update_error: Some(err),
            uplink_errors: vec![],
        }
    }

    pub fn for_uplink_errors(route: RelativePath, errs: Vec<UplinkError>) -> Self {
        assert!(!errs.is_empty());
        LaneIoError {
            route,
            update_error: None,
            uplink_errors: errs,
        }
    }

    pub fn new(route: RelativePath, upd: UpdateError, upl: Vec<UplinkError>) -> Self {
        LaneIoError {
            route,
            update_error: Some(upd),
            uplink_errors: upl,
        }
    }

}

impl Display for LaneIoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let LaneIoError { route, update_error, uplink_errors } = self;
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
    let arc_handler = Arc::new(message_handler);

    let act_buffer_size = context.configuration().action_buffer;
    let upd_buffer_size = context.configuration().update_buffer;

    let (mut act_tx, act_rx) = mpsc::channel(act_buffer_size.get());
    let (mut upd_tx, upd_rx) = mpsc::channel(upd_buffer_size.get());

    let spawner = UplinkSpawner::new(
        arc_handler.clone(),
        events,
        context.router_handle(),
        act_rx,
        act_buffer_size,
        route.clone(),
    );

    let update_task = arc_handler.make_update().run_update(upd_rx);

    let uplink_spawn_task = spawner.run(context.spawner());

    let envelope_task = async move {
        pin_mut!(envelopes);

        while let Some(envelope) = envelopes.next().await {
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
                        .is_err() {
                        break;
                    }
                }
                Some(Either::Right(command)) => {
                    if upd_tx
                        .send(Form::try_convert(command))
                        .await
                        .is_err() {
                        break;
                    }
                }
                _ => {
                    //TODO handle incoming messages to support server side downlinks.
                }
            }
        }
    };

    let (_, r2, _) = join3(uplink_spawn_task, update_task, envelope_task)
        .await;
    r2.map_err(|upd_err| LaneIoError::for_update_err(route, upd_err))
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
