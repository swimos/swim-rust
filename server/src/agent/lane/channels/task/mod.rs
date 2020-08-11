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
use crate::agent::lane::channels::uplink::spawn::{UplinkErrorReport, UplinkSpawner};
use crate::agent::lane::channels::uplink::{MapLaneUplink, UplinkAction, ValueLaneUplink};
use crate::agent::lane::channels::{
    AgentExecutionConfig, AgentExecutionContext, InputMessage, LaneMessageHandler, OutputMessage,
    TaggedAction,
};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::ValueLane;
use crate::routing::{RoutingAddr, TaggedEnvelope};
use common::model::Value;
use common::topic::Topic;
use common::warp::envelope::{Envelope, EnvelopeHeader, OutgoingHeader};
use common::warp::path::RelativePath;
use either::Either;
use futures::future::{join3, BoxFuture};
use futures::{select, FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use stm::transaction::RetryManager;
use swim_form::Form;
use tokio::sync::mpsc;

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

pub struct UplinkChannels<Top> {
    events: Top,
    actions: mpsc::Receiver<TaggedAction>,
    errors_collector: mpsc::Sender<UplinkErrorReport>,
}

pub trait LaneUplinks {
    fn make_task<Handler, Top, Context>(
        &self,
        message_handler: Arc<Handler>,
        channels: UplinkChannels<Top>,
        route: RelativePath,
        context: &Context,
    ) -> BoxFuture<'static, ()>
    where
        Handler: LaneMessageHandler + 'static,
        OutputMessage<Handler>: Into<Value>,
        Top: Topic<Handler::Event> + Send + 'static,
        Context: AgentExecutionContext;
}

struct SpawnerUplinkFactory(AgentExecutionConfig);

impl LaneUplinks for SpawnerUplinkFactory {
    fn make_task<Handler, Top, Context>(
        &self,
        message_handler: Arc<Handler>,
        channels: UplinkChannels<Top>,
        route: RelativePath,
        context: &Context,
    ) -> BoxFuture<'static, ()>
    where
        Handler: LaneMessageHandler + 'static,
        OutputMessage<Handler>: Into<Value>,
        Top: Topic<Handler::Event> + Send + 'static,
        Context: AgentExecutionContext,
    {
        let SpawnerUplinkFactory(AgentExecutionConfig {
            action_buffer,
            max_uplink_start_attempts,
            ..
        }) = self;

        let UplinkChannels {
            events,
            actions,
            errors_collector: error_collector,
        } = channels;

        let spawner = UplinkSpawner::new(
            message_handler,
            events,
            actions,
            *action_buffer,
            *max_uplink_start_attempts,
            route,
        );

        spawner
            .run(context.router_handle(), context.spawner(), error_collector)
            .boxed()
    }
}

impl Error for LaneIoError {}

pub async fn run_lane_io<Handler, Uplinks>(
    message_handler: Handler,
    lane_uplinks: Uplinks,
    envelopes: impl Stream<Item = TaggedEnvelope>,
    events: impl Topic<Handler::Event> + Send + 'static,
    config: &AgentExecutionConfig,
    context: &impl AgentExecutionContext,
    route: RelativePath,
) -> Result<(), LaneIoError>
where
    Handler: LaneMessageHandler + 'static,
    Uplinks: LaneUplinks,
    OutputMessage<Handler>: Into<Value>,
    InputMessage<Handler>: Debug + Form,
{
    let envelopes = envelopes.fuse();
    let arc_handler = Arc::new(message_handler);

    let AgentExecutionConfig {
        action_buffer,
        update_buffer,
        uplink_err_buffer,
        max_fatal_uplink_errors,
        ..
    } = config;

    let (mut act_tx, act_rx) = mpsc::channel(action_buffer.get());
    let (mut upd_tx, upd_rx) = mpsc::channel(update_buffer.get());
    let (err_tx, err_rx) = mpsc::channel(uplink_err_buffer.get());

    let spawner_channels = UplinkChannels {
        events,
        actions: act_rx,
        errors_collector: err_tx,
    };

    let update_task = arc_handler.make_update().run_update(upd_rx);

    let uplink_spawn_task =
        lane_uplinks.make_task(arc_handler, spawner_channels, route.clone(), context);

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
                    if num_fatal > *max_fatal_uplink_errors {
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

    fn make_uplink(&self, _addr: RoutingAddr) -> Self::Uplink {
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

    fn make_uplink(&self, _addr: RoutingAddr) -> Self::Uplink {
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
