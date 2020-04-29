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

use crate::configuration::downlink::{
    BackpressureMode, Config, DownlinkKind, DownlinkParams, MuxMode,
};
use crate::downlink::any::{AnyDownlink, AnyReceiver, AnyWeakDownlink};
use crate::downlink::model::map::{MapAction, MapModification, ViewWithEvent};
use crate::downlink::model::value::{self, Action, SharedValue};
use crate::downlink::watch_adapter::map::KeyedWatch;
use crate::downlink::watch_adapter::value::ValuePump;
use crate::downlink::{Command, DownlinkError, Message, StoppedFuture};
use crate::router::{Router, RouterEvent, RoutingError};
use common::model::Value;
use common::request::Request;
use common::sink::item::either::EitherSink;
use common::sink::item::ItemSender;
use common::topic::Topic;
use common::warp::path::AbsolutePath;
use either::Either;
use futures::stream::Fuse;
use futures::Stream;
use futures_util::future::{ready, TryFutureExt};
use futures_util::select_biased;
use futures_util::stream::{FuturesUnordered, StreamExt};
use pin_utils::pin_mut;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use utilities::future::{SwimFutureExt, Transformation, TransformedFuture};

pub mod envelopes;
#[cfg(test)]
pub mod tests;

pub type ValueDownlink = AnyDownlink<value::Action, SharedValue>;
type WeakValueDownlink = AnyWeakDownlink<value::Action, SharedValue>;
pub type MapDownlink = AnyDownlink<MapAction, ViewWithEvent>;
type WeakMapDownlink = AnyWeakDownlink<MapAction, ViewWithEvent>;

pub type ValueReceiver = AnyReceiver<SharedValue>;
pub type MapReceiver = AnyReceiver<ViewWithEvent>;

pub struct Downlinks {
    sender: mpsc::Sender<DownlinkRequest>,
    _task: JoinHandle<()>,
}

/// Contains all running Warp downlinks and allows requests for downlink subscriptions.
impl Downlinks {
    /// Create a new downlink manager, using the specified configuration, which will attach all
    /// create downlinks to the provided router.
    pub async fn new<C, R>(config: Arc<C>, router: R) -> Downlinks
    where
        C: Config + 'static,
        R: Router + 'static,
    {
        let client_params = config.client_params();
        let task = DownlinkTask::new(config, router);
        let (tx, rx) = mpsc::channel(client_params.dl_req_buffer_size.get());
        let task_handle = tokio::task::spawn(task.run(rx));
        Downlinks {
            sender: tx,
            _task: task_handle,
        }
    }

    /// Attempt to subscribe to a value lane. The downlink is returned with a single active
    /// subscription to its events (if there are ever no subscribers the downlink will stop
    /// running.
    pub async fn subscribe_value(
        &mut self,
        init: Value,
        path: AbsolutePath,
    ) -> Result<(ValueDownlink, ValueReceiver)> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(DownlinkRequest::Value(init, path, Request::new(tx)))
            .err_into::<SubscriptionError>()
            .await?;
        rx.await.map_err(Into::into).and_then(|r| r)
    }

    /// Attempt to subscribe to a map lane. The downlink is returned with a single active
    /// subscription to its events (if there are ever no subscribers the downlink will stop
    /// running.
    pub async fn subscribe_map(
        &mut self,
        path: AbsolutePath,
    ) -> Result<(MapDownlink, MapReceiver)> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(DownlinkRequest::Map(path, Request::new(tx)))
            .err_into::<SubscriptionError>()
            .await?;
        rx.await.map_err(Into::into).and_then(|r| r)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionError {
    BadKind {
        expected: DownlinkKind,
        actual: DownlinkKind,
    },
    DownlinkTaskStopped,
}

impl From<mpsc::error::SendError<DownlinkRequest>> for SubscriptionError {
    fn from(_: SendError<DownlinkRequest>) -> Self {
        SubscriptionError::DownlinkTaskStopped
    }
}

impl From<oneshot::error::RecvError> for SubscriptionError {
    fn from(_: RecvError) -> Self {
        SubscriptionError::DownlinkTaskStopped
    }
}

impl Display for SubscriptionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriptionError::BadKind { expected, actual } => write!(
                f,
                "Requested {} downlink but a {} downlink was already open for that lane.",
                expected, actual
            ),
            SubscriptionError::DownlinkTaskStopped => {
                write!(f, "The downlink task has already stopped.")
            }
        }
    }
}

impl std::error::Error for SubscriptionError {}

impl SubscriptionError {
    pub fn bad_kind(expected: DownlinkKind, actual: DownlinkKind) -> SubscriptionError {
        SubscriptionError::BadKind { expected, actual }
    }
}

pub type Result<T> = std::result::Result<T, SubscriptionError>;

pub enum DownlinkRequest {
    Value(
        Value,
        AbsolutePath,
        Request<Result<(ValueDownlink, ValueReceiver)>>,
    ),
    Map(AbsolutePath, Request<Result<(MapDownlink, MapReceiver)>>),
}

type StopEvents = FuturesUnordered<TransformedFuture<StoppedFuture, MakeStopEvent>>;

struct DownlinkTask<R> {
    config: Arc<dyn Config>,
    value_downlinks: HashMap<AbsolutePath, WeakValueDownlink>,
    map_downlinks: HashMap<AbsolutePath, WeakMapDownlink>,
    stopped_watch: StopEvents,
    router: R,
}

/// Event that is generated after a downlink stops to allow it to be cleaned up.
struct DownlinkStoppedEvent {
    kind: DownlinkKind,
    path: AbsolutePath,
    //TODO Currently ignored, should be logged.
    _error: Option<DownlinkError>,
}

struct MakeStopEvent {
    kind: DownlinkKind,
    path: AbsolutePath,
}

impl MakeStopEvent {
    fn new(kind: DownlinkKind, path: AbsolutePath) -> Self {
        MakeStopEvent { kind, path }
    }
}

impl Transformation<std::result::Result<(), DownlinkError>> for MakeStopEvent {
    type Out = DownlinkStoppedEvent;

    fn transform(self, input: std::result::Result<(), DownlinkError>) -> Self::Out {
        let MakeStopEvent { kind, path } = self;
        DownlinkStoppedEvent {
            kind,
            path,
            _error: input.err(),
        }
    }
}

impl<R> DownlinkTask<R>
where
    R: Router,
{
    fn new<C>(config: Arc<C>, router: R) -> DownlinkTask<R>
    where
        C: Config + 'static,
    {
        DownlinkTask {
            config,
            value_downlinks: HashMap::new(),
            map_downlinks: HashMap::new(),
            stopped_watch: StopEvents::new(),
            router,
        }
    }

    async fn create_new_value_downlink(
        &mut self,
        init: Value,
        path: AbsolutePath,
    ) -> (ValueDownlink, ValueReceiver) {
        let config = self.config.config_for(&path);
        let (sink, incoming) = self.router.connection_for(&path).await;

        //TODO Do something with invalid envelopes rather than discarding them.
        let updates = incoming.filter_map(|event| match event {
            RouterEvent::Envelope(env) => ready(envelopes::value::try_from_envelope(env).ok()),
            _ => ready(None),
        });

        let sink_path = path.clone();
        let cmd_sink = sink
            .comap(move |cmd: Command<SharedValue>| envelopes::value_envelope(&sink_path, cmd).1);

        let (dl, rec) = match config.back_pressure {
            BackpressureMode::Propagate => {
                value_downlink_for_sink(cmd_sink, init, updates, &config)
            }
            BackpressureMode::Release { .. } => {
                let pressure_release = ValuePump::new(cmd_sink.clone()).await;

                let either_sink = EitherSink::new(cmd_sink, pressure_release).comap(
                    move |cmd: Command<SharedValue>| match cmd {
                        act @ Command::Action(_) => Either::Right(act),
                        ow => Either::Left(ow),
                    },
                );

                value_downlink_for_sink(either_sink, init, updates, &config)
            }
        };

        self.value_downlinks.insert(path.clone(), dl.downgrade());
        self.stopped_watch.push(
            dl.await_stopped()
                .transform(MakeStopEvent::new(DownlinkKind::Value, path)),
        );
        (dl, rec)
    }

    async fn create_new_map_downlink(&mut self, path: AbsolutePath) -> (MapDownlink, MapReceiver) {
        let config = self.config.config_for(&path);
        let (sink, incoming) = self.router.connection_for(&path).await;

        //TODO Do something with invalid envelopes rather than discarding them.
        let updates = incoming.filter_map(|event| match event {
            RouterEvent::Envelope(env) => ready(envelopes::map::try_from_envelope(env).ok()),
            _ => ready(None),
        });

        let sink_path = path.clone();

        let (dl, rec) = match config.back_pressure {
            BackpressureMode::Propagate => {
                let cmd_sink = sink.comap(move |cmd: Command<MapModification<Arc<Value>>>| {
                    envelopes::map_envelope(&sink_path, cmd).1
                });
                map_downlink_for_sink(cmd_sink, updates, &config)
            }
            BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
            } => {
                let sink_path_duplicate = sink_path.clone();
                let direct_sink =
                    sink.clone()
                        .comap(move |cmd: Command<MapModification<Arc<Value>>>| {
                            envelopes::map_envelope(&sink_path_duplicate, cmd).1
                        });
                let action_sink = sink.comap(move |act: MapModification<Arc<Value>>| {
                    envelopes::map_envelope(&sink_path, Command::Action(act)).1
                });

                let pressure_release = KeyedWatch::new(
                    action_sink,
                    input_buffer_size,
                    bridge_buffer_size,
                    max_active_keys,
                )
                .await;

                let either_sink = EitherSink::new(direct_sink, pressure_release).comap(
                    move |cmd: Command<MapModification<Arc<Value>>>| match cmd {
                        Command::Action(act) => Either::Right(act),
                        ow => Either::Left(ow),
                    },
                );
                map_downlink_for_sink(either_sink, updates, &config)
            }
        };

        self.map_downlinks.insert(path.clone(), dl.downgrade());
        self.stopped_watch.push(
            dl.await_stopped()
                .transform(MakeStopEvent::new(DownlinkKind::Map, path)),
        );
        (dl, rec)
    }

    #[allow(clippy::cognitive_complexity)]
    async fn run<Req>(mut self, requests: Req)
    where
        Req: Stream<Item = DownlinkRequest>,
    {
        pin_mut!(requests);

        let mut pinned_requests: Fuse<Pin<&mut Req>> = requests.fuse();

        loop {
            let item: Option<Either<DownlinkRequest, DownlinkStoppedEvent>> =
                if self.stopped_watch.is_empty() {
                    pinned_requests.next().await.map(Either::Left)
                } else {
                    select_biased! {
                        maybe_req = pinned_requests.next() => maybe_req.map(Either::Left),
                        maybe_closed = self.stopped_watch.next() => maybe_closed.map(Either::Right),
                    }
                };

            match item {
                Some(Either::Left(DownlinkRequest::Value(init, path, value_req))) => {
                    let dl = match self.value_downlinks.get(&path) {
                        Some(dl) => {
                            let maybe_dl = dl.upgrade();
                            match maybe_dl {
                                Some(mut dl_clone) if dl_clone.is_running() => {
                                    match dl_clone.subscribe().await {
                                        Ok(rec) => Ok((dl_clone, rec)),
                                        Err(_) => {
                                            self.value_downlinks.remove(&path);
                                            Ok(self
                                                .create_new_value_downlink(init, path.clone())
                                                .await)
                                        }
                                    }
                                }
                                _ => {
                                    self.value_downlinks.remove(&path);
                                    Ok(self.create_new_value_downlink(init, path.clone()).await)
                                }
                            }
                        }
                        _ => match self.map_downlinks.get(&path) {
                            Some(_) => Err(SubscriptionError::bad_kind(
                                DownlinkKind::Value,
                                DownlinkKind::Map,
                            )),
                            _ => Ok(self.create_new_value_downlink(init, path.clone()).await),
                        },
                    };
                    let _ = value_req.send(dl);
                }
                Some(Either::Left(DownlinkRequest::Map(path, map_req))) => {
                    let dl = match self.map_downlinks.get(&path) {
                        Some(dl) => {
                            let maybe_dl = dl.upgrade();
                            match maybe_dl {
                                Some(mut dl_clone) if dl_clone.is_running() => {
                                    match dl_clone.subscribe().await {
                                        Ok(rec) => Ok((dl_clone, rec)),
                                        Err(_) => {
                                            self.map_downlinks.remove(&path);
                                            Ok(self.create_new_map_downlink(path.clone()).await)
                                        }
                                    }
                                }
                                _ => {
                                    self.map_downlinks.remove(&path);
                                    Ok(self.create_new_map_downlink(path.clone()).await)
                                }
                            }
                        }
                        _ => match self.value_downlinks.get(&path) {
                            Some(_) => Err(SubscriptionError::bad_kind(
                                DownlinkKind::Map,
                                DownlinkKind::Value,
                            )),
                            _ => Ok(self.create_new_map_downlink(path.clone()).await),
                        },
                    };
                    let _ = map_req.send(dl);
                }
                Some(Either::Right(stop_event)) => match stop_event.kind {
                    DownlinkKind::Value => {
                        if let Some(weak_dl) = self.value_downlinks.get(&stop_event.path) {
                            let is_running =
                                weak_dl.upgrade().map(|dl| dl.is_running()).unwrap_or(false);
                            if !is_running {
                                self.value_downlinks.remove(&stop_event.path);
                            }
                        }
                    }
                    DownlinkKind::Map => {
                        if let Some(weak_dl) = self.map_downlinks.get(&stop_event.path) {
                            let is_running =
                                weak_dl.upgrade().map(|dl| dl.is_running()).unwrap_or(false);
                            if !is_running {
                                self.map_downlinks.remove(&stop_event.path);
                            }
                        }
                    }
                },
                None => {
                    break;
                }
            }
        }
    }
}

fn value_downlink_for_sink<Updates, Snk>(
    cmd_sink: Snk,
    init: Value,
    updates: Updates,
    config: &DownlinkParams,
) -> (AnyDownlink<Action, SharedValue>, AnyReceiver<SharedValue>)
where
    Updates: Stream<Item = Message<Value>> + Send + 'static,
    Snk: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    let buffer_size = config.buffer_size.get();
    let dl_cmd_sink = cmd_sink.map_err_into();
    match config.mux_mode {
        MuxMode::Queue(n) => {
            let (dl, rec) = value::create_queue_downlink(
                init,
                None,
                updates,
                dl_cmd_sink,
                buffer_size,
                n.get(),
                config.on_invalid,
            );
            (AnyDownlink::Queue(dl), AnyReceiver::Queue(rec))
        }
        MuxMode::Dropping => {
            let (dl, rec) = value::create_dropping_downlink(
                init,
                None,
                updates,
                dl_cmd_sink,
                buffer_size,
                config.on_invalid,
            );
            (AnyDownlink::Dropping(dl), AnyReceiver::Dropping(rec))
        }
        MuxMode::Buffered(n) => {
            let (dl, rec) = value::create_buffered_downlink(
                init,
                None,
                updates,
                dl_cmd_sink,
                buffer_size,
                n.get(),
                config.on_invalid,
            );
            (AnyDownlink::Buffered(dl), AnyReceiver::Buffered(rec))
        }
    }
}

fn map_downlink_for_sink<Updates, Snk>(
    cmd_sink: Snk,
    updates: Updates,
    config: &DownlinkParams,
) -> (
    AnyDownlink<MapAction, ViewWithEvent>,
    AnyReceiver<ViewWithEvent>,
)
where
    Updates: Stream<Item = Message<MapModification<Value>>> + Send + 'static,
    Snk: ItemSender<Command<MapModification<Arc<Value>>>, RoutingError> + Send + 'static,
{
    use crate::downlink::model::map::*;
    let buffer_size = config.buffer_size.get();
    let dl_cmd_sink = cmd_sink.map_err_into();
    match config.mux_mode {
        MuxMode::Queue(n) => {
            let (dl, rec) = create_queue_downlink(
                None,
                None,
                updates,
                dl_cmd_sink,
                buffer_size,
                n.get(),
                config.on_invalid,
            );
            (AnyDownlink::Queue(dl), AnyReceiver::Queue(rec))
        }
        MuxMode::Dropping => {
            let (dl, rec) = create_dropping_downlink(
                None,
                None,
                updates,
                dl_cmd_sink,
                buffer_size,
                config.on_invalid,
            );
            (AnyDownlink::Dropping(dl), AnyReceiver::Dropping(rec))
        }
        MuxMode::Buffered(n) => {
            let (dl, rec) = create_buffered_downlink(
                None,
                None,
                updates,
                dl_cmd_sink,
                buffer_size,
                n.get(),
                config.on_invalid,
            );
            (AnyDownlink::Buffered(dl), AnyReceiver::Buffered(rec))
        }
    }
}
