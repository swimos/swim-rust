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
use crate::downlink::typed::topic::{ApplyForm, ApplyFormsMap};
use crate::downlink::typed::{MapDownlink, ValueDownlink};
use crate::downlink::watch_adapter::map::KeyedWatch;
use crate::downlink::watch_adapter::value::ValuePump;
use crate::downlink::{Command, DownlinkError, Message, StoppedFuture};
use crate::router::{Router, RouterEvent, RoutingError};
use common::model::schema::StandardSchema;
use common::model::Value;
use common::request::request_future::RequestError;
use common::request::Request;
use common::sink::item::either::EitherSink;
use common::sink::item::ItemSender;
use common::topic::Topic;
use common::warp::path::AbsolutePath;

use either::Either;
use form::ValidatedForm;
use futures::stream::Fuse;
use futures::Stream;
use futures_util::future::TryFutureExt;
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
use tracing::{error, info, instrument, trace_span};

use utilities::future::{SwimFutureExt, TransformOnce, TransformedFuture, UntilFailure};

pub mod envelopes;
#[cfg(test)]
pub mod tests;

pub type AnyValueDownlink = AnyDownlink<value::Action, SharedValue>;
pub type TypedValueDownlink<T> = ValueDownlink<AnyValueDownlink, T>;

pub type AnyMapDownlink = AnyDownlink<MapAction, ViewWithEvent>;
pub type TypedMapDownlink<K, V> = MapDownlink<AnyMapDownlink, K, V>;

pub type ValueReceiver = AnyReceiver<SharedValue>;
pub type TypedValueReceiver<T> = UntilFailure<ValueReceiver, ApplyForm<T>>;
pub type MapReceiver = AnyReceiver<ViewWithEvent>;
pub type TypedMapReceiver<K, V> = UntilFailure<MapReceiver, ApplyFormsMap<K, V>>;

type AnyWeakValueDownlink = AnyWeakDownlink<value::Action, SharedValue>;
type AnyWeakMapDownlink = AnyWeakDownlink<MapAction, ViewWithEvent>;

pub struct Downlinks {
    sender: mpsc::Sender<DownlinkSpecifier>,
    _task: JoinHandle<RequestResult<()>>,
}

/// Contains all running Warp downlinks and allows requests for downlink subscriptions.
impl Downlinks {
    /// Create a new downlink manager, using the specified configuration, which will attach all
    /// create downlinks to the provided router.
    #[instrument(skip(config, router))]
    pub async fn new<C, R>(config: Arc<C>, router: R) -> Downlinks
    where
        C: Config + 'static,
        R: Router + 'static,
    {
        info!("Initialising downlink manager");

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
    /// subscription to its events.
    #[instrument(skip(self), level = "info")]
    pub async fn subscribe_value_untyped(
        &mut self,
        init: Value,
        path: AbsolutePath,
    ) -> RequestResult<(AnyValueDownlink, ValueReceiver)> {
        info!("Subscribing to untyped value lane");

        self.subscribe_value_inner(init, StandardSchema::Anything, path)
            .await
    }

    /// Attempt to subscribe to a remote value lane where the type of the values is described by a
    /// [`ValidatedForm`]. The downlink is returned with a single active
    /// subscription to its events.
    #[instrument(skip(self, init), level = "info")]
    pub async fn subscribe_value<T>(
        &mut self,
        init: T,
        path: AbsolutePath,
    ) -> RequestResult<(TypedValueDownlink<T>, TypedValueReceiver<T>)>
    where
        T: ValidatedForm + Send + 'static,
    {
        info!("Subscribing to typed value lane");

        let init_value = init.into_value();
        let (dl, rec) = self
            .subscribe_value_inner(init_value, T::schema(), path)
            .await?;
        let typed_rec = UntilFailure::new(rec, Default::default());
        Ok((ValueDownlink::new(dl), typed_rec))
    }

    async fn subscribe_value_inner(
        &mut self,
        init: Value,
        schema: StandardSchema,
        path: AbsolutePath,
    ) -> RequestResult<(AnyValueDownlink, ValueReceiver)> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(DownlinkSpecifier::Value {
                init,
                path,
                schema,
                request: Request::new(tx),
            })
            .err_into::<SubscriptionError>()
            .await?;
        rx.await.map_err(Into::into).and_then(|r| r)
    }

    /// Attempt to subscribe to a map lane. The downlink is returned with a single active
    /// subscription to its events.
    #[instrument(skip(self), level = "info")]
    pub async fn subscribe_map_untyped(
        &mut self,
        path: AbsolutePath,
    ) -> RequestResult<(AnyMapDownlink, MapReceiver)> {
        info!("Subscribing to untyped map lane");

        self.subscribe_map_inner(StandardSchema::Anything, StandardSchema::Anything, path)
            .await
    }

    /// Attempt to subscribe to a remote map lane where the types of the keys and values are
    /// described by  [`ValidatedForm`]s. The downlink is returned with a single active
    /// subscription to its events.
    #[instrument(skip(self), level = "info")]
    pub async fn subscribe_map<K, V>(
        &mut self,
        path: AbsolutePath,
    ) -> RequestResult<(TypedMapDownlink<K, V>, TypedMapReceiver<K, V>)>
    where
        K: ValidatedForm + Send + 'static,
        V: ValidatedForm + Send + 'static,
    {
        info!("Subscribing to typed map lane");

        let (dl, rec) = self
            .subscribe_map_inner(K::schema(), V::schema(), path)
            .await?;
        let typed_rec = UntilFailure::new(rec, Default::default());
        Ok((MapDownlink::new(dl), typed_rec))
    }

    async fn subscribe_map_inner(
        &mut self,
        key_schema: StandardSchema,
        value_schema: StandardSchema,
        path: AbsolutePath,
    ) -> RequestResult<(AnyMapDownlink, MapReceiver)> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(DownlinkSpecifier::Map {
                path,
                key_schema,
                value_schema,
                request: Request::new(tx),
            })
            .err_into::<SubscriptionError>()
            .await?;
        rx.await.map_err(Into::into).and_then(|r| r)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SubscriptionError {
    BadKind {
        expected: DownlinkKind,
        actual: DownlinkKind,
    },
    DownlinkTaskStopped,
    IncompatibleValueSchema {
        path: AbsolutePath,
        existing: Box<StandardSchema>,
        requested: Box<StandardSchema>,
    },
    IncompatibleMapSchema {
        is_key: bool,
        path: AbsolutePath,
        existing: Box<StandardSchema>,
        requested: Box<StandardSchema>,
    },
    ConnectionError,
}

impl SubscriptionError {
    pub fn incompatibile_value(
        path: AbsolutePath,
        existing: StandardSchema,
        requested: StandardSchema,
    ) -> Self {
        SubscriptionError::IncompatibleValueSchema {
            path,
            existing: Box::new(existing),
            requested: Box::new(requested),
        }
    }

    pub fn incompatibile_map_key(
        path: AbsolutePath,
        existing: StandardSchema,
        requested: StandardSchema,
    ) -> Self {
        SubscriptionError::IncompatibleMapSchema {
            is_key: true,
            path,
            existing: Box::new(existing),
            requested: Box::new(requested),
        }
    }

    pub fn incompatibile_map_value(
        path: AbsolutePath,
        existing: StandardSchema,
        requested: StandardSchema,
    ) -> Self {
        SubscriptionError::IncompatibleMapSchema {
            is_key: false,
            path,
            existing: Box::new(existing),
            requested: Box::new(requested),
        }
    }
}

impl From<mpsc::error::SendError<DownlinkSpecifier>> for SubscriptionError {
    fn from(_: SendError<DownlinkSpecifier>) -> Self {
        SubscriptionError::DownlinkTaskStopped
    }
}

impl From<oneshot::error::RecvError> for SubscriptionError {
    fn from(_: RecvError) -> Self {
        SubscriptionError::DownlinkTaskStopped
    }
}

impl From<RequestError> for SubscriptionError {
    fn from(_: RequestError) -> Self {
        SubscriptionError::ConnectionError
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
            SubscriptionError::IncompatibleValueSchema {
                path,
                existing,
                requested
            } => {
                write!(f, "A downlink was requested to {} with schema {} but one is already running with schema {}.",
                       path, existing, requested)
            }
            SubscriptionError::IncompatibleMapSchema {
                is_key,
                path,
                existing,
                requested
            } => {
                let key_or_val = if *is_key { "key" } else { "value" };
                write!(f, "A map downlink was requested to {} with {} schema {} but one is already running with schema {}.",
                       path, key_or_val, existing, requested)
            }
            SubscriptionError::ConnectionError => {
                write!(f, "The downlink could not establish a connection.")
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

pub type RequestResult<T> = std::result::Result<T, SubscriptionError>;

pub enum DownlinkSpecifier {
    Value {
        init: Value,
        path: AbsolutePath,
        schema: StandardSchema,
        request: Request<RequestResult<(AnyValueDownlink, ValueReceiver)>>,
    },
    Map {
        path: AbsolutePath,
        key_schema: StandardSchema,
        value_schema: StandardSchema,
        request: Request<RequestResult<(AnyMapDownlink, MapReceiver)>>,
    },
}

type StopEvents = FuturesUnordered<TransformedFuture<StoppedFuture, MakeStopEvent>>;

struct ValueHandle {
    ptr: AnyWeakValueDownlink,
    schema: StandardSchema,
}

impl ValueHandle {
    fn new(ptr: AnyWeakValueDownlink, schema: StandardSchema) -> Self {
        ValueHandle { ptr, schema }
    }
}

struct MapHandle {
    ptr: AnyWeakMapDownlink,
    key_schema: StandardSchema,
    value_schema: StandardSchema,
}

impl MapHandle {
    fn new(
        ptr: AnyWeakMapDownlink,
        key_schema: StandardSchema,
        value_schema: StandardSchema,
    ) -> Self {
        MapHandle {
            ptr,
            key_schema,
            value_schema,
        }
    }
}

struct DownlinkTask<R> {
    config: Arc<dyn Config>,
    value_downlinks: HashMap<AbsolutePath, ValueHandle>,
    map_downlinks: HashMap<AbsolutePath, MapHandle>,
    stopped_watch: StopEvents,
    router: R,
}

/// Event that is generated after a downlink stops to allow it to be cleaned up.
struct DownlinkStoppedEvent {
    kind: DownlinkKind,
    path: AbsolutePath,
    error: Option<DownlinkError>,
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

impl TransformOnce<std::result::Result<(), DownlinkError>> for MakeStopEvent {
    type Out = DownlinkStoppedEvent;

    fn transform(self, input: std::result::Result<(), DownlinkError>) -> Self::Out {
        let MakeStopEvent { kind, path } = self;
        DownlinkStoppedEvent {
            kind,
            path,
            error: input.err(),
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
        schema: StandardSchema,
        path: AbsolutePath,
    ) -> RequestResult<(AnyValueDownlink, ValueReceiver)> {
        let span = trace_span!("value downlink", path = ?path);
        let _g = span.enter();

        let config = self.config.config_for(&path);
        let (sink, incoming) = self.router.connection_for(&path).await?;
        let schema_cpy = schema.clone();

        let updates = incoming.map(|e| match e {
            RouterEvent::Message(l) => Ok(envelopes::value::from_envelope(l)),
            RouterEvent::ConnectionClosed => Err(RoutingError::ConnectionError),
            RouterEvent::Unreachable(_) => Err(RoutingError::HostUnreachable),
            RouterEvent::Stopping => Err(RoutingError::RouterDropped),
        });

        let sink_path = path.clone();
        let cmd_sink = sink.comap(move |cmd: Command<SharedValue>| {
            envelopes::value_envelope(&sink_path, cmd).1.into()
        });

        let (dl, rec) = match config.back_pressure {
            BackpressureMode::Propagate => {
                value_downlink_for_sink(cmd_sink, init, schema, updates, &config)
            }
            BackpressureMode::Release { yield_after, .. } => {
                let pressure_release = ValuePump::new(cmd_sink.clone(), yield_after).await;

                let either_sink = EitherSink::new(cmd_sink, pressure_release).comap(
                    move |cmd: Command<SharedValue>| match cmd {
                        act @ Command::Action(_) => Either::Right(act),
                        ow => Either::Left(ow),
                    },
                );

                value_downlink_for_sink(either_sink, init, schema, updates, &config)
            }
        };

        self.value_downlinks
            .insert(path.clone(), ValueHandle::new(dl.downgrade(), schema_cpy));
        self.stopped_watch.push(
            dl.await_stopped()
                .transform(MakeStopEvent::new(DownlinkKind::Value, path)),
        );
        Ok((dl, rec))
    }

    async fn create_new_map_downlink(
        &mut self,
        path: AbsolutePath,
        key_schema: StandardSchema,
        value_schema: StandardSchema,
    ) -> RequestResult<(AnyMapDownlink, MapReceiver)> {
        let span = trace_span!("map downlink", path = ?path);
        let _g = span.enter();

        let config = self.config.config_for(&path);
        let (sink, incoming) = self.router.connection_for(&path).await?;
        let key_schema_cpy = key_schema.clone();
        let value_schema_cpy = value_schema.clone();

        let updates = incoming.map(|e| match e {
            RouterEvent::Message(l) => Ok(envelopes::map::from_envelope(l)),
            RouterEvent::ConnectionClosed => Err(RoutingError::ConnectionError),
            RouterEvent::Unreachable(_) => Err(RoutingError::HostUnreachable),
            RouterEvent::Stopping => Err(RoutingError::RouterDropped),
        });

        let sink_path = path.clone();

        let (dl, rec) = match config.back_pressure {
            BackpressureMode::Propagate => {
                let cmd_sink = sink.comap(move |cmd: Command<MapModification<Arc<Value>>>| {
                    envelopes::map_envelope(&sink_path, cmd).1.into()
                });
                map_downlink_for_sink(key_schema, value_schema, cmd_sink, updates, &config)
            }
            BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
                yield_after,
            } => {
                let sink_path_duplicate = sink_path.clone();
                let direct_sink =
                    sink.clone()
                        .comap(move |cmd: Command<MapModification<Arc<Value>>>| {
                            envelopes::map_envelope(&sink_path_duplicate, cmd).1.into()
                        });
                let action_sink = sink.comap(move |act: MapModification<Arc<Value>>| {
                    envelopes::map_envelope(&sink_path, Command::Action(act))
                        .1
                        .into()
                });

                let pressure_release = KeyedWatch::new(
                    action_sink,
                    input_buffer_size,
                    bridge_buffer_size,
                    max_active_keys,
                    yield_after,
                )
                .await;

                let either_sink = EitherSink::new(direct_sink, pressure_release).comap(
                    move |cmd: Command<MapModification<Arc<Value>>>| match cmd {
                        Command::Action(act) => Either::Right(act),
                        ow => Either::Left(ow),
                    },
                );
                map_downlink_for_sink(key_schema, value_schema, either_sink, updates, &config)
            }
        };

        self.map_downlinks.insert(
            path.clone(),
            MapHandle::new(dl.downgrade(), key_schema_cpy, value_schema_cpy),
        );
        self.stopped_watch.push(
            dl.await_stopped()
                .transform(MakeStopEvent::new(DownlinkKind::Map, path)),
        );
        Ok((dl, rec))
    }

    async fn handle_value_request(
        &mut self,
        init: Value,
        path: AbsolutePath,
        schema: StandardSchema,
        value_req: Request<RequestResult<(AnyValueDownlink, ValueReceiver)>>,
    ) -> RequestResult<()> {
        let dl = match self.value_downlinks.get(&path) {
            Some(ValueHandle {
                ptr: dl,
                schema: existing_schema,
            }) => {
                let maybe_dl = dl.upgrade();
                match maybe_dl {
                    Some(mut dl_clone) if dl_clone.is_running() => {
                        if schema.eq(existing_schema) {
                            match dl_clone.subscribe().await {
                                Ok(rec) => Ok((dl_clone, rec)),
                                Err(_) => {
                                    self.value_downlinks.remove(&path);
                                    Ok(self
                                        .create_new_value_downlink(init, schema, path.clone())
                                        .await?)
                                }
                            }
                        } else {
                            Err(SubscriptionError::incompatibile_value(
                                path,
                                existing_schema.clone(),
                                schema,
                            ))
                        }
                    }
                    _ => {
                        self.value_downlinks.remove(&path);
                        Ok(self
                            .create_new_value_downlink(init, schema, path.clone())
                            .await?)
                    }
                }
            }
            _ => match self.map_downlinks.get(&path) {
                Some(_) => Err(SubscriptionError::bad_kind(
                    DownlinkKind::Value,
                    DownlinkKind::Map,
                )),
                _ => Ok(self
                    .create_new_value_downlink(init, schema, path.clone())
                    .await?),
            },
        };
        let _ = value_req.send(dl);
        Ok(())
    }

    async fn handle_map_request(
        &mut self,
        path: AbsolutePath,
        key_schema: StandardSchema,
        value_schema: StandardSchema,
        map_req: Request<RequestResult<(AnyMapDownlink, MapReceiver)>>,
    ) -> RequestResult<()> {
        let dl = match self.map_downlinks.get(&path) {
            Some(MapHandle {
                ptr: dl,
                key_schema: existing_key_schema,
                value_schema: existing_value_schema,
            }) => {
                if !key_schema.eq(existing_key_schema) {
                    Err(SubscriptionError::incompatibile_map_key(
                        path,
                        existing_key_schema.clone(),
                        key_schema,
                    ))
                } else if !value_schema.eq(existing_value_schema) {
                    Err(SubscriptionError::incompatibile_map_value(
                        path,
                        existing_value_schema.clone(),
                        value_schema,
                    ))
                } else {
                    let maybe_dl = dl.upgrade();
                    match maybe_dl {
                        Some(mut dl_clone) if dl_clone.is_running() => {
                            match dl_clone.subscribe().await {
                                Ok(rec) => Ok((dl_clone, rec)),
                                Err(_) => {
                                    self.map_downlinks.remove(&path);
                                    Ok(self
                                        .create_new_map_downlink(
                                            path.clone(),
                                            key_schema,
                                            value_schema,
                                        )
                                        .await?)
                                }
                            }
                        }
                        _ => {
                            self.map_downlinks.remove(&path);
                            Ok(self
                                .create_new_map_downlink(path.clone(), key_schema, value_schema)
                                .await?)
                        }
                    }
                }
            }
            _ => match self.value_downlinks.get(&path) {
                Some(_) => Err(SubscriptionError::bad_kind(
                    DownlinkKind::Map,
                    DownlinkKind::Value,
                )),
                _ => Ok(self
                    .create_new_map_downlink(path.clone(), key_schema, value_schema)
                    .await?),
            },
        };
        let _ = map_req.send(dl);
        Ok(())
    }

    #[instrument(skip(self, stop_event))]
    async fn handle_stop(&mut self, stop_event: DownlinkStoppedEvent) {
        match &stop_event.error {
            Some(e) => error!("Downlink {} failed with: \"{}\"", stop_event.path, e),
            None => info!("Downlink {} stopped successfully", stop_event.path),
        }

        match stop_event.kind {
            DownlinkKind::Value => {
                if let Some(ValueHandle { ptr: weak_dl, .. }) =
                    self.value_downlinks.get(&stop_event.path)
                {
                    let is_running = weak_dl.upgrade().map(|dl| dl.is_running()).unwrap_or(false);
                    if !is_running {
                        self.value_downlinks.remove(&stop_event.path);
                    }
                }
            }
            DownlinkKind::Map => {
                if let Some(MapHandle { ptr: weak_dl, .. }) =
                    self.map_downlinks.get(&stop_event.path)
                {
                    let is_running = weak_dl.upgrade().map(|dl| dl.is_running()).unwrap_or(false);
                    if !is_running {
                        self.map_downlinks.remove(&stop_event.path);
                    }
                }
            }
        }
    }

    async fn run<Req>(mut self, requests: Req) -> RequestResult<()>
    where
        Req: Stream<Item = DownlinkSpecifier>,
    {
        pin_mut!(requests);

        let mut pinned_requests: Fuse<Pin<&mut Req>> = requests.fuse();

        loop {
            let item: Option<Either<DownlinkSpecifier, DownlinkStoppedEvent>> =
                if self.stopped_watch.is_empty() {
                    pinned_requests.next().await.map(Either::Left)
                } else {
                    select_biased! {
                        maybe_req = pinned_requests.next() => maybe_req.map(Either::Left),
                        maybe_closed = self.stopped_watch.next() => maybe_closed.map(Either::Right),
                    }
                };

            match item {
                Some(Either::Left(DownlinkSpecifier::Value {
                    init,
                    path,
                    schema,
                    request,
                })) => {
                    self.handle_value_request(init, path, schema, request)
                        .await?;
                }
                Some(Either::Left(DownlinkSpecifier::Map {
                    path,
                    key_schema,
                    value_schema,
                    request,
                })) => {
                    self.handle_map_request(path, key_schema, value_schema, request)
                        .await?;
                }
                Some(Either::Right(stop_event)) => {
                    self.handle_stop(stop_event).await;
                }
                None => {
                    break Ok(());
                }
            }
        }
    }
}

fn value_downlink_for_sink<Updates, Snk>(
    cmd_sink: Snk,
    init: Value,
    schema: StandardSchema,
    updates: Updates,
    config: &DownlinkParams,
) -> (AnyDownlink<Action, SharedValue>, AnyReceiver<SharedValue>)
where
    Updates: Stream<Item = Result<Message<Value>, RoutingError>> + Send + 'static,
    Snk: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    let dl_cmd_sink = cmd_sink.map_err_into();
    match config.mux_mode {
        MuxMode::Queue(n) => {
            let (dl, rec) =
                value::create_queue_downlink(init, Some(schema), updates, dl_cmd_sink, n, &config);
            (AnyDownlink::Queue(dl), AnyReceiver::Queue(rec))
        }
        MuxMode::Dropping => {
            let (dl, rec) =
                value::create_dropping_downlink(init, Some(schema), updates, dl_cmd_sink, &config);
            (AnyDownlink::Dropping(dl), AnyReceiver::Dropping(rec))
        }
        MuxMode::Buffered(n) => {
            let (dl, rec) = value::create_buffered_downlink(
                init,
                Some(schema),
                updates,
                dl_cmd_sink,
                n,
                &config,
            );
            (AnyDownlink::Buffered(dl), AnyReceiver::Buffered(rec))
        }
    }
}

type MapItemResult = Result<Message<MapModification<Value>>, RoutingError>;

fn map_downlink_for_sink<Updates, Snk>(
    key_schema: StandardSchema,
    value_schema: StandardSchema,
    cmd_sink: Snk,
    updates: Updates,
    config: &DownlinkParams,
) -> (
    AnyDownlink<MapAction, ViewWithEvent>,
    AnyReceiver<ViewWithEvent>,
)
where
    Updates: Stream<Item = MapItemResult> + Send + 'static,
    Snk: ItemSender<Command<MapModification<Arc<Value>>>, RoutingError> + Send + 'static,
{
    use crate::downlink::model::map::*;
    let dl_cmd_sink = cmd_sink.map_err_into();
    match config.mux_mode {
        MuxMode::Queue(n) => {
            let (dl, rec) = create_queue_downlink(
                Some(key_schema),
                Some(value_schema),
                updates,
                dl_cmd_sink,
                n,
                &config,
            );
            (AnyDownlink::Queue(dl), AnyReceiver::Queue(rec))
        }
        MuxMode::Dropping => {
            let (dl, rec) = create_dropping_downlink(
                Some(key_schema),
                Some(value_schema),
                updates,
                dl_cmd_sink,
                &config,
            );
            (AnyDownlink::Dropping(dl), AnyReceiver::Dropping(rec))
        }
        MuxMode::Buffered(n) => {
            let (dl, rec) = create_buffered_downlink(
                Some(key_schema),
                Some(value_schema),
                updates,
                dl_cmd_sink,
                n,
                &config,
            );
            (AnyDownlink::Buffered(dl), AnyReceiver::Buffered(rec))
        }
    }
}
