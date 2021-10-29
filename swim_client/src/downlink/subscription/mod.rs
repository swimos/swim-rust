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

use swim_runtime::configuration::{BackpressureMode, DownlinksConfig};
use crate::connections::{ConnectionPool, ConnectionType};
use crate::connections::{ConnectionReceiver, ConnectionSender, SwimConnPool};
use crate::downlink::error::SubscriptionError;
use crate::downlink::model::map::UntypedMapModification;
use crate::downlink::model::value::SharedValue;
use crate::downlink::subscription::watch_adapter::KeyedWatch;
use crate::downlink::typed::command::TypedCommandDownlink;
use crate::downlink::typed::event::TypedEventDownlink;
use crate::downlink::typed::map::{MapDownlinkReceiver, TypedMapDownlink};
use crate::downlink::typed::value::{TypedValueDownlink, ValueDownlinkReceiver};
use crate::downlink::typed::{
    UntypedCommandDownlink, UntypedEventDownlink, UntypedMapDownlink, UntypedMapReceiver,
    UntypedValueDownlink, UntypedValueReceiver,
};
use crate::downlink::{
    command_downlink, event_downlink, map_downlink, value_downlink, Command, Downlink,
    DownlinkError, DownlinkKind, Message, SchemaViolations,
};
use crate::router::RouterEvent;
use either::Either;
use futures::FutureExt;
use futures_util::future::TryFutureExt;
use futures_util::select_biased;
use futures_util::stream::{FuturesUnordered, StreamExt};
use pin_utils::pin_mut;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};
use swim_form::Form;
use swim_model::Value;
use swim_model::path::Addressable;
use swim_schema::ValueSchema;
use swim_schema::schema::StandardSchema;
use swim_utilities::future::request::Request;
use swim_utilities::future::item_sink::either::SplitSink;
use swim_utilities::future::item_sink::ItemSender;
use swim_runtime::error::RoutingError;
use swim_runtime::routing::CloseReceiver;
use swim_warp::envelope::Envelope;
use swim_utilities::future::{SwimFutureExt, TransformOnce, TransformedFuture};
use swim_utilities::sync::circular_buffer;
use swim_utilities::trigger::promise::{self, PromiseError};
use swim_runtime::backpressure;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, instrument, trace_span};

pub mod envelopes;
#[cfg(test)]
mod tests;
mod watch_adapter;

#[derive(Clone, Debug)]
pub struct Downlinks<Path: Addressable> {
    downlink_request_tx: mpsc::Sender<DownlinkRequest<Path>>,
}

pub enum DownlinkRequest<Path: Addressable> {
    Subscription(DownlinkSpecifier<Path>),
    DirectCommand { path: Path, envelope: Envelope },
}

impl<Path: Addressable> From<mpsc::error::SendError<DownlinkRequest<Path>>>
for SubscriptionError<Path>
{
    fn from(_: mpsc::error::SendError<DownlinkRequest<Path>>) -> Self {
        SubscriptionError::DownlinkTaskStopped
    }
}

/// Contains all running WARP downlinks and allows requests for downlink subscriptions.
impl<Path: Addressable> Downlinks<Path> {
    /// Create tasks for opening remote connections and attaching them to downlinks.
    #[instrument(skip(connection_pool, config))]
    pub fn new<Cfg>(
        buffer_size: NonZeroUsize,
        connection_pool: SwimConnPool<Path>,
        config: Arc<Cfg>,
        close_rx: CloseReceiver,
    ) -> (Downlinks<Path>, DownlinksTask<Path>)
        where
            Cfg: DownlinksConfig<PathType = Path> + 'static,
    {
        info!("Initialising downlink manager");

        let (downlink_request_tx, downlink_request_rx) = mpsc::channel(buffer_size.get());

        (
            Downlinks {
                downlink_request_tx,
            },
            DownlinksTask::new(config, downlink_request_rx, connection_pool, close_rx),
        )
    }

    pub async fn send_command(&self, path: Path, envelope: Envelope) -> RequestResult<(), Path> {
        self.downlink_request_tx
            .send(DownlinkRequest::DirectCommand { path, envelope })
            .map_err(|_| SubscriptionError::ConnectionError)
            .await
    }

    /// Attempt to subscribe to a value lane. The downlink is returned with a single active
    /// subscription to its events.
    #[instrument(skip(self), level = "info")]
    pub async fn subscribe_value_untyped(
        &self,
        init: Value,
        path: Path,
    ) -> RequestResult<(Arc<UntypedValueDownlink>, UntypedValueReceiver), Path> {
        info!("Subscribing to untyped value lane");

        self.subscribe_value_inner(init, StandardSchema::Anything, path)
            .await
    }

    /// Attempt to subscribe to a remote value lane where the type of the values is described by a
    /// [`ValueSchema`]. The downlink is returned with a single active
    /// subscription to its events.
    #[instrument(skip(self, init), level = "info")]
    pub async fn subscribe_value<T>(
        &self,
        init: T,
        path: Path,
    ) -> RequestResult<(TypedValueDownlink<T>, ValueDownlinkReceiver<T>), Path>
        where
            T: Form + ValueSchema + Send + 'static,
    {
        info!("Subscribing to typed value lane");

        let init_value = init.into_value();
        let (dl, rec) = self
            .subscribe_value_inner(init_value, T::schema(), path)
            .await?;

        Ok((TypedValueDownlink::new(dl), ValueDownlinkReceiver::new(rec)))
    }

    async fn subscribe_value_inner(
        &self,
        init: Value,
        schema: StandardSchema,
        path: Path,
    ) -> RequestResult<(Arc<UntypedValueDownlink>, UntypedValueReceiver), Path> {
        let (tx, rx) = oneshot::channel();
        self.downlink_request_tx
            .send(DownlinkRequest::Subscription(DownlinkSpecifier::Value {
                init,
                path,
                schema,
                request: Request::new(tx),
            }))
            .err_into::<SubscriptionError<Path>>()
            .await?;
        rx.await.map_err(Into::into).and_then(|r| r)
    }

    /// Attempt to subscribe to a map lane. The downlink is returned with a single active
    /// subscription to its events.
    #[instrument(skip(self), level = "info")]
    pub async fn subscribe_map_untyped(
        &self,
        path: Path,
    ) -> RequestResult<(Arc<UntypedMapDownlink>, UntypedMapReceiver), Path> {
        info!("Subscribing to untyped map lane");

        self.subscribe_map_inner(StandardSchema::Anything, StandardSchema::Anything, path)
            .await
    }

    /// Attempt to subscribe to a remote map lane where the types of the keys and values are
    /// described by  [`ValueSchema`]s. The downlink is returned with a single active
    /// subscription to its events.
    #[instrument(skip(self), level = "info")]
    pub async fn subscribe_map<K, V>(
        &self,
        path: Path,
    ) -> RequestResult<(TypedMapDownlink<K, V>, MapDownlinkReceiver<K, V>), Path>
        where
            K: ValueSchema + Send + 'static,
            V: ValueSchema + Send + 'static,
    {
        info!("Subscribing to typed map lane");

        let (dl, rec) = self
            .subscribe_map_inner(K::schema(), V::schema(), path)
            .await?;

        Ok((TypedMapDownlink::new(dl), MapDownlinkReceiver::new(rec)))
    }

    async fn subscribe_map_inner(
        &self,
        key_schema: StandardSchema,
        value_schema: StandardSchema,
        path: Path,
    ) -> RequestResult<(Arc<UntypedMapDownlink>, UntypedMapReceiver), Path> {
        let (tx, rx) = oneshot::channel();
        self.downlink_request_tx
            .send(DownlinkRequest::Subscription(DownlinkSpecifier::Map {
                path,
                key_schema,
                value_schema,
                request: Request::new(tx),
            }))
            .err_into::<SubscriptionError<Path>>()
            .await?;
        rx.await.map_err(Into::into).and_then(|r| r)
    }

    pub async fn subscribe_command_untyped(
        &self,
        path: Path,
    ) -> RequestResult<Arc<UntypedCommandDownlink>, Path> {
        self.subscribe_command_inner(StandardSchema::Anything, path)
            .await
    }

    pub async fn subscribe_command<T>(
        &self,
        path: Path,
    ) -> RequestResult<TypedCommandDownlink<T>, Path>
        where
            T: ValueSchema + Send + 'static,
    {
        Ok(TypedCommandDownlink::new(
            self.subscribe_command_inner(T::schema(), path).await?,
        ))
    }

    async fn subscribe_command_inner(
        &self,
        schema: StandardSchema,
        path: Path,
    ) -> RequestResult<Arc<UntypedCommandDownlink>, Path> {
        let (tx, rx) = oneshot::channel();

        self.downlink_request_tx
            .send(DownlinkRequest::Subscription(DownlinkSpecifier::Command {
                schema,
                path,
                request: Request::new(tx),
            }))
            .err_into::<SubscriptionError<Path>>()
            .await?;

        rx.await.map_err(Into::into).and_then(|r| r)
    }

    pub async fn subscribe_event_untyped(
        &self,
        path: Path,
    ) -> RequestResult<Arc<UntypedEventDownlink>, Path> {
        self.subscribe_event_inner(StandardSchema::Anything, path, SchemaViolations::Ignore)
            .await
    }

    pub async fn subscribe_event<T>(
        &self,
        path: Path,
        violations: SchemaViolations,
    ) -> RequestResult<TypedEventDownlink<T>, Path>
        where
            T: ValueSchema + Send + 'static,
    {
        Ok(TypedEventDownlink::new(
            self.subscribe_event_inner(T::schema(), path, violations)
                .await?,
        ))
    }

    async fn subscribe_event_inner(
        &self,
        schema: StandardSchema,
        path: Path,
        violations: SchemaViolations,
    ) -> RequestResult<Arc<UntypedEventDownlink>, Path> {
        let (tx, rx) = oneshot::channel();

        self.downlink_request_tx
            .send(DownlinkRequest::Subscription(DownlinkSpecifier::Event {
                schema,
                path,
                request: Request::new(tx),
                violations,
            }))
            .err_into::<SubscriptionError<Path>>()
            .await?;

        rx.await.map_err(Into::into).and_then(|r| r)
    }
}

pub type RequestResult<T, Path> = Result<T, SubscriptionError<Path>>;

pub enum DownlinkSpecifier<Path: Addressable> {
    Value {
        init: Value,
        path: Path,
        schema: StandardSchema,
        request: Request<RequestResult<(Arc<UntypedValueDownlink>, UntypedValueReceiver), Path>>,
    },
    Map {
        path: Path,
        key_schema: StandardSchema,
        value_schema: StandardSchema,
        request: Request<RequestResult<(Arc<UntypedMapDownlink>, UntypedMapReceiver), Path>>,
    },
    Command {
        path: Path,
        schema: StandardSchema,
        request: Request<RequestResult<Arc<UntypedCommandDownlink>, Path>>,
    },
    Event {
        path: Path,
        schema: StandardSchema,
        request: Request<RequestResult<Arc<UntypedEventDownlink>, Path>>,
        violations: SchemaViolations,
    },
}

type StopEvents<Path> = FuturesUnordered<
    TransformedFuture<promise::Receiver<Result<(), DownlinkError>>, MakeStopEvent<Path>>,
>;

struct ValueHandle {
    ptr: Weak<UntypedValueDownlink>,
    schema: StandardSchema,
}

impl ValueHandle {
    fn new(ptr: Weak<UntypedValueDownlink>, schema: StandardSchema) -> Self {
        ValueHandle { ptr, schema }
    }
}

struct MapHandle {
    ptr: Weak<UntypedMapDownlink>,
    key_schema: StandardSchema,
    value_schema: StandardSchema,
}

impl MapHandle {
    fn new(
        ptr: Weak<UntypedMapDownlink>,
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

struct CommandHandle {
    dl: Weak<UntypedCommandDownlink>,
    schema: StandardSchema,
}

struct EventHandle {
    dl: Weak<UntypedEventDownlink>,
    schema: StandardSchema,
}

pub struct DownlinksTask<Path: Addressable> {
    config: Arc<dyn DownlinksConfig<PathType = Path>>,
    value_downlinks: HashMap<Path, ValueHandle>,
    map_downlinks: HashMap<Path, MapHandle>,
    command_downlinks: HashMap<Path, CommandHandle>,
    event_downlinks: HashMap<(Path, SchemaViolations), EventHandle>,
    stopped_watch: StopEvents<Path>,
    downlink_request_rx: Option<mpsc::Receiver<DownlinkRequest<Path>>>,
    connection_pool: SwimConnPool<Path>,
    close_rx: CloseReceiver,
}

/// Event that is generated after a downlink stops to allow it to be cleaned up.
struct DownlinkStoppedEvent<Path: Addressable> {
    kind: DownlinkKind,
    path: Path,
    error: Option<DownlinkError>,
}

struct MakeStopEvent<Path: Addressable> {
    kind: DownlinkKind,
    path: Path,
}

impl<Path: Addressable> MakeStopEvent<Path> {
    fn new(kind: DownlinkKind, path: Path) -> Self {
        MakeStopEvent { kind, path }
    }
}

impl<Path: Addressable> TransformOnce<Result<Arc<Result<(), DownlinkError>>, PromiseError>>
for MakeStopEvent<Path>
{
    type Out = DownlinkStoppedEvent<Path>;

    fn transform(self, input: Result<Arc<Result<(), DownlinkError>>, PromiseError>) -> Self::Out {
        let MakeStopEvent { kind, path } = self;
        let error = match input {
            Ok(r) => (*r).clone().err(),
            _ => Some(DownlinkError::DroppedChannel),
        };
        DownlinkStoppedEvent { kind, path, error }
    }
}

impl<Path: Addressable> DownlinksTask<Path> {
    pub(crate) fn new<C>(
        config: Arc<C>,
        downlink_request_rx: mpsc::Receiver<DownlinkRequest<Path>>,
        connection_pool: SwimConnPool<Path>,
        close_rx: CloseReceiver,
    ) -> DownlinksTask<Path>
        where
            C: DownlinksConfig<PathType = Path> + 'static,
    {
        DownlinksTask {
            config,
            value_downlinks: HashMap::new(),
            map_downlinks: HashMap::new(),
            command_downlinks: HashMap::new(),
            event_downlinks: HashMap::new(),
            stopped_watch: StopEvents::new(),
            downlink_request_rx: Some(downlink_request_rx),
            connection_pool,
            close_rx,
        }
    }

    pub async fn run(mut self) -> RequestResult<(), Path> {
        let requests = ReceiverStream::new(self.downlink_request_rx.take().unwrap());
        pin_mut!(requests);

        let mut pinned_requests = requests.fuse();
        let mut close_trigger = self.close_rx.clone().fuse();

        loop {
            let item: Option<Either<DownlinkRequest<Path>, DownlinkStoppedEvent<Path>>> =
                if self.stopped_watch.is_empty() {
                    select_biased! {
                        _stop = close_trigger => None,
                        maybe_req = pinned_requests.next() => maybe_req.map(Either::Left),
                    }
                } else {
                    select_biased! {
                         _stop = close_trigger => None,
                        maybe_req = pinned_requests.next() => maybe_req.map(Either::Left),
                        maybe_closed = self.stopped_watch.next() => maybe_closed.map(Either::Right),
                    }
                };

            match item {
                Some(Either::Left(left)) => match left {
                    DownlinkRequest::Subscription(DownlinkSpecifier::Value {
                                                      init,
                                                      path,
                                                      schema,
                                                      request,
                                                  }) => {
                        self.handle_value_request(init, path, schema, request)
                            .await?;
                    }

                    DownlinkRequest::Subscription(DownlinkSpecifier::Map {
                                                      path,
                                                      key_schema,
                                                      value_schema,
                                                      request,
                                                  }) => {
                        self.handle_map_request(path, key_schema, value_schema, request)
                            .await?;
                    }

                    DownlinkRequest::Subscription(DownlinkSpecifier::Command {
                                                      path,
                                                      schema,
                                                      request,
                                                  }) => {
                        self.handle_command_request(path, schema, request).await?;
                    }

                    DownlinkRequest::Subscription(DownlinkSpecifier::Event {
                                                      path,
                                                      schema,
                                                      request,
                                                      violations,
                                                  }) => {
                        self.handle_event_request(path, schema, request, violations)
                            .await?;
                    }

                    DownlinkRequest::DirectCommand { path, envelope } => {
                        self.handle_command_message(path, envelope).await?;
                    }
                },
                Some(Either::Right(stop_event)) => {
                    self.handle_stop(stop_event).await;
                }
                None => {
                    break Ok(());
                }
            }
        }
    }

    pub async fn connection_for(
        &mut self,
        path: Path,
    ) -> RequestResult<(ConnectionSender, ConnectionReceiver), Path> {
        let (sender, receiver) = self
            .connection_pool
            .request_connection(path, ConnectionType::Full)
            .await
            .map_err(|_| SubscriptionError::ConnectionError)?
            .map_err(|_| SubscriptionError::ConnectionError)?;

        Ok((sender, receiver.ok_or(SubscriptionError::ConnectionError)?))
    }

    pub async fn sink_for(&mut self, path: Path) -> RequestResult<ConnectionSender, Path> {
        let (sender, _) = self
            .connection_pool
            .request_connection(path, ConnectionType::Outgoing)
            .await??;

        Ok(sender)
    }

    async fn create_new_value_downlink(
        &mut self,
        init: Value,
        schema: StandardSchema,
        path: Path,
    ) -> RequestResult<(Arc<UntypedValueDownlink>, UntypedValueReceiver), Path> {
        let span = trace_span!("value downlink", path = ?path);
        let _g = span.enter();

        let config = self.config.config_for(&path);
        let (sink, incoming) = self.connection_for(path.clone()).await?;
        let schema_cpy = schema.clone();

        let updates = ReceiverStream::new(incoming).map(map_router_events);

        let sink_path = path.clone();
        let cmd_sink = sink.map_err_into().comap(move |cmd: Command<SharedValue>| {
            envelopes::value_envelope(&sink_path, cmd).into()
        });

        let (raw_dl, rec) = match config.back_pressure {
            BackpressureMode::Propagate => {
                value_downlink(init, Some(schema), updates, cmd_sink, (&config).into())
            }
            BackpressureMode::Release {
                input_buffer_size,
                yield_after,
                ..
            } => {
                let (release_tx, release_rx) = circular_buffer::channel(input_buffer_size);

                let release_task =
                    backpressure::release_pressure(release_rx, cmd_sink.clone(), yield_after);
                //TODO Use a Spawner instead.
                swim_async_runtime::task::spawn(release_task);

                let pressure_release = release_tx.map_err_into();

                let either_sink = SplitSink::new(cmd_sink, pressure_release).comap(
                    move |cmd: Command<SharedValue>| match cmd {
                        act @ Command::Action(_) => Either::Right(act),
                        ow => Either::Left(ow),
                    },
                );

                value_downlink(init, Some(schema), updates, either_sink, (&config).into())
            }
        };

        let dl = Arc::new(raw_dl);

        self.value_downlinks.insert(
            path.clone(),
            ValueHandle::new(Arc::downgrade(&dl), schema_cpy),
        );
        self.stopped_watch.push(
            dl.await_stopped()
                .transform(MakeStopEvent::new(DownlinkKind::Value, path)),
        );
        Ok((dl, rec))
    }

    async fn create_new_map_downlink(
        &mut self,
        path: Path,
        key_schema: StandardSchema,
        value_schema: StandardSchema,
    ) -> RequestResult<(Arc<UntypedMapDownlink>, UntypedMapReceiver), Path> {
        let span = trace_span!("map downlink", path = ?path);
        let _g = span.enter();

        let config = self.config.config_for(&path);
        let (sink, incoming) = self.connection_for(path.clone()).await?;
        let key_schema_cpy = key_schema.clone();
        let value_schema_cpy = value_schema.clone();

        let updates = ReceiverStream::new(incoming).map(|e| match e {
            RouterEvent::Message(l) => Ok(envelopes::map::from_envelope(l)),
            RouterEvent::ConnectionClosed => Err(RoutingError::ConnectionError),
            RouterEvent::Unreachable(_) => Err(RoutingError::HostUnreachable),
            RouterEvent::Stopping => Err(RoutingError::RouterDropped),
        });

        let sink_path = path.clone();

        let (raw_dl, rec) = match config.back_pressure {
            BackpressureMode::Propagate => {
                let cmd_sink = sink.comap(move |cmd: Command<UntypedMapModification<Value>>| {
                    envelopes::map_envelope(&sink_path, cmd).into()
                });
                map_downlink(
                    Some(key_schema),
                    Some(value_schema),
                    updates,
                    cmd_sink.map_err_into(),
                    (&config).into(),
                )
            }
            BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
                yield_after,
            } => {
                let sink_path_duplicate = sink_path.clone();
                let direct_sink = sink.clone().map_err_into().comap(
                    move |cmd: Command<UntypedMapModification<Value>>| {
                        envelopes::map_envelope(&sink_path_duplicate, cmd).into()
                    },
                );
                let action_sink =
                    sink.map_err_into()
                        .comap(move |act: UntypedMapModification<Value>| {
                            envelopes::map_envelope(&sink_path, Command::Action(act)).into()
                        });

                let pressure_release = KeyedWatch::new(
                    action_sink,
                    input_buffer_size,
                    bridge_buffer_size,
                    max_active_keys,
                    yield_after,
                )
                    .await;

                let either_sink = SplitSink::new(direct_sink, pressure_release.into_item_sender())
                    .comap(
                        move |cmd: Command<UntypedMapModification<Value>>| match cmd {
                            Command::Action(act) => Either::Right(act),
                            ow => Either::Left(ow),
                        },
                    );
                map_downlink(
                    Some(key_schema),
                    Some(value_schema),
                    updates,
                    either_sink.map_err_into(),
                    (&config).into(),
                )
            }
        };

        let dl = Arc::new(raw_dl);

        self.map_downlinks.insert(
            path.clone(),
            MapHandle::new(Arc::downgrade(&dl), key_schema_cpy, value_schema_cpy),
        );
        self.stopped_watch.push(
            dl.await_stopped()
                .transform(MakeStopEvent::new(DownlinkKind::Map, path)),
        );
        Ok((dl, rec))
    }

    async fn create_new_command_downlink(
        &mut self,
        path: Path,
        schema: StandardSchema,
    ) -> RequestResult<Arc<UntypedCommandDownlink>, Path> {
        let sink = self.sink_for(path.clone()).await?;

        let config = self.config.config_for(&path);
        let sink_path = path.clone();

        let cmd_sink = sink
            .map_err_into()
            .comap(move |cmd: Command<Value>| envelopes::command_envelope(&sink_path, cmd).into());

        let dl = match config.back_pressure {
            BackpressureMode::Propagate => {
                Arc::new(command_downlink(schema.clone(), cmd_sink, (&config).into()))
            }

            BackpressureMode::Release {
                input_buffer_size,
                yield_after,
                ..
            } => {
                let (release_tx, release_rx) = circular_buffer::channel(input_buffer_size);

                let release_task =
                    backpressure::release_pressure(release_rx, cmd_sink.clone(), yield_after);
                //TODO Use a Spawner instead.
                swim_async_runtime::task::spawn(release_task);
                let pressure_release = release_tx.map_err_into();
                let either_sink =
                    SplitSink::new(cmd_sink, pressure_release).comap(move |cmd: Command<Value>| {
                        match cmd {
                            act @ Command::Action(_) => Either::Right(act),
                            ow => Either::Left(ow),
                        }
                    });

                Arc::new(command_downlink(
                    schema.clone(),
                    either_sink.map_err_into(),
                    (&config).into(),
                ))
            }
        };

        self.command_downlinks.insert(
            path,
            CommandHandle {
                dl: Arc::downgrade(&dl),
                schema,
            },
        );

        Ok(dl)
    }

    async fn create_new_event_downlink(
        &mut self,
        path: Path,
        schema: StandardSchema,
        violations: SchemaViolations,
    ) -> RequestResult<Arc<UntypedEventDownlink>, Path> {
        let (sink, incoming) = self.connection_for(path.clone()).await?;

        let updates = ReceiverStream::new(incoming).map(map_router_events);

        let config = self.config.config_for(&path);

        let path_cpy = path.clone();
        let cmd_sink = sink
            .map_err_into()
            .comap(move |cmd: Command<()>| envelopes::dummy_envelope(&path_cpy, cmd).into());

        let (raw_dl, _) = event_downlink(
            schema.clone(),
            violations,
            updates,
            cmd_sink,
            (&config).into(),
        );

        let dl = Arc::new(raw_dl);

        self.event_downlinks.insert(
            (path, violations),
            EventHandle {
                dl: Arc::downgrade(&dl),
                schema,
            },
        );

        Ok(dl)
    }

    async fn handle_value_request(
        &mut self,
        init: Value,
        path: Path,
        schema: StandardSchema,
        value_req: Request<RequestResult<(Arc<UntypedValueDownlink>, UntypedValueReceiver), Path>>,
    ) -> RequestResult<(), Path> {
        let dl = match self.value_downlinks.get(&path) {
            Some(ValueHandle {
                     ptr: dl,
                     schema: existing_schema,
                 }) => {
                let maybe_dl = dl.upgrade();
                match maybe_dl {
                    Some(dl_clone) if dl_clone.is_running() => {
                        if schema.eq(existing_schema) {
                            if let Some(rec) = dl_clone.subscribe() {
                                Ok((dl_clone, rec))
                            } else {
                                self.value_downlinks.remove(&path);
                                Ok(self
                                    .create_new_value_downlink(init, schema, path.clone())
                                    .await?)
                            }
                        } else {
                            Err(SubscriptionError::incompatible_value(
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
        path: Path,
        key_schema: StandardSchema,
        value_schema: StandardSchema,
        map_req: Request<RequestResult<(Arc<UntypedMapDownlink>, UntypedMapReceiver), Path>>,
    ) -> RequestResult<(), Path> {
        let dl = match self.map_downlinks.get(&path) {
            Some(MapHandle {
                     ptr: dl,
                     key_schema: existing_key_schema,
                     value_schema: existing_value_schema,
                 }) => {
                if !key_schema.eq(existing_key_schema) {
                    Err(SubscriptionError::incompatible_map_key(
                        path,
                        existing_key_schema.clone(),
                        key_schema,
                    ))
                } else if !value_schema.eq(existing_value_schema) {
                    Err(SubscriptionError::incompatible_map_value(
                        path,
                        existing_value_schema.clone(),
                        value_schema,
                    ))
                } else {
                    let maybe_dl = dl.upgrade();
                    match maybe_dl {
                        Some(dl_clone) if dl_clone.is_running() => {
                            if let Some(rec) = dl_clone.subscribe() {
                                Ok((dl_clone, rec))
                            } else {
                                self.map_downlinks.remove(&path);
                                Ok(self
                                    .create_new_map_downlink(path.clone(), key_schema, value_schema)
                                    .await?)
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

    async fn handle_command_request(
        &mut self,
        path: Path,
        schema: StandardSchema,
        value_req: Request<RequestResult<Arc<UntypedCommandDownlink>, Path>>,
    ) -> RequestResult<(), Path> {
        let downlink = match self.command_downlinks.get(&path) {
            Some(CommandHandle {
                     dl,
                     schema: existing_schema,
                 }) => {
                let maybe_dl = dl.upgrade();
                match maybe_dl {
                    Some(dl) if dl.is_running() => {
                        if !schema.eq(existing_schema) {
                            Err(SubscriptionError::incompatible_value(
                                path,
                                existing_schema.clone(),
                                schema,
                            ))
                        } else {
                            Ok(dl)
                        }
                    }
                    _ => {
                        self.command_downlinks.remove(&path);
                        Ok(self
                            .create_new_command_downlink(path.clone(), schema)
                            .await?)
                    }
                }
            }
            _ => self.create_new_command_downlink(path.clone(), schema).await,
        };

        let _ = value_req.send(downlink);
        Ok(())
    }

    async fn handle_event_request(
        &mut self,
        path: Path,
        schema: StandardSchema,
        value_req: Request<RequestResult<Arc<UntypedEventDownlink>, Path>>,
        violations: SchemaViolations,
    ) -> RequestResult<(), Path> {
        let dl = match self.event_downlinks.get(&(path.clone(), violations)) {
            Some(EventHandle {
                     dl,
                     schema: existing_schema,
                 }) => {
                let maybe_dl = dl.upgrade();
                match maybe_dl {
                    Some(dl_clone) if dl_clone.is_running() => {
                        if schema.eq(existing_schema) {
                            Ok(dl_clone)
                        } else {
                            Err(SubscriptionError::incompatible_value(
                                path,
                                existing_schema.clone(),
                                schema,
                            ))
                        }
                    }
                    _ => {
                        self.event_downlinks.remove(&(path.clone(), violations));
                        Ok(self
                            .create_new_event_downlink(path, schema, violations)
                            .await?)
                    }
                }
            }
            _ => {
                self.create_new_event_downlink(path.clone(), schema, violations)
                    .await
            }
        };
        let _ = value_req.send(dl);
        Ok(())
    }

    #[instrument(skip(self, stop_event))]
    async fn handle_stop(&mut self, stop_event: DownlinkStoppedEvent<Path>) {
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
            DownlinkKind::Command => {
                if let Some(CommandHandle { dl: weak_dl, .. }) =
                self.command_downlinks.get(&stop_event.path)
                {
                    let is_running = weak_dl.upgrade().map(|dl| dl.is_running()).unwrap_or(false);
                    if is_running {
                        self.command_downlinks.remove(&stop_event.path);
                    }
                }
            }
        }
    }

    async fn handle_command_message(
        &mut self,
        path: Path,
        envelope: Envelope,
    ) -> RequestResult<(), Path> {
        let mut sink = self.sink_for(path).await?;
        sink.send_item(envelope)
            .await
            .map_err(|_| SubscriptionError::ConnectionError)
    }
}

fn map_router_events(event: RouterEvent) -> Result<Message<Value>, RoutingError> {
    match event {
        RouterEvent::Message(l) => Ok(envelopes::value::from_envelope(l)),
        RouterEvent::ConnectionClosed => Err(RoutingError::ConnectionError),
        RouterEvent::Unreachable(_) => Err(RoutingError::HostUnreachable),
        RouterEvent::Stopping => Err(RoutingError::RouterDropped),
    }
}
