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

use crate::configuration::downlink::{BackpressureMode, Config, DownlinkKind};
use crate::connections::SwimConnPool;
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
    DownlinkError, Message, SchemaViolations,
};
use crate::router::ClientConnectionsManager;
use crate::router::RouterEvent;
use crate::router::RouterMessageRequest;
use crate::router::TaskManager;
use crate::router::{ClientRequest, ClientRouterFactory};
use crate::router::{CloseSender, RouterConnRequest};
use either::Either;
use futures::future::BoxFuture;
use futures::join;
use futures::stream::Fuse;
use futures::Stream;
use futures_util::future::TryFutureExt;
use futures_util::select_biased;
use futures_util::stream::{FuturesUnordered, StreamExt};
use pin_utils::pin_mut;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use swim_common::form::ValidatedForm;
use swim_common::model::schema::StandardSchema;
use swim_common::model::Value;
use swim_common::request::Request;
use swim_common::routing::error::RoutingError;
use swim_common::routing::remote::config::ConnectionConfig;
use swim_common::routing::remote::net::dns::Resolver;
use swim_common::routing::remote::net::plain::TokioPlainTextNetworking;
use swim_common::routing::remote::{
    ExternalConnections, Listener, RemoteConnectionChannels, RemoteConnectionsTask,
    RemoteRoutingRequest,
};
use swim_common::routing::ws::tungstenite::TungsteniteWsConnections;
use swim_common::routing::ws::WsConnections;
use swim_common::routing::{ConnectionDropped, Router, RouterFactory, RoutingAddr};
use swim_common::sink::item;
use swim_common::sink::item::either::SplitSink;
use swim_common::sink::item::ItemSender;
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::{AbsolutePath, Addressable};
use swim_runtime::task::{spawn, TaskError, TaskHandle};
use swim_warp::backpressure;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tracing::{error, info, instrument, trace_span};
use utilities::future::open_ended::OpenEndedFutures;
use utilities::future::{SwimFutureExt, TransformOnce, TransformedFuture};
use utilities::sync::promise::PromiseError;
use utilities::sync::{circular_buffer, promise, trigger};
use utilities::task::Spawner;

pub mod envelopes;
#[cfg(test)]
mod tests;
mod watch_adapter;

#[derive(Clone, Debug)]
pub struct Downlinks<Path: Addressable> {
    sender: mpsc::Sender<DownlinkRequest<Path>>,
}

pub struct DownlinksHandle<Path: Addressable> {
    pub downlinks_task: DownlinksTask<Path>,
    pub request_receiver: mpsc::Receiver<DownlinkRequest<Path>>,
    pub task_manager: TaskManager<SwimConnPool<Path>, Path>,
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
    #[instrument(skip(client_conn_request_tx, config))]
    pub fn new<Cfg>(
        client_conn_request_tx: mpsc::Sender<ClientRequest<Path>>,
        config: Arc<Cfg>,
    ) -> (Downlinks<Path>, DownlinksHandle<Path>)
    where
        Cfg: Config<PathType = Path> + 'static,
    {
        info!("Initialising downlink manager");

        let client_params = config.client_params();

        let (task_manager_close_tx, task_manager_close_rx) = promise::promise();

        let connection_pool =
            SwimConnPool::new(client_params.conn_pool_params, client_conn_request_tx);

        let (task_manager, connection_request_tx, router_sink_tx) = TaskManager::new(
            connection_pool,
            task_manager_close_rx,
            client_params.router_params,
        );

        let downlinks_task = DownlinksTask::new(
            config,
            connection_request_tx,
            router_sink_tx,
            task_manager_close_tx,
        );
        let (tx, rx) = mpsc::channel(client_params.dl_req_buffer_size.get());

        (
            Downlinks { sender: tx },
            DownlinksHandle {
                downlinks_task,
                request_receiver: rx,
                task_manager,
            },
        )
    }

    pub async fn send_command(&self, path: Path, envelope: Envelope) -> RequestResult<(), Path> {
        self.sender
            .send(DownlinkRequest::DirectCommand { path, envelope })
            .map_err(|_| SubscriptionError::ConnectionError)
            .await?;

        Ok(())
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
    /// [`ValidatedForm`]. The downlink is returned with a single active
    /// subscription to its events.
    #[instrument(skip(self, init), level = "info")]
    pub async fn subscribe_value<T>(
        &self,
        init: T,
        path: Path,
    ) -> RequestResult<(TypedValueDownlink<T>, ValueDownlinkReceiver<T>), Path>
    where
        T: ValidatedForm + Send + 'static,
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
        self.sender
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
    /// described by  [`ValidatedForm`]s. The downlink is returned with a single active
    /// subscription to its events.
    #[instrument(skip(self), level = "info")]
    pub async fn subscribe_map<K, V>(
        &self,
        path: Path,
    ) -> RequestResult<(TypedMapDownlink<K, V>, MapDownlinkReceiver<K, V>), Path>
    where
        K: ValidatedForm + Send + 'static,
        V: ValidatedForm + Send + 'static,
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
        self.sender
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
        T: ValidatedForm + Send + 'static,
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

        self.sender
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
        T: ValidatedForm + Send + 'static,
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

        self.sender
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

pub type RequestResult<T, Path: Addressable> = Result<T, SubscriptionError<Path>>;

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

type StopEvents<Path: Addressable> = FuturesUnordered<
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
    config: Arc<dyn Config<PathType = Path>>,
    value_downlinks: HashMap<Path, ValueHandle>,
    map_downlinks: HashMap<Path, MapHandle>,
    command_downlinks: HashMap<Path, CommandHandle>,
    event_downlinks: HashMap<(Path, SchemaViolations), EventHandle>,
    stopped_watch: StopEvents<Path>,
    conn_request_tx: mpsc::Sender<RouterConnRequest<Path>>,
    sink_tx: mpsc::Sender<RouterMessageRequest<Path>>,
    close_tx: CloseSender,
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
    pub fn new<C>(
        config: Arc<C>,
        conn_request_tx: mpsc::Sender<RouterConnRequest<Path>>,
        sink_tx: mpsc::Sender<RouterMessageRequest<Path>>,
        close_tx: CloseSender,
    ) -> DownlinksTask<Path>
    where
        C: Config<PathType = Path> + 'static,
    {
        DownlinksTask {
            config,
            value_downlinks: HashMap::new(),
            map_downlinks: HashMap::new(),
            command_downlinks: HashMap::new(),
            event_downlinks: HashMap::new(),
            stopped_watch: StopEvents::new(),
            conn_request_tx,
            sink_tx,
            close_tx,
        }
    }

    pub async fn run<Req>(mut self, requests: Req) -> RequestResult<(), Path>
    where
        Req: Stream<Item = DownlinkRequest<Path>>,
    {
        pin_mut!(requests);

        let mut pinned_requests: Fuse<Pin<&mut Req>> = requests.fuse();

        loop {
            let item: Option<Either<DownlinkRequest<Path>, DownlinkStoppedEvent<Path>>> =
                if self.stopped_watch.is_empty() {
                    pinned_requests.next().await.map(Either::Left)
                } else {
                    select_biased! {
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
                None => break Ok(()),
            }
        }
    }

    pub async fn connection_for(
        &mut self,
        path: &Path,
    ) -> RequestResult<(mpsc::Sender<Envelope>, mpsc::Receiver<RouterEvent>), Path> {
        let (tx, rx) = oneshot::channel();

        self.conn_request_tx
            .send((path.clone(), tx))
            .await
            .map_err(|_| SubscriptionError::ConnectionError)?;

        rx.await.map_err(|_| SubscriptionError::ConnectionError)
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
        let (sink, incoming) = self.connection_for(&path).await?;
        let schema_cpy = schema.clone();

        let updates = ReceiverStream::new(incoming).map(map_router_events);

        let sink_path = path.clone();
        let cmd_sink =
            item::for_mpsc_sender(sink)
                .map_err_into()
                .comap(move |cmd: Command<SharedValue>| {
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
                swim_runtime::task::spawn(release_task);

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
        let (sink, incoming) = self.connection_for(&path).await?;
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
                let cmd_sink = item::for_mpsc_sender(sink).comap(
                    move |cmd: Command<UntypedMapModification<Value>>| {
                        envelopes::map_envelope(&sink_path, cmd).into()
                    },
                );
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
                let direct_sink = item::for_mpsc_sender(sink.clone()).map_err_into().comap(
                    move |cmd: Command<UntypedMapModification<Value>>| {
                        envelopes::map_envelope(&sink_path_duplicate, cmd).into()
                    },
                );
                let action_sink = item::for_mpsc_sender(sink).map_err_into().comap(
                    move |act: UntypedMapModification<Value>| {
                        envelopes::map_envelope(&sink_path, Command::Action(act)).into()
                    },
                );

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
        //Todo dm get only sink
        let (sink, _) = self.connection_for(&path).await?;

        let config = self.config.config_for(&path);

        let sink_path = path.clone();

        let cmd_sink = item::for_mpsc_sender(sink)
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
                swim_runtime::task::spawn(release_task);
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
        let (sink, incoming) = self.connection_for(&path).await?;

        let updates = ReceiverStream::new(incoming).map(map_router_events);

        let config = self.config.config_for(&path);

        let path_cpy = path.clone();
        let cmd_sink = item::for_mpsc_sender(sink)
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
        //Todo dm get only sink
        let (sink, _) = self.connection_for(&path).await?;
        sink.send(envelope)
            .await
            .map_err(|_| SubscriptionError::ConnectionError)?;
        Ok(())
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
