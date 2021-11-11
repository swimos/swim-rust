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

//! Interface for creating and running Swim client instances.
//!
//! The module provides methods and structures for creating and running Swim client instances.
use crate::configuration::ConfigError;
use crate::configuration::SwimClientConfig;
use crate::connections::SwimConnPool;
use crate::downlink::error::{DownlinkError, SubscriptionError};
use crate::downlink::typed::command::TypedCommandDownlink;
use crate::downlink::typed::event::TypedEventDownlink;
use crate::downlink::typed::map::{MapDownlinkReceiver, TypedMapDownlink};
use crate::downlink::typed::value::{TypedValueDownlink, ValueDownlinkReceiver};
use crate::downlink::typed::{
    UntypedCommandDownlink, UntypedEventDownlink, UntypedMapDownlink, UntypedMapReceiver,
    UntypedValueDownlink, UntypedValueReceiver,
};
use crate::downlink::Downlinks;
use crate::downlink::SchemaViolations;
use crate::router::{ClientRouterFactory, TopLevelClientRouterFactory};
use crate::runtime::task::TaskHandle;
use futures::join;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::io::Read;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_async_runtime::task::{spawn, TaskError};
use swim_form::Form;
use swim_model::path::{AbsolutePath, Addressable};
use swim_model::Value;
use swim_recon::parser::parse_value as parse_single;
use swim_runtime::error::ConnectionError;
use swim_runtime::error::RoutingError;
use swim_runtime::remote::net::dns::Resolver;
use swim_runtime::remote::net::plain::TokioPlainTextNetworking;
use swim_runtime::remote::{RemoteConnectionChannels, RemoteConnectionsTask};
use swim_runtime::routing::CloseSender;
use swim_runtime::ws::tungstenite::TungsteniteWsConnections;

use swim_schema::ValueSchema;
use swim_utilities::future::open_ended::OpenEndedFutures;
use swim_utilities::trigger::promise;
use swim_warp::envelope::Envelope;
use tokio::sync::mpsc;
use tracing::info;

/// Builder to create Swim client instance.
///
/// The builder can be created with default or custom configuration.
/// The custom configuration can be read from a file.
pub struct SwimClientBuilder {
    config: SwimClientConfig,
}

impl SwimClientBuilder {
    /// Create a new client builder with custom configuration.
    ///
    /// # Arguments
    /// * `config` - The custom configuration for the client.
    pub fn new(config: SwimClientConfig) -> Self {
        SwimClientBuilder { config }
    }

    /// Create a new client builder with configuration from a file.
    ///
    /// # Arguments
    /// * `config_file` - Configuration file for the client.
    pub fn new_from_file(mut config_file: File) -> Result<Self, ConfigError> {
        let mut contents = String::new();
        config_file
            .read_to_string(&mut contents)
            .map_err(ConfigError::File)?;

        let config =
            SwimClientConfig::try_from_value(&parse_single(&contents).map_err(ConfigError::Parse)?)
                .map_err(ConfigError::Recognizer)?;

        Ok(SwimClientBuilder { config })
    }

    /// Build the Swim client.
    pub async fn build(self) -> SwimClient<AbsolutePath> {
        let SwimClientBuilder { config } = self;

        info!("Initialising Swim Client");

        let (remote_tx, remote_rx) =
            mpsc::channel(config.remote_connections_config.router_buffer_size.get());

        let (client_tx, client_rx) =
            mpsc::channel(config.remote_connections_config.router_buffer_size.get());

        let top_level_router_fac =
            TopLevelClientRouterFactory::new(client_tx.clone(), remote_tx.clone());

        let client_router_factory =
            ClientRouterFactory::new(client_tx.clone(), top_level_router_fac.clone());
        let (close_tx, close_rx) = promise::promise();

        let remote_connections_task = RemoteConnectionsTask::new_client_task(
            config.remote_connections_config,
            TokioPlainTextNetworking::new(Arc::new(Resolver::new().await)),
            TungsteniteWsConnections {
                config: config.websocket_config,
            },
            top_level_router_fac.clone(),
            OpenEndedFutures::new(),
            RemoteConnectionChannels::new(remote_tx, remote_rx, close_rx.clone()),
        )
        .await;

        // The connection pool handles the connections behind the downlinks
        let (connection_pool, pool_task) = SwimConnPool::new(
            config.downlink_connections_config,
            (client_tx, client_rx),
            client_router_factory,
            close_rx.clone(),
        );

        // The downlinks are state machines and request connections from the pool
        let (downlinks, downlinks_task) = Downlinks::new(
            config.downlink_connections_config.dl_req_buffer_size,
            connection_pool,
            Arc::new(config.downlinks_config),
            close_rx,
        );

        let task_handle = spawn(async {
            join!(
                downlinks_task.run(),
                remote_connections_task.run(),
                pool_task.run(),
            )
        });

        SwimClient {
            inner: ClientContext { downlinks },
            close_buffer_size: config.remote_connections_config.router_buffer_size,
            task_handle,
            stop_trigger: close_tx,
        }
    }

    /// Build the Swim client with default WS factory and configuration.
    pub async fn build_with_default() -> SwimClient<AbsolutePath> {
        SwimClientBuilder::new(SwimClientConfig::default())
            .build()
            .await
    }
}

/// A Swim streaming API client for linking to stateful Web Agents using WARP. The Swim client handles
/// opening downlinks, message routing and connections.
///
/// # Downlinks
/// A downlink provides a virtual, bidirectional stream between the client and the lane of a remote
/// Web Agent. Client connections are multiplexed over a single WebSocket connection to the remote
/// host and network failures during message routing are managed. Swim clients handle multicast
/// event routing to the same remote Web Agent.
///
/// Two downlink types are available:
///
/// - A `ValueDownlink` synchronises a shared real-time value with a remote value lane.
/// - A `MapDownlink` synchronises a shared real-time key-value map with a remote map lane.
///
/// Both value and map downlinks are available in typed and untyped flavours. Typed downlinks must
/// conform to a contract that is imposed by a `Form` implementation and all actions are verified
/// against the provided schema to ensure that its views are consistent.
///
#[derive(Debug)]
pub struct SwimClient<Path: Addressable> {
    inner: ClientContext<Path>,
    close_buffer_size: NonZeroUsize,
    task_handle: ClientTaskHandle<Path>,
    stop_trigger: CloseSender,
}

impl<Path: Addressable> SwimClient<Path> {
    /// Sends a command directly to the provided `target` lane.
    pub async fn send_command<T: Form>(
        &self,
        target: Path,
        message: T,
    ) -> Result<(), ClientError<Path>> {
        self.inner.send_command(target, message).await
    }

    /// Opens a new typed value downlink at the provided path and initialises it with `initial`.
    pub async fn value_downlink<T>(
        &self,
        path: Path,
        initial: T,
    ) -> Result<(TypedValueDownlink<T>, ValueDownlinkReceiver<T>), ClientError<Path>>
    where
        T: Form + ValueSchema + Send + 'static,
    {
        self.inner.value_downlink(path, initial).await
    }

    /// Opens a new typed map downlink at the provided path.
    pub async fn map_downlink<K, V>(
        &self,
        path: Path,
    ) -> Result<(TypedMapDownlink<K, V>, MapDownlinkReceiver<K, V>), ClientError<Path>>
    where
        K: ValueSchema + Send + 'static,
        V: ValueSchema + Send + 'static,
    {
        self.inner.map_downlink(path).await
    }

    /// Opens a new command downlink at the provided path.
    pub async fn command_downlink<T>(
        &self,
        path: Path,
    ) -> Result<TypedCommandDownlink<T>, ClientError<Path>>
    where
        T: ValueSchema + Send + 'static,
    {
        self.inner.command_downlink(path).await
    }

    /// Opens a new event downlink at the provided path.
    pub async fn event_downlink<T>(
        &self,
        path: Path,
        violations: SchemaViolations,
    ) -> Result<TypedEventDownlink<T>, ClientError<Path>>
    where
        T: ValueSchema + Send + 'static,
    {
        self.inner.event_downlink(path, violations).await
    }

    /// Opens a new untyped value downlink at the provided path and initialises it with `initial`
    /// value.
    pub async fn untyped_value_downlink(
        &self,
        path: Path,
        initial: Value,
    ) -> Result<(Arc<UntypedValueDownlink>, UntypedValueReceiver), ClientError<Path>> {
        self.inner.untyped_value_downlink(path, initial).await
    }

    /// Opens a new untyped value downlink at the provided path.
    pub async fn untyped_map_downlink(
        &self,
        path: Path,
    ) -> Result<(Arc<UntypedMapDownlink>, UntypedMapReceiver), ClientError<Path>> {
        self.inner.untyped_map_downlink(path).await
    }

    /// Opens a new untyped command downlink at the provided path.
    pub async fn untyped_command_downlink(
        &self,
        path: Path,
    ) -> Result<Arc<UntypedCommandDownlink>, ClientError<Path>> {
        self.inner.untyped_command_downlink(path).await
    }

    /// Opens a new untyped event downlink at the provided path.
    pub async fn untyped_event_downlink(
        &self,
        path: Path,
    ) -> Result<Arc<UntypedEventDownlink>, ClientError<Path>> {
        self.inner.untyped_event_downlink(path).await
    }

    /// Shut down the client and wait for all tasks to finish running.
    pub async fn stop(self) -> Result<(), ClientError<Path>> {
        let SwimClient {
            close_buffer_size,
            task_handle,
            stop_trigger,
            ..
        } = self;

        let (tx, mut rx) = mpsc::channel(close_buffer_size.get());

        if stop_trigger.provide(tx).is_err() {
            return Err(ClientError::Close);
        }

        if let Some(Err(routing_err)) = rx.recv().await {
            return Err(routing_err.into());
        }

        let result = task_handle.await?;

        match result {
            (Err(err), _, _) => Err(err.into()),
            (_, Err(err), _) => Err(err.into()),
            (_, _, Err(err)) => Err(err.into()),
            _ => Ok(()),
        }
    }
}

type ClientTaskHandle<Path> = TaskHandle<(
    Result<(), SubscriptionError<Path>>,
    Result<(), std::io::Error>,
    Result<(), swim_runtime::error::ConnectionError>,
)>;

#[derive(Clone, Debug)]
pub struct ClientContext<Path: Addressable> {
    downlinks: Downlinks<Path>,
}

impl<Path: Addressable> ClientContext<Path> {
    pub fn new(downlinks: Downlinks<Path>) -> Self {
        ClientContext { downlinks }
    }

    pub async fn send_command<T: Form>(
        &self,
        target: Path,
        message: T,
    ) -> Result<(), ClientError> {
        let envelope = Envelope::command()
            .node_uri(&target.node)
            .lane_uri(&target.lane)
            .body(message)
            .done();

        self.downlinks
            .send_command(target, envelope)
            .await
            .map_err(ClientError::Subscription)
    }

    pub async fn value_downlink<T>(
        &self,
        path: Path,
        initial: T,
    ) -> Result<(TypedValueDownlink<T>, ValueDownlinkReceiver<T>), ClientError<Path>>
    where
        T: Form + ValueSchema + Send + 'static,
    {
        self.downlinks
            .subscribe_value(initial, path)
            .await
            .map_err(ClientError::Subscription)
    }

    pub async fn map_downlink<K, V>(
        &self,
        path: Path,
    ) -> Result<(TypedMapDownlink<K, V>, MapDownlinkReceiver<K, V>), ClientError<Path>>
    where
        K: ValueSchema + Send + 'static,
        V: ValueSchema + Send + 'static,
    {
        self.downlinks
            .subscribe_map(path)
            .await
            .map_err(ClientError::Subscription)
    }

    pub async fn command_downlink<T>(
        &self,
        path: Path,
    ) -> Result<TypedCommandDownlink<T>, ClientError<Path>>
    where
        T: ValueSchema + Send + 'static,
    {
        self.downlinks
            .subscribe_command(path)
            .await
            .map_err(ClientError::Subscription)
    }

    pub async fn event_downlink<T>(
        &self,
        path: Path,
        violations: SchemaViolations,
    ) -> Result<TypedEventDownlink<T>, ClientError<Path>>
    where
        T: ValueSchema + Send + 'static,
    {
        self.downlinks
            .subscribe_event(path, violations)
            .await
            .map_err(ClientError::Subscription)
    }

    pub async fn untyped_value_downlink(
        &self,
        path: Path,
        initial: Value,
    ) -> Result<(Arc<UntypedValueDownlink>, UntypedValueReceiver), ClientError<Path>> {
        self.downlinks
            .subscribe_value_untyped(initial, path)
            .await
            .map_err(ClientError::Subscription)
    }

    pub async fn untyped_map_downlink(
        &self,
        path: Path,
    ) -> Result<(Arc<UntypedMapDownlink>, UntypedMapReceiver), ClientError<Path>> {
        self.downlinks
            .subscribe_map_untyped(path)
            .await
            .map_err(ClientError::Subscription)
    }

    pub async fn untyped_command_downlink(
        &self,
        path: Path,
    ) -> Result<Arc<UntypedCommandDownlink>, ClientError<Path>> {
        self.downlinks
            .subscribe_command_untyped(path)
            .await
            .map_err(ClientError::Subscription)
    }

    pub async fn untyped_event_downlink(
        &self,
        path: Path,
    ) -> Result<Arc<UntypedEventDownlink>, ClientError<Path>> {
        self.downlinks
            .subscribe_event_untyped(path)
            .await
            .map_err(ClientError::Subscription)
    }
}

/// Represents errors that can occur in the client.
#[derive(Debug)]
pub enum ClientError<Path: Addressable> {
    /// An error that occurred when the client was running.
    RuntimeError(io::Error),
    /// An error that occurred when subscribing to a downlink.
    Subscription(SubscriptionError<Path>),
    /// An error that occurred in the router.
    Routing(RoutingError),
    /// An error that occurred in a downlink.
    Downlink(DownlinkError),
    /// An error that occurred when closing the client.
    Close,
}

impl<Path: Addressable> From<TaskError> for ClientError<Path> {
    fn from(_err: TaskError) -> Self {
        ClientError::Close
    }
}

impl<Path: Addressable> From<SubscriptionError<Path>> for ClientError<Path> {
    fn from(err: SubscriptionError<Path>) -> Self {
        ClientError::Subscription(err)
    }
}

impl<Path: Addressable> From<ConnectionError> for ClientError<Path> {
    fn from(err: ConnectionError) -> Self {
        ClientError::Routing(RoutingError::PoolError(err))
    }
}

impl<Path: Addressable> From<io::Error> for ClientError<Path> {
    fn from(err: io::Error) -> Self {
        ClientError::RuntimeError(err)
    }
}

impl<Path: Addressable + 'static> Display for ClientError<Path> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.source() {
            Some(e) => write!(f, "Client error. Caused by: {}", e),
            None => write!(f, "Client error"),
        }
    }
}

impl<Path: Addressable + 'static> Error for ClientError<Path> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self {
            ClientError::RuntimeError(e) => Some(e),
            ClientError::Subscription(e) => Some(e),
            ClientError::Routing(e) => Some(e),
            ClientError::Downlink(e) => Some(e),
            ClientError::Close => None,
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.source()
    }
}

impl<Path: Addressable> From<RoutingError> for ClientError<Path> {
    fn from(err: RoutingError) -> Self {
        ClientError::Routing(err)
    }
}
