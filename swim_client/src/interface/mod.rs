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
use crate::configuration::downlink::{Config, ConfigHierarchy};
use crate::connections::SwimConnPool;
use crate::downlink::error::{DownlinkError, SubscriptionError};
use crate::downlink::subscription::{DownlinksHandle, RequestResult};
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
use crate::router::ClientRouterFactory;
use crate::runtime::task::TaskHandle;
use futures::join;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::form::{Form, ValidatedForm};
use swim_common::model::Value;
use swim_common::routing::error::RoutingError;
use swim_common::routing::remote::net::dns::Resolver;
use swim_common::routing::remote::net::plain::TokioPlainTextNetworking;
use swim_common::routing::remote::{RemoteConnectionChannels, RemoteConnectionsTask};
use swim_common::routing::ws::tungstenite::TungsteniteWsConnections;
use swim_common::routing::CloseSender;
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::{AbsolutePath, Addressable, Path};
use swim_runtime::task::spawn;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::promise;

/// Builder to create Swim client instance.
///
/// The builder can be created with default or custom configuration.
/// The custom configuration can be read from a file.
pub struct SwimClientBuilder {
    config: ConfigHierarchy<AbsolutePath>,
}

impl SwimClientBuilder {
    /// Create a new client builder with custom configuration.
    ///
    /// # Arguments
    /// * `config` - The custom configuration for the client.
    pub fn new(config: ConfigHierarchy<AbsolutePath>) -> Self {
        SwimClientBuilder { config }
    }

    //Todo dm this needs to be changed after the new client configuration is finalised.
    // /// Create a new client builder with configuration from a file.
    // ///
    // /// # Arguments
    // /// * `config_file` - Configuration file for the client.
    // /// * `use_defaults` - Whether or not missing values should be replaced with default ones.
    // pub fn new_from_file(
    //     mut config_file: File,
    //     use_defaults: bool,
    // ) -> Result<Self, ConfigParseError> {
    //     let mut contents = String::new();
    //     config_file
    //         .read_to_string(&mut contents)
    //         .map_err(ConfigParseError::FileError)?;
    //
    //     let config = ConfigHierarchy::try_from_value(
    //         parse_single(&contents).map_err(ConfigParseError::ReconError)?,
    //         use_defaults,
    //     )?;
    //
    //     Ok(SwimClientBuilder { config })
    // }

    pub fn build_from_downlinks(downlinks: Downlinks<Path>) -> SwimClient<Path> {
        SwimClient { downlinks }
    }

    /// Build the Swim client.
    pub async fn build(self) -> (SwimClient<AbsolutePath>, ClientHandle<AbsolutePath>) {
        let SwimClientBuilder {
            config: downlinks_config,
        } = self;

        info!("Initialising Swim Client");

        let client_params = downlinks_config.client_params();

        let (remote_tx, remote_rx) =
            mpsc::channel(client_params.connections_params.router_buffer_size.get());
        let (client_conn_request_tx, client_conn_request_rx) =
            mpsc::channel(client_params.connections_params.router_buffer_size.get());
        let client_router_factory =
            ClientRouterFactory::new(client_conn_request_tx.clone(), remote_tx.clone());
        let (close_tx, close_rx) = promise::promise();

        let remote_connections_task = RemoteConnectionsTask::new_client_task(
            client_params.connections_params,
            TokioPlainTextNetworking::new(Arc::new(Resolver::new().await)),
            TungsteniteWsConnections {
                config: client_params.websocket_params,
            },
            client_router_factory.clone(),
            OpenEndedFutures::new(),
            RemoteConnectionChannels::new(remote_tx, remote_rx, close_rx.clone()),
        )
        .await;

        // The connection pool handles the connections behnid the downlinks
        let (connection_pool, pool_task) = SwimConnPool::new(
            client_params.conn_pool_params,
            client_router_factory,
            client_conn_request_tx,
        );

        // The downlinks are state machines and request connections from the pool
        let (downlinks, downlinks_task) =
            Downlinks::new(connection_pool, Arc::new(downlinks_config), close_rx);

        // let remote_conn_manager = ClientConnectionsManager::new(
        //     client_conn_request_rx,
        //     remote_router_tx,
        //     None,
        //     client_params.dl_req_buffer_size,
        //     close_rx.clone(),
        // );

        let task_handle = spawn(async {
            join!(
                downlinks_task.run(),
                remote_connections_task.run(),
                pool_task.run(),
            )
            .0
        });

        (
            SwimClient { downlinks },
            ClientHandle {
                close_buffer_size: client_params.connections_params.router_buffer_size,
                task_handle,
                stop_trigger: close_tx,
            },
        )
    }

    // Todo dm
    //     /// Build the Swim client with default WS factory and configuration.
    //     pub async fn build_with_default() -> (SwimClient<AbsolutePath>, ClientHandle<AbsolutePath>) {
    //         info!("Initialising Swim Client");
    //
    //         let config: ConfigHierarchy<AbsolutePath> = ConfigHierarchy::default();
    //
    //         let client_params = config.client_params();
    //
    //         let (remote_router_tx, remote_router_rx) =
    //             mpsc::channel(client_params.connections_params.router_buffer_size.get());
    //         let (client_router_tx, client_router_rx) =
    //             mpsc::channel(client_params.connections_params.router_buffer_size.get());
    //         let delegate_router_factory = ClientRouterFactory::new(client_router_tx.clone());
    //         let (close_tx, close_rx) = promise::promise();
    //
    //         let remote_connections_task = RemoteConnectionsTask::new_client_task(
    //             client_params.connections_params,
    //             TokioPlainTextNetworking::new(Arc::new(Resolver::new().await)),
    //             TungsteniteWsConnections {
    //                 config: client_params.websocket_params,
    //             },
    //             delegate_router_factory,
    //             OpenEndedFutures::new(),
    //             RemoteConnectionChannels::new(
    //                 remote_router_tx.clone(),
    //                 remote_router_rx,
    //                 close_rx.clone(),
    //             ),
    //         )
    //         .await;
    //
    //         let remote_conn_manager = ClientConnectionsManager::new(
    //             client_router_rx,
    //             remote_router_tx,
    //             None,
    //             client_params.dl_req_buffer_size,
    //             close_rx.clone(),
    //         );
    //
    //         let (downlinks, downlinks_handle) =
    //             Downlinks::new(client_router_tx, Arc::new(config), close_rx);
    //
    //         let DownlinksHandle {
    //             downlinks_task,
    //             request_receiver,
    //         } = downlinks_handle;
    //
    //         let task_handle = spawn(async {
    //             join!(
    //                 downlinks_task.run(ReceiverStream::new(request_receiver)),
    //                 remote_connections_task.run(),
    //                 remote_conn_manager.run(),
    //                 task_manager.run(),
    //                 pool_task.run()
    //             )
    //             .0
    //         });
    //
    //         (
    //             SwimClient { downlinks },
    //             ClientHandle {
    //                 close_buffer_size: client_params.connections_params.router_buffer_size,
    //                 task_handle,
    //                 stop_trigger: close_tx,
    //             },
    //         )
    //     }
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
#[derive(Clone, Debug)]
pub struct SwimClient<Path: Addressable> {
    /// The downlinks manager attached to this Swim Client.
    downlinks: Downlinks<Path>,
}

pub struct ClientHandle<Path: Addressable> {
    close_buffer_size: NonZeroUsize,
    task_handle: TaskHandle<RequestResult<(), Path>>,
    stop_trigger: CloseSender,
}

impl<Path: Addressable> ClientHandle<Path> {
    /// Shut down the client and wait for all tasks to finish running.
    pub async fn close(self) -> Result<(), ClientError<Path>> {
        let ClientHandle {
            close_buffer_size,
            task_handle,
            stop_trigger,
        } = self;

        let (tx, mut rx) = mpsc::channel(close_buffer_size.get());

        if stop_trigger.provide(tx).is_err() {
            return Err(ClientError::CloseError);
        }

        match rx.recv().await {
            Some(close_result) => {
                if let Err(routing_err) = close_result {
                    return Err(ClientError::RoutingError(routing_err));
                }
            }
            None => return Err(ClientError::CloseError),
        }

        let result = task_handle.await;

        match result {
            Ok(r) => r.map_err(ClientError::SubscriptionError),
            Err(_) => Err(ClientError::CloseError),
        }
    }
}

impl<Path: Addressable> SwimClient<Path> {
    /// Sends a command directly to the provided `target` lane.
    pub async fn send_command<T: Form>(
        &self,
        target: Path,
        message: T,
    ) -> Result<(), ClientError<Path>> {
        let envelope =
            Envelope::make_command(target.node(), target.lane(), Some(message.into_value()));

        self.downlinks
            .send_command(target, envelope)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new typed value downlink at the provided path and initialises it with `initial`.
    pub async fn value_downlink<T>(
        &self,
        path: Path,
        initial: T,
    ) -> Result<(TypedValueDownlink<T>, ValueDownlinkReceiver<T>), ClientError<Path>>
    where
        T: ValidatedForm + Send + 'static,
    {
        self.downlinks
            .subscribe_value(initial, path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new typed map downlink at the provided path.
    pub async fn map_downlink<K, V>(
        &self,
        path: Path,
    ) -> Result<(TypedMapDownlink<K, V>, MapDownlinkReceiver<K, V>), ClientError<Path>>
    where
        K: ValidatedForm + Send + 'static,
        V: ValidatedForm + Send + 'static,
    {
        self.downlinks
            .subscribe_map(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new command downlink at the provided path.
    pub async fn command_downlink<T>(
        &self,
        path: Path,
    ) -> Result<TypedCommandDownlink<T>, ClientError<Path>>
    where
        T: ValidatedForm + Send + 'static,
    {
        self.downlinks
            .subscribe_command(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new event downlink at the provided path.
    pub async fn event_downlink<T>(
        &self,
        path: Path,
        violations: SchemaViolations,
    ) -> Result<TypedEventDownlink<T>, ClientError<Path>>
    where
        T: ValidatedForm + Send + 'static,
    {
        self.downlinks
            .subscribe_event(path, violations)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped value downlink at the provided path and initialises it with `initial`
    /// value.
    pub async fn untyped_value_downlink(
        &self,
        path: Path,
        initial: Value,
    ) -> Result<(Arc<UntypedValueDownlink>, UntypedValueReceiver), ClientError<Path>> {
        self.downlinks
            .subscribe_value_untyped(initial, path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped value downlink at the provided path.
    pub async fn untyped_map_downlink(
        &self,
        path: Path,
    ) -> Result<(Arc<UntypedMapDownlink>, UntypedMapReceiver), ClientError<Path>> {
        self.downlinks
            .subscribe_map_untyped(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped command downlink at the provided path.
    pub async fn untyped_command_downlink(
        &self,
        path: Path,
    ) -> Result<Arc<UntypedCommandDownlink>, ClientError<Path>> {
        self.downlinks
            .subscribe_command_untyped(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped event downlink at the provided path.
    pub async fn untyped_event_downlink(
        &self,
        path: Path,
    ) -> Result<Arc<UntypedEventDownlink>, ClientError<Path>> {
        self.downlinks
            .subscribe_event_untyped(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }
}

/// Represents errors that can occur in the client.
#[derive(Debug)]
pub enum ClientError<Path: Addressable> {
    /// An error that occurred when subscribing to a downlink.
    SubscriptionError(SubscriptionError<Path>),
    /// An error that occurred in the router.
    RoutingError(RoutingError),
    /// An error that occurred in a downlink.
    DownlinkError(DownlinkError),
    /// An error that occurred when closing the client.
    CloseError,
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
            ClientError::SubscriptionError(e) => Some(e),
            ClientError::RoutingError(e) => Some(e),
            ClientError::DownlinkError(e) => Some(e),
            ClientError::CloseError => None,
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.source()
    }
}
