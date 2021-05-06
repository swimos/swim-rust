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
use crate::router::{ClientRouterFactory, RemoteConnectionsManager};
use crate::runtime::task::TaskHandle;
use futures::join;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use swim_common::form::{Form, ValidatedForm};
use swim_common::model::Value;
use swim_common::routing::error::RoutingError;
use swim_common::routing::remote::net::dns::Resolver;
use swim_common::routing::remote::net::plain::TokioPlainTextNetworking;
use swim_common::routing::remote::{RemoteConnectionChannels, RemoteConnectionsTask};
use swim_common::routing::ws::tungstenite::TungsteniteWsConnections;
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::AbsolutePath;
use swim_runtime::task::spawn;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::trigger;

/// Builder to create Swim client instance.
///
/// The builder can be created with default or custom configuration.
/// The custom configuration can be read from a file.
pub struct SwimClientBuilder {
    config: ConfigHierarchy,
}

impl SwimClientBuilder {
    /// Create a new client builder with custom configuration.
    ///
    /// # Arguments
    /// * `config` - The custom configuration for the client.
    pub fn new(config: ConfigHierarchy) -> Self {
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

    /// Build the Swim client.
    pub async fn build(self) -> (SwimClient, ClientHandle) {
        let SwimClientBuilder {
            config: downlinks_config,
        } = self;

        info!("Initialising Swim Client");

        let client_params = downlinks_config.client_params();

        let (remote_router_tx, remote_router_rx) =
            mpsc::channel(client_params.connections_params.router_buffer_size.get());
        let (client_conn_request_tx, client_conn_request_rx) =
            mpsc::channel(client_params.connections_params.router_buffer_size.get());
        let top_level_router_fac = ClientRouterFactory::new(client_conn_request_tx.clone());
        let (stop_trigger_tx, stop_trigger_rx) = trigger::trigger();

        let remote_connections_task = RemoteConnectionsTask::new_client_task(
            client_params.connections_params,
            TokioPlainTextNetworking::new(Arc::new(Resolver::new().await)),
            TungsteniteWsConnections {
                config: client_params.websocket_params,
            },
            top_level_router_fac,
            OpenEndedFutures::new(),
            RemoteConnectionChannels::new(
                remote_router_tx.clone(),
                remote_router_rx,
                stop_trigger_rx,
            ),
        )
        .await;

        let remote_conn_manager = RemoteConnectionsManager::new(
            client_conn_request_rx,
            remote_router_tx,
            client_params.dl_req_buffer_size,
        );

        let (downlinks, downlinks_handle) =
            Downlinks::new(client_conn_request_tx, Arc::new(downlinks_config));

        let DownlinksHandle {
            downlinks_task,
            request_receiver,
            task_manager,
        } = downlinks_handle;

        let task_handle = spawn(async {
            join!(
                downlinks_task.run(ReceiverStream::new(request_receiver)),
                remote_connections_task.run(),
                remote_conn_manager.run(),
                task_manager.run()
            )
            .0
        });

        (
            SwimClient { downlinks },
            ClientHandle {
                task_handle,
                stop_trigger: stop_trigger_tx,
            },
        )
    }

    /// Build the Swim client with default WS factory and configuration.
    #[cfg(feature = "websocket")]
    pub async fn build_with_default() -> (SwimClient, ClientHandle) {
        info!("Initialising Swim Client");

        let config = ConfigHierarchy::default();

        let client_params = config.client_params();

        let (remote_router_tx, remote_router_rx) =
            mpsc::channel(client_params.connections_params.router_buffer_size.get());
        let (client_conn_request_tx, client_conn_request_rx) =
            mpsc::channel(client_params.connections_params.router_buffer_size.get());
        let top_level_router_fac = ClientRouterFactory::new(client_conn_request_tx.clone());
        let (stop_trigger_tx, stop_trigger_rx) = trigger::trigger();

        let remote_connections_task = RemoteConnectionsTask::new_client_task(
            client_params.connections_params,
            TokioPlainTextNetworking::new(Arc::new(Resolver::new().await)),
            TungsteniteWsConnections {
                config: client_params.websocket_params,
            },
            top_level_router_fac,
            OpenEndedFutures::new(),
            RemoteConnectionChannels::new(
                remote_router_tx.clone(),
                remote_router_rx,
                stop_trigger_rx,
            ),
        )
        .await;

        let remote_conn_manager = RemoteConnectionsManager::new(
            client_conn_request_rx,
            remote_router_tx,
            client_params.dl_req_buffer_size,
        );

        let (downlinks, downlinks_handle) =
            Downlinks::new(client_conn_request_tx, Arc::new(config));

        let DownlinksHandle {
            downlinks_task,
            request_receiver,
            task_manager,
        } = downlinks_handle;

        let task_handle = spawn(async {
            join!(
                downlinks_task.run(ReceiverStream::new(request_receiver)),
                remote_connections_task.run(),
                remote_conn_manager.run(),
                task_manager.run()
            )
            .0
        });

        (
            SwimClient { downlinks },
            ClientHandle {
                task_handle,
                stop_trigger: stop_trigger_tx,
            },
        )
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
#[derive(Clone, Debug)]
pub struct SwimClient {
    /// The downlinks manager attached to this Swim Client.
    pub downlinks: Downlinks,
}

pub struct ClientHandle {
    task_handle: TaskHandle<RequestResult<()>>,
    stop_trigger: trigger::Sender,
}

impl ClientHandle {
    /// Shut down the client and wait for all tasks to finish running.
    pub async fn close(self) -> Result<(), ClientError> {
        let ClientHandle {
            task_handle,
            stop_trigger,
        } = self;

        if !stop_trigger.trigger() {
            return Err(ClientError::CloseError);
        }

        let result = task_handle.await;

        match result {
            Ok(r) => r.map_err(ClientError::SubscriptionError),
            Err(_) => Err(ClientError::CloseError),
        }
    }
}

impl SwimClient {
    /// Sends a command directly to the provided `target` lane.
    pub async fn send_command<T: Form>(
        &mut self,
        target: AbsolutePath,
        message: T,
    ) -> Result<(), ClientError> {
        let envelope = Envelope::make_command(
            target.node.clone(),
            target.lane.clone(),
            Some(message.into_value()),
        );

        self.downlinks
            .send_command(target, envelope)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new typed value downlink at the provided path and initialises it with `initial`.
    pub async fn value_downlink<T>(
        &mut self,
        path: AbsolutePath,
        initial: T,
    ) -> Result<(TypedValueDownlink<T>, ValueDownlinkReceiver<T>), ClientError>
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
        &mut self,
        path: AbsolutePath,
    ) -> Result<(TypedMapDownlink<K, V>, MapDownlinkReceiver<K, V>), ClientError>
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
        &mut self,
        path: AbsolutePath,
    ) -> Result<TypedCommandDownlink<T>, ClientError>
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
        &mut self,
        path: AbsolutePath,
        violations: SchemaViolations,
    ) -> Result<TypedEventDownlink<T>, ClientError>
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
        &mut self,
        path: AbsolutePath,
        initial: Value,
    ) -> Result<(Arc<UntypedValueDownlink>, UntypedValueReceiver), ClientError> {
        self.downlinks
            .subscribe_value_untyped(initial, path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped value downlink at the provided path.
    pub async fn untyped_map_downlink(
        &mut self,
        path: AbsolutePath,
    ) -> Result<(Arc<UntypedMapDownlink>, UntypedMapReceiver), ClientError> {
        self.downlinks
            .subscribe_map_untyped(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped command downlink at the provided path.
    pub async fn untyped_command_downlink(
        &mut self,
        path: AbsolutePath,
    ) -> Result<Arc<UntypedCommandDownlink>, ClientError> {
        self.downlinks
            .subscribe_command_untyped(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped event downlink at the provided path.
    pub async fn untyped_event_downlink(
        &mut self,
        path: AbsolutePath,
    ) -> Result<Arc<UntypedEventDownlink>, ClientError> {
        self.downlinks
            .subscribe_event_untyped(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }
}

/// Represents errors that can occur in the client.
#[derive(Debug)]
pub enum ClientError {
    /// An error that occurred when subscribing to a downlink.
    SubscriptionError(SubscriptionError),
    /// An error that occurred in the router.
    RoutingError(RoutingError),
    /// An error that occurred in a downlink.
    DownlinkError(DownlinkError),
    /// An error that occurred when closing the client.
    CloseError,
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.source() {
            Some(e) => write!(f, "Client error. Caused by: {}", e),
            None => write!(f, "Client error"),
        }
    }
}

impl Error for ClientError {
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
