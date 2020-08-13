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

use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use tracing::info;

use swim_common::form::ValidatedForm;
use swim_common::model::Value;
use swim_common::warp::path::AbsolutePath;

use crate::configuration::downlink::{Config, ConfigHierarchy, ConfigParseError};
use crate::configuration::router::RouterParamBuilder;
use crate::connections::SwimConnPool;
use crate::downlink::subscription::{
    AnyCommandDownlink, AnyEventDownlink, AnyMapDownlink, AnyValueDownlink, Downlinks, MapReceiver,
    SubscriptionError, TypedCommandDownlink, TypedEventDownlink, TypedMapDownlink,
    TypedMapReceiver, TypedValueDownlink, TypedValueReceiver, ValueReceiver,
};
use crate::downlink::typed::SchemaViolations;
use crate::downlink::DownlinkError;
use crate::router::{RoutingError, SwimRouter};
use std::fs::File;
use std::io::Read;
use swim_common::connections::WebsocketFactory;
use swim_common::model::parser::parse_single;
use swim_common::warp::envelope::Envelope;

/// Represents errors that can occur in the client.
#[derive(Debug)]
pub enum ClientError {
    /// An error that occurred when subscribing to a downlink.
    SubscriptionError(SubscriptionError),
    /// An error that occurred in the router.
    RoutingError(RoutingError),
    /// An error that occurred in a downlink.
    DownlinkError(DownlinkError),
    /// An error that occurred when parsing the client configuration.
    ConfigError(ConfigParseError),
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
            ClientError::ConfigError(e) => Some(e),
            ClientError::CloseError => None,
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.source()
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
/// conform to a contract that is imposed by a form implementation and all actions are verified
/// against the provided schema to ensure that its views are consistent.
///
pub struct SwimClient {
    /// The downlink manager attached to this Swim Client.
    downlinks: Downlinks,
}

impl SwimClient {
    /// Creates a new SWIM Client using the default configuration.
    pub async fn new_with_default<Fac>(connection_factory: Fac) -> Self
    where
        Fac: WebsocketFactory + 'static,
    {
        SwimClient::new(ConfigHierarchy::default(), connection_factory).await
    }

    /// Creates a new SWIM Client using configuration from a Recon file.
    pub async fn new_from_file<Fac>(
        mut config_file: File,
        use_defaults: bool,
        connection_factory: Fac,
    ) -> Result<Self, ClientError>
    where
        Fac: WebsocketFactory + 'static,
    {
        let mut contents = String::new();
        config_file
            .read_to_string(&mut contents)
            .map_err(|err| ClientError::ConfigError(ConfigParseError::FileError(err)))?;

        let config = ConfigHierarchy::try_from_value(
            parse_single(&contents)
                .map_err(|err| ClientError::ConfigError(ConfigParseError::ReconError(err)))?,
            use_defaults,
        )
        .map_err(ClientError::ConfigError)?;

        Ok(SwimClient::new(config, connection_factory).await)
    }

    /// Creates a new Swim Client and associates the provided [`configuration`] with the downlinks.
    /// The provided configuration is used when opening new downlinks.
    pub async fn new<C, Fac>(configuration: C, connection_factory: Fac) -> Self
    where
        C: Config + 'static,
        Fac: WebsocketFactory + 'static,
    {
        info!("Initialising Swim Client");

        let config = RouterParamBuilder::default().build();
        let pool = SwimConnPool::new(config.connection_pool_params(), connection_factory);
        let router = SwimRouter::new(config, pool);

        SwimClient {
            downlinks: Downlinks::new(Arc::new(configuration), router).await,
        }
    }

    /// Shut down the client and wait for all tasks to finish running.
    pub async fn close(self) -> Result<(), ClientError> {
        let result = self.downlinks.close().await;

        match result {
            Ok(r) => r.map_err(ClientError::SubscriptionError),
            Err(_) => Err(ClientError::CloseError),
        }
    }

    /// Sends a command directly to the provided [`target`] lane.
    pub async fn send_command(
        &mut self,
        target: AbsolutePath,
        envelope: Envelope,
    ) -> Result<(), ClientError> {
        self.downlinks
            .send_command(target, envelope)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new typed value downlink at the provided path and initialises it with [`initial`].
    pub async fn value_downlink<T>(
        &mut self,
        path: AbsolutePath,
        initial: T,
    ) -> Result<(TypedValueDownlink<T>, TypedValueReceiver<T>), ClientError>
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
    ) -> Result<(TypedMapDownlink<K, V>, TypedMapReceiver<K, V>), ClientError>
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

    /// Opens a new untyped value downlink at the provided path and initialises it with [`initial`] value.
    pub async fn untyped_value_downlink(
        &mut self,
        path: AbsolutePath,
        initial: Value,
    ) -> Result<(AnyValueDownlink, ValueReceiver), ClientError> {
        self.downlinks
            .subscribe_value_untyped(initial, path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped value downlink at the provided path.
    pub async fn untyped_map_downlink(
        &mut self,
        path: AbsolutePath,
    ) -> Result<(AnyMapDownlink, MapReceiver), ClientError> {
        self.downlinks
            .subscribe_map_untyped(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped command downlink at the provided path.
    pub async fn untyped_command_downlink(
        &mut self,
        path: AbsolutePath,
    ) -> Result<AnyCommandDownlink, ClientError> {
        self.downlinks
            .subscribe_command_untyped(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }

    /// Opens a new untyped event downlink at the provided path.
    pub async fn untyped_event_downlink(
        &mut self,
        path: AbsolutePath,
    ) -> Result<AnyEventDownlink, ClientError> {
        self.downlinks
            .subscribe_event_untyped(path)
            .await
            .map_err(ClientError::SubscriptionError)
    }
}
