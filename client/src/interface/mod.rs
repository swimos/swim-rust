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

use common::model::Value;
use common::warp::path::AbsolutePath;
use form::{Form, ValidatedForm};

use crate::configuration::downlink::Config;
use crate::configuration::router::RouterParamBuilder;
use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::connections::SwimConnPool;
use crate::downlink::subscription::{
    AnyMapDownlink, AnyValueDownlink, Downlinks, MapReceiver, SubscriptionError, TypedMapDownlink,
    TypedMapReceiver, TypedValueDownlink, TypedValueReceiver, ValueReceiver,
};
use crate::downlink::DownlinkError;
use crate::router::{RoutingError, SwimRouter};

#[cfg(test)]
mod tests;

/// Respresents errors that can occur in the client.
#[derive(Debug, PartialEq)]
pub enum ClientError {
    /// An error that occured when subscribing to a downlink.
    SubscriptionError(SubscriptionError),
    /// An error that occured in the router.
    RoutingError(RoutingError),
    /// An error that occured in a downlink.
    DownlinkError(DownlinkError),
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Client error. Caused by: {}", self.source().unwrap())
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self {
            ClientError::SubscriptionError(e) => Some(e),
            ClientError::RoutingError(e) => Some(e),
            ClientError::DownlinkError(e) => Some(e),
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
    /// Creates a new Swim Client and associates the provided [`configuration`] with the downlinks.
    /// The provided configuration is used when opening new downlinks.
    pub async fn new<C>(configuration: C) -> Self
    where
        C: Config + 'static,
    {
        info!("Initialising Swim Client");

        let config = RouterParamBuilder::default().build();
        let pool = SwimConnPool::new(
            config.connection_pool_params(),
            TungsteniteWsFactory::new(config.buffer_size().get()).await,
        );
        let router = SwimRouter::new(config, pool);

        SwimClient {
            downlinks: Downlinks::new(Arc::new(configuration), router).await,
        }
    }

    /// Sends a command directly to the provided [`target`] downlink.
    pub async fn send_command<T: Form>(
        _target: AbsolutePath,
        _value: T,
    ) -> Result<(), ClientError> {
        unimplemented!()
    }

    /// Opens a new typed value downlink at the provided path and initialises it with [`default`].
    pub async fn value_downlink<T>(
        &mut self,
        path: AbsolutePath,
        default: T,
    ) -> Result<(TypedValueDownlink<T>, TypedValueReceiver<T>), ClientError>
    where
        T: ValidatedForm + Send + 'static,
    {
        self.downlinks
            .subscribe_value(default, path)
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

    /// Opens a new untyped value downlink at the provided path and initialises it with [`default`] value.
    pub async fn untyped_value_downlink(
        &mut self,
        path: AbsolutePath,
        default: Value,
    ) -> Result<(AnyValueDownlink, ValueReceiver), ClientError> {
        self.downlinks
            .subscribe_value_untyped(default, path)
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
}
