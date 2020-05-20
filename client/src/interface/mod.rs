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

#[derive(Debug, PartialEq)]
pub enum ClientError {
    SubscriptionError(SubscriptionError),
    RoutingError(RoutingError),
    DownlinkError(DownlinkError),
}

pub struct SwimClient {
    downlinks: Downlinks,
}

impl SwimClient {
    pub async fn new<C>(configuration: C) -> Self
    where
        C: Config + 'static,
    {
        info!("Initialising Swim Client");

        let config = RouterParamBuilder::default().build();
        let pool = SwimConnPool::new(
            config.connection_pool_params(),
            TungsteniteWsFactory::new(5).await,
        );
        let router = SwimRouter::new(config, pool);

        SwimClient {
            downlinks: Downlinks::new(Arc::new(configuration), router).await,
        }
    }

    pub async fn send_command<T: Form>(
        _target: AbsolutePath,
        _value: T,
    ) -> Result<(), ClientError> {
        unimplemented!()
    }

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
