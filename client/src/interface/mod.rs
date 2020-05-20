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

use futures::Future;
use tokio::sync::Mutex;
use tokio::task::JoinError;
use tracing::{info, trace};

use common::model::Value;
use common::warp::path::AbsolutePath;
use form::{Form, ValidatedForm};

use crate::configuration::downlink::{
    BackpressureMode, ClientParams, Config, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};
use crate::downlink::model::value::Action;
use crate::downlink::subscription::{
    AnyMapDownlink, AnyValueDownlink, Downlinks, MapReceiver, SubscriptionError, TypedMapDownlink,
    TypedMapReceiver, TypedValueDownlink, TypedValueReceiver, ValueReceiver,
};
use crate::downlink::DownlinkError;
use crate::downlink::Operation::Error;
use crate::interface::stub::StubRouter;
use crate::router::Router;
use crate::router::RoutingError;
use common::sink::item::ItemSink;
use tokio::time::Duration;

mod stub;

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

        SwimClient {
            downlinks: Downlinks::new(Arc::new(configuration), stub::StubRouter::new()).await,
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

fn config() -> ConfigHierarchy {
    let client_params = ClientParams::new(2).unwrap();
    let default_params = DownlinkParams::new_queue(
        BackpressureMode::Propagate,
        5,
        Duration::from_secs(60000),
        5,
        OnInvalidMessage::Terminate,
    )
    .unwrap();

    ConfigHierarchy::new(client_params, default_params)
}

#[tokio::test]
async fn test() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let mut client = SwimClient::new(config()).await;
    let val_path = AbsolutePath::new("my_host", "my_agent", "value_lane");
    let (mut dl, _receiver) = client.value_downlink::<i32>(val_path, 0).await.unwrap();
    let r = dl.send_item(Action::set(1.into())).await;

    info!("{:?}", r);
}
