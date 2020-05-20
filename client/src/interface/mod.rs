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

use futures::Future;
use tokio::task::JoinError;
use tracing::{info, trace};

use common::warp::path::AbsolutePath;
use form::{Form, ValidatedForm};

use crate::configuration::downlink::Config;
use crate::downlink::subscription::{
    AnyMapDownlink, AnyValueDownlink, Downlinks, MapReceiver, SubscriptionError, TypedMapDownlink,
    TypedMapReceiver, TypedValueDownlink, TypedValueReceiver, ValueReceiver,
};
use crate::downlink::Operation::Error;
use crate::interface::error::ClientError;
use crate::interface::error::ErrorKind;
use crate::interface::stub::StubRouter;
use crate::router::Router;
use common::model::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod error;
mod stub;

pub struct SwimClient {
    downlinks: Arc<Mutex<Downlinks>>,
}

impl SwimClient {
    #[allow(clippy::new_without_default)]
    pub async fn new<C>(configuration: C) -> Self
    where
        C: Config + 'static,
    {
        info!("Initialising Swim Client");

        SwimClient {
            downlinks: Arc::new(Mutex::new(
                Downlinks::new(Arc::new(configuration), stub::StubRouter::new()).await,
            )),
        }
    }

    pub async fn send_command<T: Form>(
        _target: AbsolutePath,
        _value: T,
    ) -> Result<(), ClientError> {
        unimplemented!()
    }

    pub async fn value_downlink<T>(
        &self,
        path: AbsolutePath,
        default: T,
    ) -> Result<(TypedValueDownlink<T>, TypedValueReceiver<T>), ClientError>
    where
        T: ValidatedForm + Send + 'static,
    {
        self.downlinks
            .lock()
            .await
            .subscribe_value(default, path)
            .await
            .map_err(|e| ClientError::with_cause(ErrorKind::SubscriptionError, e))
    }

    pub async fn map_downlink<K, V>(
        &self,

        path: AbsolutePath,
    ) -> Result<(TypedMapDownlink<K, V>, TypedMapReceiver<K, V>), ClientError>
    where
        K: ValidatedForm + Send + 'static,
        V: ValidatedForm + Send + 'static,
    {
        self.downlinks
            .lock()
            .await
            .subscribe_map(path)
            .await
            .map_err(|e| ClientError::with_cause(ErrorKind::SubscriptionError, e))
    }

    pub async fn untyped_value_downlink(
        &self,

        path: AbsolutePath,
        default: Value,
    ) -> Result<(AnyValueDownlink, ValueReceiver), ClientError> {
        self.downlinks
            .lock()
            .await
            .subscribe_value_untyped(default, path)
            .await
            .map_err(|e| ClientError::with_cause(ErrorKind::SubscriptionError, e))
    }

    pub async fn untyped_map_downlink(
        &self,

        path: AbsolutePath,
    ) -> Result<(AnyMapDownlink, MapReceiver), ClientError> {
        self.downlinks
            .lock()
            .await
            .subscribe_map_untyped(path)
            .await
            .map_err(|e| ClientError::with_cause(ErrorKind::SubscriptionError, e))
    }
}
