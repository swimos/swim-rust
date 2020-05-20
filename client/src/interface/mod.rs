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
    AnyMapDownlink, AnyValueDownlink, MapReceiver, SubscriptionError, TypedMapDownlink,
    TypedMapReceiver, TypedValueDownlink, TypedValueReceiver, ValueReceiver,
};
use crate::downlink::Operation::Error;
use crate::interface::context::{swim_context, SwimContext};
use crate::interface::error::ClientError;
use crate::interface::error::ErrorKind;
use crate::interface::stub::StubRouter;
use crate::router::Router;
use common::model::Value;

pub mod context;
pub mod error;
mod stub;

pub struct SwimClient {
    // router: Router,
// configuration: Box<dyn Config>,
}

impl SwimClient {
    #[allow(clippy::new_without_default)]
    pub async fn new<C>(configuration: C) -> Self
    where
        C: Config + 'static,
    {
        info!("Initialising Swim Client");

        let ctx = SwimContext::build(configuration, StubRouter::new()).await;
        SwimContext::enter(ctx);

        SwimClient {}
    }

    pub async fn send_command<T: Form>(
        _target: AbsolutePath,
        _value: T,
    ) -> Result<(), ClientError> {
        unimplemented!()
    }

    pub async fn value_downlink<T>(
        path: AbsolutePath,
        default: T,
    ) -> Result<(TypedValueDownlink<T>, TypedValueReceiver<T>), ClientError>
    where
        T: ValidatedForm + Send + 'static,
    {
        let mut ctx = Self::swim_context()?;
        ctx.value_downlink(default, path).await
    }

    pub async fn map_downlink<K, V>(
        path: AbsolutePath,
    ) -> Result<(TypedMapDownlink<K, V>, TypedMapReceiver<K, V>), ClientError>
    where
        K: ValidatedForm + Send + 'static,
        V: ValidatedForm + Send + 'static,
    {
        let ctx = Self::swim_context()?;
        ctx.map_downlink(path).await
    }

    pub async fn untyped_value_downlink(
        path: AbsolutePath,
        default: Value,
    ) -> Result<(AnyValueDownlink, ValueReceiver), ClientError> {
        let ctx = Self::swim_context()?;
        ctx.untyped_value_downlink(path, default).await
    }

    pub async fn untyped_map_downlink(
        path: AbsolutePath,
    ) -> Result<(AnyMapDownlink, MapReceiver), ClientError> {
        let ctx = Self::swim_context()?;
        ctx.untyped_map_downlink(path).await
    }

    pub fn swim_context() -> Result<SwimContext, ClientError> {
        match swim_context() {
            Some(ctx) => Ok(ctx),
            None => Err(ClientError::from(ErrorKind::RuntimeError, None)),
        }
    }

    pub async fn run_session<S, F>(&mut self, session: S) -> Result<F::Output, ClientError>
    where
        S: FnOnce(SwimContext) -> F,
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let mut ctx = Self::swim_context()?;

        trace!("Running new session");

        ctx.spawn(session(ctx.clone()))
            .await
            .map_err(|_| ClientError::from(ErrorKind::RuntimeError, None))
    }
}
