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

use std::borrow::Borrow;
use std::cell::RefCell;
use std::sync::Arc;

use futures::Future;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use common::model::Value;
use common::warp::path::AbsolutePath;
use form::{Form, ValidatedForm};

use crate::configuration::downlink::Config;
use crate::downlink::subscription::{
    AnyMapDownlink, AnyValueDownlink, Downlinks, MapReceiver, TypedMapDownlink, TypedMapReceiver,
    TypedValueDownlink, TypedValueReceiver, ValueReceiver,
};
use crate::interface::error::{ClientError, ErrorKind};
use crate::router::Router;

thread_local! {
    static CONTEXT: RefCell<Option<SwimContext>> = RefCell::new(None)
}

pub(crate) fn swim_context() -> Option<SwimContext> {
    CONTEXT.with(|ctx| ctx.borrow().clone())
}

#[derive(Clone)]
pub struct SwimContext {
    downlinks: Arc<Mutex<Downlinks>>,
}

impl SwimContext {
    pub fn spawn<F>(&mut self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        tokio::spawn(future)
    }

    pub(crate) async fn build<R, C>(configuration: C, router: R) -> SwimContext
    where
        R: Router + 'static,
        C: Config + 'static,
    {
        SwimContext {
            downlinks: Arc::new(Mutex::new(
                Downlinks::new(Arc::new(configuration), router).await,
            )),
        }
    }

    pub(crate) fn enter(new: SwimContext) {
        CONTEXT.with(|ctx| {
            let _old = ctx.borrow_mut().replace(new);
        });
    }

    pub async fn send_command<T>(
        &self,
        _target: AbsolutePath,
        _value: T,
    ) -> Result<(), ClientError> {
        unimplemented!()
    }

    pub async fn value_downlink<T>(
        &mut self,
        default: T,
        path: AbsolutePath,
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
