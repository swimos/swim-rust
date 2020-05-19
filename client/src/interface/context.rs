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

use crate::downlink::subscription::{
    AnyMapDownlink, AnyValueDownlink, Downlinks, TypedMapDownlink, TypedValueDownlink,
};
use futures::Future;
use tokio::task::JoinHandle;

use common::warp::path::AbsolutePath;
use form::{Form, ValidatedForm};

use crate::configuration::downlink::Config;
use crate::interface::ClientError;
use crate::router::Router;
use common::model::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

// TODO: Client context writing doing properly
thread_local! {
    static CONTEXT: RefCell<Option<SwimContext>> = RefCell::new(None)
}

pub fn swim_context() -> Option<SwimContext> {
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

    pub async fn send_command<T: Form>(
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
    ) -> Result<TypedValueDownlink<T>, ClientError>
    where
        T: ValidatedForm + Send + 'static,
    {
        self.downlinks
            .lock()
            .await
            .subscribe_value(default, path)
            .await
            .map_err(|_| ClientError::RuntimeError)
            // todo
            .map(|r| r.0)
    }

    pub async fn map_downlink<K: ValidatedForm, V: ValidatedForm>(
        &self,
        _path: AbsolutePath,
    ) -> Result<TypedMapDownlink<K, V>, ClientError> {
        unimplemented!()
    }

    pub async fn untyped_value_downlink(
        &self,
        _path: AbsolutePath,
    ) -> Result<AnyValueDownlink, ClientError> {
        unimplemented!()
    }

    pub async fn untyped_map_downlink(
        &self,
        _path: AbsolutePath,
    ) -> Result<AnyMapDownlink, ClientError> {
        unimplemented!()
    }
}
