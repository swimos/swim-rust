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

use common::warp::path::AbsolutePath;
use form::{Form, ValidatedForm};

use crate::configuration::downlink::Config;
use crate::downlink::subscription::{
    AnyMapDownlink, AnyValueDownlink, TypedMapDownlink, TypedValueDownlink,
};
pub use crate::interface::context::{swim_context, SwimContext};
use futures::Future;
use tokio::task::JoinError;

pub mod context;

pub struct SwimClient {
    // router: Router,
    // configuration: Box<dyn Config>,
    context: SwimContext,
}

#[derive(Debug)]
pub enum ClientError {
    Shutdown,
    RuntimeError,
}

impl SwimClient {
    pub fn new() -> Self {
        SwimClient {
            // router,
            // configuration: config.into(),
            context: SwimContext::build(),
        }
    }

    pub async fn send_command<T: Form>(
        _target: AbsolutePath,
        _value: T,
    ) -> Result<(), ClientError> {
        unimplemented!()
    }

    pub async fn value_downlink<T: ValidatedForm>(
        _path: AbsolutePath,
    ) -> Result<TypedValueDownlink<T>, ClientError> {
        unimplemented!()
    }

    pub async fn map_downlink<K: ValidatedForm, V: ValidatedForm>(
        _path: AbsolutePath,
    ) -> Result<TypedMapDownlink<K, V>, ClientError> {
        unimplemented!()
    }

    pub async fn untyped_value_downlink(
        _path: AbsolutePath,
    ) -> Result<AnyValueDownlink, ClientError> {
        unimplemented!()
    }

    pub async fn untyped_map_downlink(_path: AbsolutePath) -> Result<AnyMapDownlink, ClientError> {
        unimplemented!()
    }

    pub async fn run_session<S, F>(&mut self, session: S) -> Result<F::Output, ClientError>
    where
        S: FnOnce(SwimContext) -> F,
        F: Future + Send + 'static,
        F::Output: Send,
    {
        match swim_context() {
            Some(ctx) => self
                .context
                .spawn(session(ctx))
                .await
                .map_err(|_| ClientError::RuntimeError),
            None => Err(ClientError::Shutdown),
        }
    }
}

#[tokio::test]
async fn test_client() {
    let mut client = SwimClient::new();

    println!("Start");

    let _ = client
        .run_session(|mut ctx| async move {
            println!("Running session");

            ctx.spawn(async {
                println!("Hello");
            });
        })
        .await;

    println!("Finish");
}
