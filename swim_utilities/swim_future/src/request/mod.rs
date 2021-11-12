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

use std::fmt::Debug;
use tokio::sync::oneshot;

pub mod request_future;

/// An error produced when the message could not be sent.
#[derive(Debug)]
pub struct RequestErr;

/// An asynchronous request for an agent to provide a value.
#[derive(Debug)]
pub struct Request<T> {
    satisfy: oneshot::Sender<T>,
}

/// An asynchronous request for an agent to provide a value or fail.
pub type TryRequest<T, E> = Request<Result<T, E>>;

impl<T> Request<T> {
    pub fn new(sender: oneshot::Sender<T>) -> Request<T> {
        Request { satisfy: sender }
    }

    pub fn send(self, data: T) -> Result<(), RequestErr> {
        match self.satisfy.send(data) {
            Ok(_) => Ok(()),
            Err(_) => Err(RequestErr),
        }
    }
}

impl<T, E> Request<Result<T, E>> {
    pub fn send_ok(self, data: T) -> Result<(), RequestErr> {
        self.send(Ok(data))
    }

    pub fn send_err(self, err: E) -> Result<(), RequestErr> {
        self.send(Err(err))
    }
}