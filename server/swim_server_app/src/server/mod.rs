// Copyright 2015-2021 Swim Inc.
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

use futures::future::BoxFuture;
use swim_utilities::trigger;

mod builder;
mod runtime;

pub use builder::ServerBuilder;

pub struct ServerHandle {
    stop_trigger: Option<trigger::Sender>,
}

impl ServerHandle {
    fn new(tx: trigger::Sender) -> Self {
        ServerHandle {
            stop_trigger: Some(tx),
        }
    }
}

/// Allows the server to be stopped externally.
impl ServerHandle {
    /// After this is called, the associated task will begin to stop.
    pub fn stop(&mut self) {
        if let Some(tx) = self.stop_trigger.take() {
            tx.trigger();
        }
    }
}

/// Interface for Swim server implementations.
pub trait Server {
    /// Running the server produces a future and a handle. The future is the task that will
    /// run the main event loop of the server (listening on a socket, creating new agent
    /// instances, etc.). The handle is used to signal that the task should stop from
    /// outside the event loop. If the handle is dropped, this will also cause the server
    /// to stop.
    fn run(self) -> (BoxFuture<'static, Result<(), std::io::Error>>, ServerHandle);
}
