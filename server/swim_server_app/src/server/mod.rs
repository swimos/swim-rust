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

use std::net::SocketAddr;

use futures::future::BoxFuture;
use swim_utilities::trigger;

mod builder;
mod runtime;
mod store;

pub use builder::ServerBuilder;

use tokio::sync::oneshot;

use crate::error::ServerError;

pub struct ServerHandle {
    stop_trigger: Option<trigger::Sender>,
    addr: Option<SocketAddr>,
    addr_rx: Option<oneshot::Receiver<SocketAddr>>,
}

/// Allows the server to be stopped externally.
impl ServerHandle {
    fn new(tx: trigger::Sender, addr_rx: oneshot::Receiver<SocketAddr>) -> Self {
        ServerHandle {
            stop_trigger: Some(tx),
            addr: None,
            addr_rx: Some(addr_rx),
        }
    }

    /// Wait until the server has bound to an address and return it. If the bind fails, this
    /// will return nothing. Primarily useful when binding to a random port.
    pub async fn bound_addr(&mut self) -> Option<SocketAddr> {
        let ServerHandle { addr, addr_rx, .. } = self;
        if let Some(addr) = addr {
            Some(*addr)
        } else if let Some(addr_rx) = addr_rx.take() {
            if let Ok(bound_to) = addr_rx.await {
                *addr = Some(bound_to);
                Some(bound_to)
            } else {
                None
            }
        } else {
            None
        }
    }

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
    fn run(self) -> (BoxFuture<'static, Result<(), ServerError>>, ServerHandle);

    /// Run the server from a box.
    fn run_box(self: Box<Self>) -> (BoxFuture<'static, Result<(), ServerError>>, ServerHandle);
}

/// A boxed server implementation.
pub struct BoxServer(pub Box<dyn Server>);

impl Server for BoxServer {
    fn run(self) -> (BoxFuture<'static, Result<(), ServerError>>, ServerHandle) {
        self.0.run_box()
    }

    fn run_box(self: Box<Self>) -> (BoxFuture<'static, Result<(), ServerError>>, ServerHandle) {
        self.0.run_box()
    }
}
