// Copyright 2015-2024 Swim Inc.
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

mod egress;
mod ingress;

pub use egress::{
    EgressConnector, EgressConnectorSender, EgressContext, MessageSource, SendResult,
};
use futures::TryFuture;
pub use ingress::{suspend_connector, ConnectorStream, IngressConnector};
use swimos_agent::event_handler::EventHandler;
use swimos_utilities::trigger;

use crate::ConnectorAgent;

pub trait BaseConnector {
    /// Initialize the connector. All required lanes should be created by this handler.
    ///
    /// # Arguments
    /// * `init_complete` - The provided handler must trigger this when the initialization is complete. The connector stream will only
    ///   be started after this happens. If this is dropped, the connector will fail with an error.
    fn on_start(&self, init_complete: trigger::Sender) -> impl EventHandler<ConnectorAgent> + '_;

    /// This event handler will be executed before the connector stops (unless if fails with an error). This should be
    /// used to perform any required clean-up.
    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_;
}

/// A specialization of [`EventHandler`] for the [connector agent](ConnectorAgent).
pub trait ConnectorHandler: EventHandler<ConnectorAgent> + Send + 'static {}

impl<H> ConnectorHandler for H where H: EventHandler<ConnectorAgent> + Send + 'static {}

/// A future that results in a [`ConnectorHandler`] or an error.
pub trait ConnectorFuture<E>:
    TryFuture<Ok: ConnectorHandler, Error = E> + Send + Unpin + 'static
{
}

impl<S, E> ConnectorFuture<E> for S where
    S: TryFuture<Ok: ConnectorHandler, Error = E> + Send + Unpin + 'static
{
}
