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

use std::{collections::HashMap, error::Error};

use futures::TryFuture;
use swimos_model::Value;

use super::{BaseConnector, ConnectorHandler};

pub trait EgressConnector: BaseConnector {
    /// The type of the errors produced by the connector.
    type SendError: Error + Send + 'static;

    type Sender: EgressConnectorSender<Self::SendError> + 'static;

    fn make_sender(
        &self,
        agent_params: &HashMap<String, String>,
    ) -> Result<Self::Sender, Self::SendError>;
}

pub trait EgressConnectorSender<SendError>: Send + Clone {
    fn send(
        &self,
        name: &str,
        key: Option<&Value>,
        value: &Value,
    ) -> impl ConnectorFuture<SendError>;
}

pub trait ConnectorFuture<E>:
    TryFuture<Ok: ConnectorHandler, Error = E> + Send + Unpin + 'static
{
}

impl<S, E> ConnectorFuture<E> for S where
    S: TryFuture<Ok: ConnectorHandler, Error = E> + Send + Unpin + 'static
{
}
