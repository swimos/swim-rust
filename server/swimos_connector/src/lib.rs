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

mod connector;
mod error;
mod generic;
mod lifecycle;
mod relay;
mod route;
#[cfg(test)]
mod test_support;

pub mod config;
pub mod deser;
pub mod ingress;
pub mod selector;
pub mod ser;

pub use connector::{
    BaseConnector, ConnectorFuture, ConnectorHandler, ConnectorStream, EgressConnector,
    EgressConnectorSender, EgressContext, IngressConnector, IngressContext, MessageSource,
    SendResult,
};
pub use error::{
    BadSelector, ConnectorInitError, DeserializationError, InvalidLaneSpec, InvalidLanes,
    LoadError, SelectorError, SerializationError,
};
pub use generic::{ConnectorAgent, MapLaneSelectorFn, ValueLaneSelectorFn};
pub use lifecycle::{EgressConnectorLifecycle, IngressConnectorLifecycle};
pub use relay::{
    LaneSelector, MapRelaySpecification, NodeSelector, ParseError, PayloadSelector, Relay,
    RelaySpecification, Relays, ValueRelaySpecification,
};
pub use route::{EgressConnectorModel, IngressConnectorModel};
