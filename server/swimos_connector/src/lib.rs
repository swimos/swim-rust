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

//! # SwimOS Connector API.
//!
//! This crate provides a framework for building connectors which ingest data into agents.
//! Connectors are modelled as specialized agent lifecycles which provide an agent implementation
//! that acts as an ingress point for a Swim application for some external data source.
//!
//! # Available Implementations
//! - [Fluvio](https://github.com/swimos/swim-rust/tree/main/server/swimos_connector_fluvio)
//! - [Kafka](https://github.com/swimos/swim-rust/tree/main/server/swimos_connector_kafka)

#[cfg(test)]
mod test_support;

mod connectors;
mod lifecycle;
mod route;

/// Deserialization formats for use with connectors. These are intended to be used lazily and only
/// when a selector reads from the connector's message.
pub mod deserialization;

/// Selectors for selecting subcomponents from a connector message.
pub mod selector;

pub use connectors::{Connector, ConnectorInitError, ConnectorStream};
pub use lifecycle::ConnectorLifecycle;
pub use route::ConnectorModel;

/// Connector relay implementation. See [`crate::connectors::relay::Relay`] for more details.
pub mod relay {
    pub use crate::connectors::relay::{
        AgentRelay, ConnectorHandlerContext, FnRelay, LaneSelector, NodeSelector, PayloadSelector,
        RecordSelectors, Relay, RelayConnectorAgent, RelayError, Selectors,
    };
}

pub mod generic {
    pub use crate::connectors::generic::{
        selector::{MapLaneSelector, ValueLaneSelector},
        BadSelector, DerserializerLoadError, DeserializationFormat, GenericConnectorAgent,
        InvalidLaneSpec, InvalidLanes, LaneSelectorError, Lanes, MapLaneSelectorFn, MapLaneSpec,
        MessageSelector, ValueLaneSelectorFn, ValueLaneSpec,
    };
}
