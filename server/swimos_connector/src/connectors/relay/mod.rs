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

mod handler;
mod model;
mod selector;
#[cfg(test)]
mod tests;

pub use crate::connectors::relay::selector::{
    LaneSelector, NodeSelector, PayloadSelector, RecordSelectors, Selectors,
};

pub use model::{AgentRelay, FnRelay, Relay};

use crate::deserialization::DeserializationError;
use bytes::BytesMut;
use std::collections::HashMap;
use std::error::Error;
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    agent_model::AgentSpec,
    agent_model::{
        ItemDescriptor, ItemFlags, ItemSpec, MapLikeInitializer, ValueLikeInitializer, WriteResult,
    },
    event_handler::{Discard, Stop, UnitHandler},
};
use swimos_agent_protocol::MapMessage;
use swimos_api::agent::{HttpLaneRequest, WarpLaneKind};
use uuid::Uuid;

pub type ConnectorHandlerContext = HandlerContext<RelayConnectorAgent>;

const STOP_LANE_URI: &str = "stop";

/// Errors which may be produced when the relay is running.
#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    /// Deserializing a component of a message failed.
    #[error(transparent)]
    Deserialization(#[from] DeserializationError),
    #[error("Selector failed to select from input: {0}")]
    Selector(String),
    /// A node or lane selector failed to select from a record as it was not a primitive type.
    #[error("Invalid record structure. Expected a primitive type")]
    InvalidRecord(String),
}

impl RelayError {
    pub fn deserialization<E>(e: E) -> RelayError
    where
        E: Error + Send + 'static,
    {
        RelayError::Deserialization(DeserializationError::new(e))
    }
}

/// A relay agent type to be used by implementations of [`crate::Connector`]. This agent does not
/// have any functionality other than the ability to terminate the connector when a command is sent
/// to the `stop` lane.
#[derive(Default, Copy, Clone)]
pub struct RelayConnectorAgent;

impl AgentSpec for RelayConnectorAgent {
    type ValCommandHandler = Discard<Stop>;
    type MapCommandHandler = UnitHandler;
    type OnSyncHandler = UnitHandler;
    type HttpRequestHandler = UnitHandler;

    fn item_specs() -> HashMap<&'static str, ItemSpec> {
        HashMap::from([(
            STOP_LANE_URI,
            ItemSpec::new(
                0,
                STOP_LANE_URI,
                ItemDescriptor::WarpLane {
                    kind: WarpLaneKind::Command,
                    flags: ItemFlags::TRANSIENT,
                },
            ),
        )])
    }

    fn on_value_command(&self, lane: &str, _body: BytesMut) -> Option<Self::ValCommandHandler> {
        if lane == STOP_LANE_URI {
            Some(Discard::new(Stop))
        } else {
            None
        }
    }

    fn init_value_like_item(&self, _item: &str) -> Option<ValueLikeInitializer<Self>>
    where
        Self: 'static,
    {
        None
    }

    fn init_map_like_item(&self, _item: &str) -> Option<MapLikeInitializer<Self>>
    where
        Self: 'static,
    {
        None
    }

    fn on_map_command(
        &self,
        _lane: &str,
        _body: MapMessage<BytesMut, BytesMut>,
    ) -> Option<Self::MapCommandHandler> {
        None
    }

    fn on_sync(&self, _lane: &str, _id: Uuid) -> Option<Self::OnSyncHandler> {
        None
    }

    fn on_http_request(
        &self,
        _lane: &str,
        _request: HttpLaneRequest,
    ) -> Result<Self::HttpRequestHandler, HttpLaneRequest> {
        Ok(UnitHandler::default())
    }

    fn write_event(&self, _lane: &str, _buffer: &mut BytesMut) -> Option<WriteResult> {
        None
    }
}
