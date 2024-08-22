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

mod json;
mod selector;

pub mod formats {
    pub mod json {
        pub use crate::simple::json::{
            JsonRelay, LaneSelectorPattern, NodeSelectorPattern, PayloadSelectorPattern,
        };
    }
}

use crate::{Connector, ConnectorLifecycle};
use bytes::BytesMut;
use futures::future::BoxFuture;
use std::collections::HashMap;
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    agent_model::AgentSpec,
    agent_model::{
        AgentModel, ItemDescriptor, ItemFlags, ItemSpec, MapLikeInitializer, ValueLikeInitializer,
        WriteResult,
    },
    event_handler::HandlerActionExt,
    event_handler::{Discard, EventHandler, Stop, UnitHandler},
};
use swimos_agent_protocol::MapMessage;
use swimos_api::agent::{
    Agent, AgentConfig, AgentContext, AgentInitResult, HttpLaneRequest, WarpLaneKind,
};
use swimos_utilities::routing::RouteUri;
use uuid::Uuid;

pub trait SimpleConnectorHandler: EventHandler<SimpleConnectorAgent> + Send + 'static {}

impl<H> SimpleConnectorHandler for H where H: EventHandler<SimpleConnectorAgent> + Send + 'static {}

pub type ConnectorHandlerContext = HandlerContext<SimpleConnectorAgent>;

const STOP_LANE_URI: &str = "stop";

use crate::simple::json::JsonRelay;
use crate::DeserializationError;

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error(transparent)]
    Deserialization(#[from] DeserializationError),
    #[error("Missing field from record: {0}")]
    MissingField(String),
    #[error("Invalid record structure. Expected a primitive type")]
    InvalidRecord(String),
}

#[derive(Debug, Clone)]
pub enum Relay {
    Json(JsonRelay),
}

impl Relay {
    pub fn on_record(
        &self,
        key: &[u8],
        value: &[u8],
        context: ConnectorHandlerContext,
    ) -> Result<impl SimpleConnectorHandler, RelayError> {
        match self {
            Relay::Json(relay) => relay
                .on_record(key, value, context)
                .map(|handler| handler.boxed()),
        }
    }
}

pub struct SimpleConnectorModel<C> {
    connector: C,
}

impl<C> SimpleConnectorModel<C> {
    pub fn new(connector: C) -> Self {
        SimpleConnectorModel { connector }
    }
}

impl<C> Agent for SimpleConnectorModel<C>
where
    C: Connector<SimpleConnectorAgent> + Clone + Send + Sync + 'static,
{
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let lifecycle = ConnectorLifecycle::new(self.connector.clone());
        let agent_model = AgentModel::new(SimpleConnectorAgent::default, lifecycle);
        agent_model.run(route, route_params, config, context)
    }
}

#[derive(Default, Copy, Clone)]
pub struct SimpleConnectorAgent;

impl AgentSpec for SimpleConnectorAgent {
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
            Some(Discard::new(Stop::default()))
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
