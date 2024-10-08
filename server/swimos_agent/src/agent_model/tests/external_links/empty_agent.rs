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

use std::collections::HashMap;

use bytes::BytesMut;
use swimos_agent_protocol::MapMessage;
use swimos_api::agent::HttpLaneRequest;
use uuid::Uuid;

use crate::{
    agent_model::{ItemSpec, MapLikeInitializer, ValueLikeInitializer, WriteResult},
    event_handler::UnitHandler,
    AgentSpec,
};

#[derive(Clone, Copy, Default, Debug)]
pub struct EmptyAgent;

impl AgentSpec for EmptyAgent {
    type ValCommandHandler = UnitHandler;

    type MapCommandHandler = UnitHandler;

    type OnSyncHandler = UnitHandler;

    type HttpRequestHandler = UnitHandler;

    fn item_specs() -> HashMap<&'static str, ItemSpec> {
        HashMap::new()
    }

    fn on_value_command(&self, _lane: &str, _body: BytesMut) -> Option<Self::ValCommandHandler> {
        None
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
        request: HttpLaneRequest,
    ) -> Result<Self::HttpRequestHandler, HttpLaneRequest> {
        Err(request)
    }

    fn write_event(&self, _lane: &str, _buffer: &mut BytesMut) -> Option<WriteResult> {
        None
    }
}
