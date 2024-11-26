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

use std::{borrow::Cow, collections::HashMap};

use bytes::BytesMut;
use swimos_agent_protocol::MapMessage;
use swimos_api::agent::HttpLaneRequest;
use uuid::Uuid;

use crate::{
    agent_model::{
        AgentDescription, ItemSpec, MapLikeInitializer, ValueLikeInitializer, WriteResult,
    },
    event_handler::UnitHandler,
    AgentSpec,
};

#[derive(Clone, Copy, Default, Debug)]
pub struct EmptyAgent;

impl AgentDescription for EmptyAgent {
    fn item_name(&self, _id: u64) -> Option<Cow<'_, str>> {
        None
    }
}

impl AgentSpec for EmptyAgent {
    type ValCommandHandler<'a> = UnitHandler
    where
        Self: 'a;

    type MapCommandHandler<'a> = UnitHandler
    where
        Self: 'a;

    type OnSyncHandler = UnitHandler;

    type HttpRequestHandler = UnitHandler;

    type Deserializers = ();

    fn initializer_deserializers(&self) -> Self::Deserializers {}

    fn item_specs() -> HashMap<&'static str, ItemSpec> {
        HashMap::new()
    }

    fn on_value_command(
        &self,
        _: &mut (),
        _lane: &str,
        _body: BytesMut,
    ) -> Option<Self::ValCommandHandler<'_>> {
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

    fn on_map_command<'a>(
        &self,
        _: &'a mut (),
        _lane: &str,
        _body: MapMessage<BytesMut, BytesMut>,
    ) -> Option<Self::MapCommandHandler<'a>> {
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

    fn register_dynamic_item(
        &self,
        _name: &str,
        _descriptor: crate::agent_model::ItemDescriptor,
    ) -> Result<u64, swimos_api::error::DynamicRegistrationError> {
        Err(swimos_api::error::DynamicRegistrationError::DynamicRegistrationsNotSupported)
    }
}
