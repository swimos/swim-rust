// Copyright 2015-2023 Swim Inc.
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

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::agent::GuestLaneResponses;
use crate::wpc::WasmProcedure;

#[derive(Debug, Deserialize, Serialize)]
pub enum HostRequest {
    Init(InitValueItemRequest),
    Sync(LaneSyncRequest),
    Command(CommandEvent),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InitValueItemRequest {
    pub id: u64,
    pub with: BytesMut,
}

pub struct LaneSyncProcedure;

impl WasmProcedure for LaneSyncProcedure {
    type Request = LaneSyncRequest;
    type Response = GuestLaneResponses;
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LaneSyncRequest {
    pub id: u64,
    pub remote: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CommandEvent {
    pub id: u64,
    pub data: BytesMut,
    pub map_like: bool,
}
