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

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use interval_stream::ScheduleDef;
use swim_model::Text;

use crate::requests::GuestLaneResponses;
use crate::wpc::WasmProcedure;
use crate::{AgentSpec, LaneSpec};

mod text_str {
    use serde::{Deserialize, Deserializer, Serializer};

    use swim_model::Text;

    pub fn serialize<S>(text: &Text, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(text.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Text, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Text::from)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenLaneRequest {
    /// The URI of the lane
    #[serde(with = "text_str")]
    pub uri: Text,
    /// The specification of the lane.
    pub spec: LaneSpec,
}

pub struct OpenLaneProcedure;

impl WasmProcedure for OpenLaneProcedure {
    type Request = OpenLaneRequest;
    type Response = ();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScheduleTaskRequest {
    /// The ID of the task
    pub id: Uuid,
    /// The execution schedule of the task
    pub schedule: ScheduleDef,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelTaskRequest {
    /// The ID of the task
    pub id: Uuid,
}

pub struct CancelTaskProcedure;

impl WasmProcedure for CancelTaskProcedure {
    type Request = CancelTaskRequest;
    type Response = ();
}

pub struct DispatchLaneResponsesProcedure;

impl WasmProcedure for DispatchLaneResponsesProcedure {
    type Request = GuestLaneResponses;
    type Response = ();
}

pub struct DeployAgentSpecProcedure;

impl WasmProcedure for DeployAgentSpecProcedure {
    type Request = AgentSpec;
    type Response = ();
}

/// Requests from the guest runtime to the host.
#[derive(Debug, Deserialize, Serialize)]
pub enum GuestRuntimeEvent {
    /// Open a new lane
    OpenLane(OpenLaneRequest),
    /// Schedule a new task
    ScheduleTask(ScheduleTaskRequest),
    /// Cancel a previously scheduled task
    CancelTask(CancelTaskRequest),
    /// Forward N lane response's.
    DispatchLaneResponses(GuestLaneResponses),
}

impl From<GuestLaneResponses> for GuestRuntimeEvent {
    fn from(value: GuestLaneResponses) -> Self {
        GuestRuntimeEvent::DispatchLaneResponses(value)
    }
}
