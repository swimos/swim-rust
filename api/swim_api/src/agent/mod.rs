// Copyright 2015-2021 Swim Inc.
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

use std::{
    fmt::{Display, Formatter},
    num::NonZeroUsize,
};

use futures::future::BoxFuture;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{ByteReader, ByteWriter},
    routing::uri::RelativeUri,
};

use crate::{
    downlink::{Downlink, DownlinkConfig},
    error::{AgentInitError, AgentRuntimeError, AgentTaskError},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UplinkKind {
    Value,
    Map,
}

impl Display for UplinkKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UplinkKind::Value => f.write_str("Value"),
            UplinkKind::Map => f.write_str("Map"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LaneConfig {
    pub input_buffer_size: NonZeroUsize,
    pub output_buffer_size: NonZeroUsize,
}

const DEFAULT_BUFFER: NonZeroUsize = non_zero_usize!(4096);

impl Default for LaneConfig {
    fn default() -> Self {
        Self {
            input_buffer_size: DEFAULT_BUFFER,
            output_buffer_size: DEFAULT_BUFFER,
        }
    }
}

pub trait AgentContext {
    fn add_lane<'a>(
        &'a self,
        name: &str,
        uplink_kind: UplinkKind,
        config: Option<LaneConfig>,
    ) -> BoxFuture<'a, Result<(ByteWriter, ByteReader), AgentRuntimeError>>;

    fn open_downlink(
        &self,
        config: DownlinkConfig,
        downlink: Box<dyn Downlink + Send>,
    ) -> BoxFuture<'_, Result<(), AgentRuntimeError>>;
}

#[derive(Debug, Clone, Copy)]
pub struct AgentConfig {
    //TODO Add parameters.
}

pub type AgentTask<'a> = BoxFuture<'a, Result<(), AgentTaskError>>;
pub type AgentInitResult<'a> = Result<AgentTask<'a>, AgentInitError>;

pub trait Agent {
    fn run<'a>(
        &self,
        route: RelativeUri,
        config: AgentConfig,
        context: &'a dyn AgentContext,
    ) -> BoxFuture<'a, AgentInitResult<'a>>;
}

static_assertions::assert_obj_safe!(AgentContext, Agent);
