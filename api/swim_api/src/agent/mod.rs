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
    downlink::DownlinkKind,
    error::{
        AgentInitError, AgentRuntimeError, AgentTaskError, DownlinkRuntimeError, OpenStoreError,
    },
    store::StoreKind,
};

/// Indicates the sub-protocol that a lane uses to communicate its state.
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

/// Configuration parameters for a lane.
#[derive(Debug, Clone, Copy)]
pub struct LaneConfig {
    /// Size of the input buffer in bytes.
    pub input_buffer_size: NonZeroUsize,
    /// Size of the output buffer in bytes.
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

/// Trait for the context that is passed to an agent to allow it to interact with the runtime.
pub trait AgentContext: Sync {
    /// Add a new lane endpoint to the runtime for this agent.
    /// #Arguments
    /// * `name` - The name of the lane.
    /// * `uplink_kind` - Protocol that the runtime uses to communicate with the lane.
    /// * `config` - Configuration parameters for the lane.
    fn add_lane(
        &self,
        name: &str,
        uplink_kind: UplinkKind,
        config: Option<LaneConfig>,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>>;

    /// Open a downlink to a lane on another agent.
    /// #Arguments
    /// * `config` - The configuration for the downlink.
    /// * `host` - The host containing the node.
    /// * `node` - The node URI for the agent.
    /// * `kind` - The kind of the downlink for the runtime.
    fn open_downlink(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>>;

    fn open_store(
        &self,
        name: &str,
        kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>>;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct AgentConfig {
    //TODO Add parameters.
}

/// Type of the task for a running agent.
pub type AgentTask = BoxFuture<'static, Result<(), AgentTaskError>>;
/// Type of the task to initialize an agent.
pub type AgentInitResult = Result<AgentTask, AgentInitError>;

/// Trait to define a type of agent. Instances of this will be passed to the runtime
/// to be executed. User code should not generally need to implement this directly. It is
/// necessary for this trait to be object safe and any changes to it should take that into
/// account.
pub trait Agent {
    /// Running an agent results in future that will perform the initialization of the agent and
    /// then yield another future that will actually run the agent.
    /// #Arguments
    /// * `route` - The node URI of this agent instance.
    /// * `config` - Configuration parameters for the agent.
    /// * `context` - Context through which the agent can interact with the runtime.
    fn run(
        &self,
        route: RelativeUri,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult>;
}

static_assertions::assert_obj_safe!(AgentContext, Agent);

pub type BoxAgent = Box<dyn Agent + Send + 'static>;

impl Agent for BoxAgent {
    fn run(
        &self,
        route: RelativeUri,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        (**self).run(route, config, context)
    }
}

impl<'a, A> Agent for &'a A
where
    A: Agent,
{
    fn run(
        &self,
        route: RelativeUri,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        (*self).run(route, config, context)
    }
}
