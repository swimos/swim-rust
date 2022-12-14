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

use futures::future::BoxFuture;
use swim_api::{
    agent::{AgentContext, LaneConfig, UplinkKind},
    downlink::DownlinkKind,
    error::AgentRuntimeError,
};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};

use crate::{
    agent_model::downlink::handlers::BoxDownlinkChannel,
    event_handler::{ActionContext, HandlerFuture, Spawner, WriteStream},
};

struct NoSpawn;
pub struct DummyAgentContext;

pub fn no_downlink<Context>(
    _dl: BoxDownlinkChannel<Context>,
    _write_stream: WriteStream,
) -> Result<(), AgentRuntimeError> {
    panic!("Launching downlinks no supported.");
}

const NO_SPAWN: NoSpawn = NoSpawn;
const NO_AGENT: DummyAgentContext = DummyAgentContext;

pub fn dummy_context<'a, Context>() -> ActionContext<'a, Context> {
    ActionContext::new(&NO_SPAWN, &NO_AGENT, &no_downlink)
}

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }
}

impl AgentContext for DummyAgentContext {
    fn add_lane(
        &self,
        _name: &str,
        _uplink_kind: UplinkKind,
        _config: Option<LaneConfig>,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Dummy context used.");
    }

    fn open_downlink(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Dummy context used.");
    }
}
