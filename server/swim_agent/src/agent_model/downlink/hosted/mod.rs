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

mod map;
mod value;

pub use map::{map_dl_write_stream, HostedMapDownlinkChannel, MapDlState, MapDownlinkHandle};
pub use value::{value_dl_write_stream, HostedValueDownlinkChannel, ValueDownlinkHandle};

#[derive(Clone, Copy, PartialEq, Eq)]
enum DlState {
    Unlinked,
    Linked,
    Synced,
}

#[cfg(test)]
mod test_support {
    use futures::future::BoxFuture;
    use swim_api::{
        agent::{AgentConfig, AgentContext, LaneConfig},
        downlink::DownlinkKind,
        error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError},
        meta::lane::LaneKind,
        store::StoreKind,
    };
    use swim_utilities::{
        io::byte_channel::{ByteReader, ByteWriter},
        routing::route_uri::RouteUri,
    };

    use crate::{
        agent_model::downlink::handlers::BoxDownlinkChannel,
        event_handler::{
            ActionContext, BoxEventHandler, DownlinkSpawner, HandlerFuture, Spawner, StepResult,
            WriteStream,
        },
        meta::AgentMetadata,
    };

    struct NoSpawn;

    impl<FakeAgent> Spawner<FakeAgent> for NoSpawn {
        fn spawn_suspend(&self, _fut: HandlerFuture<FakeAgent>) {
            panic!("Unexpected spawn.");
        }
    }

    impl<FakeAgent> DownlinkSpawner<FakeAgent> for NoSpawn {
        fn spawn_downlink(
            &self,
            _dl_channel: BoxDownlinkChannel<FakeAgent>,
            _dl_writer: WriteStream,
        ) -> Result<(), DownlinkRuntimeError> {
            panic!("Unexpected downlink.");
        }
    }

    struct NoAgentRuntime;

    impl AgentContext for NoAgentRuntime {
        fn add_lane(
            &self,
            _name: &str,
            _lane_kind: LaneKind,
            _config: LaneConfig,
        ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
            panic!("Unexpected runtime interaction.");
        }

        fn open_downlink(
            &self,
            _host: Option<&str>,
            _node: &str,
            _lane: &str,
            _kind: DownlinkKind,
        ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
            panic!("Unexpected runtime interaction.");
        }

        fn add_store(
            &self,
            _name: &str,
            _kind: StoreKind,
        ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
            panic!("Unexpected runtime interaction.");
        }
    }

    const NODE_URI: &str = "/node";
    const CONFIG: AgentConfig = AgentConfig::DEFAULT;

    fn make_uri() -> RouteUri {
        RouteUri::try_from(NODE_URI).expect("Bad URI.")
    }

    fn make_meta(uri: &RouteUri) -> AgentMetadata<'_> {
        AgentMetadata::new(uri, &CONFIG)
    }

    pub fn run_handler<FakeAgent>(mut handler: BoxEventHandler<'_, FakeAgent>, agent: &FakeAgent) {
        let uri = make_uri();
        let meta = make_meta(&uri);
        let no_spawn = NoSpawn;
        let no_runtime = NoAgentRuntime;
        let context = ActionContext::new(&no_spawn, &no_runtime, &no_spawn);
        loop {
            match handler.step(context, meta, agent) {
                StepResult::Continue { modified_lane } => {
                    assert!(modified_lane.is_none());
                }
                StepResult::Fail(err) => {
                    panic!("Handler failed: {}", err);
                }
                StepResult::Complete { modified_lane, .. } => {
                    assert!(modified_lane.is_none());
                    break;
                }
            }
        }
    }
}
