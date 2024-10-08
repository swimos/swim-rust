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

mod event;
mod map;
mod value;

use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Weak,
};

pub use event::{EventDownlinkFactory, EventDownlinkHandle};
pub use map::{MapDownlinkFactory, MapDownlinkHandle};
use swimos_utilities::byte_channel::ByteWriter;
pub use value::{ValueDownlinkFactory, ValueDownlinkHandle};

#[derive(Clone, Copy, PartialEq, Eq)]
enum DlState {
    Unlinked,
    Linked,
    Synced,
    Stopped,
}

impl DlState {
    fn is_linked(&self) -> bool {
        matches!(self, DlState::Linked | DlState::Synced)
    }
}

const UNLINKED: u8 = 0;
const LINKED: u8 = 1;
const SYNCED: u8 = 2;
const STOPPED: u8 = 3;

impl From<DlState> for u8 {
    fn from(value: DlState) -> Self {
        match value {
            DlState::Unlinked => UNLINKED,
            DlState::Linked => LINKED,
            DlState::Synced => SYNCED,
            DlState::Stopped => STOPPED,
        }
    }
}

impl From<u8> for DlState {
    fn from(value: u8) -> Self {
        match value {
            UNLINKED => DlState::Unlinked,
            LINKED => DlState::Linked,
            SYNCED => DlState::Synced,
            _ => DlState::Stopped,
        }
    }
}

#[derive(Debug)]
pub(super) struct DlStateTracker {
    state: Arc<AtomicU8>,
}

impl DlStateTracker {
    pub fn new(state: Arc<AtomicU8>) -> Self {
        let tracker = DlStateTracker { state };
        tracker.set(DlState::Unlinked);
        tracker
    }
}

#[derive(Debug)]
struct DlStateObserver {
    state: Weak<AtomicU8>,
}

impl DlStateObserver {
    fn new(state: &Arc<AtomicU8>) -> Self {
        DlStateObserver {
            state: Arc::downgrade(state),
        }
    }
}

impl DlStateTracker {
    fn set(&self, state: DlState) {
        self.state.store(state.into(), Ordering::Release)
    }

    fn get(&self) -> DlState {
        self.state.load(Ordering::Acquire).into()
    }
}

impl DlStateObserver {
    fn get(&self) -> DlState {
        self.state
            .upgrade()
            .map(|s| s.load(Ordering::Acquire).into())
            .unwrap_or(DlState::Stopped)
    }
}

enum OutputWriter<W: RestartableOutput> {
    Active(W),
    Inactive(W::Source),
    Stopped,
}

impl<W: RestartableOutput + std::fmt::Debug> std::fmt::Debug for OutputWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active(arg0) => f.debug_tuple("Active").field(arg0).finish(),
            Self::Inactive(_) => f.debug_tuple("Inactive").field(&"..").finish(),
            Self::Stopped => write!(f, "Stopped"),
        }
    }
}

trait RestartableOutput {
    type Source;

    fn make_inactive(self) -> Self::Source;

    fn restart(writer: ByteWriter, source: Self::Source) -> Self;
}

impl<W: RestartableOutput> OutputWriter<W> {
    fn as_mut(&mut self) -> Option<&mut W> {
        match self {
            OutputWriter::Active(w) => Some(w),
            _ => None,
        }
    }

    fn is_active(&self) -> bool {
        matches!(self, OutputWriter::Active(_))
    }

    fn make_inactive(&mut self) {
        *self = match std::mem::replace(self, OutputWriter::Stopped) {
            OutputWriter::Active(w) => OutputWriter::Inactive(w.make_inactive()),
            OutputWriter::Inactive(i) => OutputWriter::Inactive(i),
            OutputWriter::Stopped => OutputWriter::Stopped,
        };
    }

    fn restart(&mut self, writer: ByteWriter) {
        *self = match std::mem::replace(self, OutputWriter::Stopped) {
            OutputWriter::Active(w) => {
                OutputWriter::Active(<W as RestartableOutput>::restart(writer, w.make_inactive()))
            }
            OutputWriter::Inactive(i) => {
                OutputWriter::Active(<W as RestartableOutput>::restart(writer, i))
            }
            OutputWriter::Stopped => OutputWriter::Stopped,
        };
    }
}

#[cfg(test)]
mod test_support {
    use std::collections::HashMap;

    use bytes::BytesMut;

    use swimos_api::{address::Address, agent::AgentConfig, error::CommanderRegistrationError};
    use swimos_model::Text;
    use swimos_utilities::routing::RouteUri;
    use tokio::time::Instant;

    use crate::{
        agent_model::downlink::BoxDownlinkChannelFactory,
        event_handler::{
            ActionContext, DownlinkSpawnOnDone, HandlerFuture, LinkSpawner, LocalBoxEventHandler,
            Spawner, StepResult,
        },
        meta::AgentMetadata,
        test_context::NoDynamicLanes,
    };

    struct NoSpawn;

    impl<FakeAgent> Spawner<FakeAgent> for NoSpawn {
        fn spawn_suspend(&self, _fut: HandlerFuture<FakeAgent>) {
            panic!("Unexpected spawn.");
        }

        fn schedule_timer(&self, _at: Instant, _id: u64) {
            panic!("Unexpected timer.");
        }
    }

    impl<FakeAgent> LinkSpawner<FakeAgent> for NoSpawn {
        fn spawn_downlink(
            &self,
            _path: Address<Text>,
            _make_channel: BoxDownlinkChannelFactory<FakeAgent>,
            _on_done: DownlinkSpawnOnDone<FakeAgent>,
        ) {
            panic!("Opening downlinks not supported.");
        }

        fn register_commander(
            &self,
            _path: Address<Text>,
        ) -> Result<u16, CommanderRegistrationError> {
            panic!("Registering commanders not supported.");
        }
    }

    const NODE_URI: &str = "/node";
    const CONFIG: AgentConfig = AgentConfig::DEFAULT;

    fn make_uri() -> RouteUri {
        RouteUri::try_from(NODE_URI).expect("Bad URI.")
    }

    fn make_meta<'a>(
        uri: &'a RouteUri,
        route_params: &'a HashMap<String, String>,
    ) -> AgentMetadata<'a> {
        AgentMetadata::new(uri, route_params, &CONFIG)
    }

    pub fn run_handler<FakeAgent>(
        mut handler: LocalBoxEventHandler<'_, FakeAgent>,
        agent: &FakeAgent,
    ) {
        let uri = make_uri();
        let route_params = HashMap::new();
        let meta = make_meta(&uri, &route_params);
        let no_spawn = NoSpawn;
        let no_dyn_lanes = NoDynamicLanes;
        let mut join_lane_init = HashMap::new();
        let mut command_buffer = BytesMut::new();
        let mut context = ActionContext::new(
            &no_spawn,
            &no_spawn,
            &no_dyn_lanes,
            &mut join_lane_init,
            &mut command_buffer,
        );
        loop {
            match handler.step(&mut context, meta, agent) {
                StepResult::Continue { modified_item } => {
                    assert!(modified_item.is_none());
                }
                StepResult::Fail(err) => {
                    panic!("Handler failed: {}", err);
                }
                StepResult::Complete { modified_item, .. } => {
                    assert!(modified_item.is_none());
                    break;
                }
            }
        }
    }
}
