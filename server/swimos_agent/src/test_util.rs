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

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use parking_lot::Mutex;
use swimos_api::{
    address::Address,
    agent::{DownlinkKind, WarpLaneKind},
    error::DynamicRegistrationError,
};
use swimos_model::Text;
use swimos_utilities::{
    byte_channel::{byte_channel, ByteReader, ByteWriter},
    non_zero_usize,
};

use crate::{
    agent_model::downlink::{BoxDownlinkChannel, BoxDownlinkChannelFactory},
    event_handler::{
        BoxEventHandler, CommanderSpawnOnDone, DownlinkSpawnOnDone, LaneSpawnOnDone, LaneSpawner,
        LinkSpawner,
    },
};

pub struct TestDlContextInner<Agent> {
    pub requests: Vec<DownlinkSpawnRequest<Agent>>,
    pub downlink_channels: HashMap<Address<Text>, (ByteWriter, ByteReader)>,
    pub downlinks: Vec<BoxDownlinkChannel<Agent>>,
}

impl<Agent> Default for TestDlContextInner<Agent> {
    fn default() -> Self {
        Self {
            downlink_channels: Default::default(),
            downlinks: Default::default(),
            requests: Default::default(),
        }
    }
}

pub struct TestDownlinkContext<Agent> {
    pub inner: Arc<Mutex<TestDlContextInner<Agent>>>,
    expected_kind: DownlinkKind,
}

impl<Agent> Default for TestDownlinkContext<Agent> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            expected_kind: DownlinkKind::Event,
        }
    }
}

impl<Agent> TestDownlinkContext<Agent> {
    pub fn new(expected_kind: DownlinkKind) -> Self {
        Self {
            inner: Default::default(),
            expected_kind,
        }
    }
}

pub struct DownlinkSpawnRequest<Agent> {
    path: Address<Text>,
    kind: DownlinkKind,
    make_channel: BoxDownlinkChannelFactory<Agent>,
    on_done: DownlinkSpawnOnDone<Agent>,
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

impl<Agent> TestDownlinkContext<Agent> {
    pub fn push_dl(&self, request: DownlinkSpawnRequest<Agent>) {
        assert_eq!(request.kind, self.expected_kind);
        let mut guard = self.inner.lock();
        guard.requests.push(request);
    }

    pub fn handle_dl_requests(&self, agent: &Agent) -> Vec<BoxEventHandler<'static, Agent>> {
        let mut handlers = vec![];
        let mut guard = self.inner.lock();
        let TestDlContextInner {
            requests,
            downlink_channels,
            downlinks,
        } = &mut *guard;
        for DownlinkSpawnRequest {
            path,
            make_channel,
            on_done,
            ..
        } in requests.drain(..)
        {
            let (in_tx, in_rx) = byte_channel(BUFFER_SIZE);
            let (out_tx, out_rx) = byte_channel(BUFFER_SIZE);
            let channel = make_channel.create_box(agent, out_tx, in_rx);
            downlinks.push(channel);
            downlink_channels.insert(path, (in_tx, out_rx));
            handlers.push(on_done(Ok(())));
        }
        handlers
    }

    pub fn take_channels(&self) -> HashMap<Address<Text>, (ByteWriter, ByteReader)> {
        let mut guard = self.inner.lock();
        std::mem::take(&mut guard.downlink_channels)
    }

    pub fn take_downlinks(&self) -> Vec<BoxDownlinkChannel<Agent>> {
        let mut guard = self.inner.lock();
        std::mem::take(&mut guard.downlinks)
    }
}

impl<Agent> LinkSpawner<Agent> for TestDownlinkContext<Agent> {
    fn spawn_downlink(
        &self,
        path: Address<Text>,
        make_channel: BoxDownlinkChannelFactory<Agent>,
        on_done: DownlinkSpawnOnDone<Agent>,
    ) {
        self.push_dl(DownlinkSpawnRequest {
            path,
            kind: make_channel.kind(),
            make_channel,
            on_done,
        });
    }

    fn register_commander(&self, _path: Address<Text>, _on_done: CommanderSpawnOnDone<Agent>) {
        panic!("Registering commanders not supported.");
    }
}

impl<Agent> LaneSpawner<Agent> for TestDownlinkContext<Agent> {
    fn spawn_warp_lane(
        &self,
        _name: &str,
        _kind: WarpLaneKind,
        _on_done: LaneSpawnOnDone<Agent>,
    ) -> Result<(), DynamicRegistrationError> {
        panic!("Spawning dynamic lanes not supported.");
    }
}
