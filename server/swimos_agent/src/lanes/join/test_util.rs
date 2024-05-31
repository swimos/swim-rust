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

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use futures::{future::BoxFuture, FutureExt};
use parking_lot::Mutex;
use swimos_api::{
    address::Address,
    agent::{
        AgentContext, DownlinkKind, HttpLaneRequestChannel, LaneConfig, StoreKind, WarpLaneKind,
    },
    error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError},
};
use swimos_model::Text;
use swimos_utilities::{
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    non_zero_usize,
};

use crate::{agent_model::downlink::handlers::BoxDownlinkChannel, event_handler::DownlinkSpawner};

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

pub struct TestDlContextInner<Agent> {
    pub downlink_channels: HashMap<Address<Text>, (ByteWriter, ByteReader)>,
    pub downlinks: Vec<BoxDownlinkChannel<Agent>>,
}

impl<Agent> Default for TestDlContextInner<Agent> {
    fn default() -> Self {
        Self {
            downlink_channels: Default::default(),
            downlinks: Default::default(),
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

impl<Agent> TestDownlinkContext<Agent> {
    pub fn push_dl(&self, dl_channel: BoxDownlinkChannel<Agent>) {
        self.inner.lock().downlinks.push(dl_channel);
    }

    pub fn push_channels(&self, key: Address<Text>, io: (ByteWriter, ByteReader)) {
        self.inner.lock().downlink_channels.insert(key, io);
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

impl<Agent> DownlinkSpawner<Agent> for TestDownlinkContext<Agent> {
    fn spawn_downlink(
        &self,
        dl_channel: BoxDownlinkChannel<Agent>,
    ) -> Result<(), DownlinkRuntimeError> {
        self.push_dl(dl_channel);
        Ok(())
    }
}

impl<Agent> AgentContext for TestDownlinkContext<Agent> {
    fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        panic!("Unexpected request for ad hoc channel.");
    }

    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: WarpLaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Unexpected new lane.");
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Unexpected new store.");
    }

    fn open_downlink(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        assert_eq!(kind, self.expected_kind);
        let key = Address::text(host, node, lane);
        let (in_tx, in_rx) = byte_channel(BUFFER_SIZE);
        let (out_tx, out_rx) = byte_channel(BUFFER_SIZE);
        self.push_channels(key, (in_tx, out_rx));
        async move { Ok((out_tx, in_rx)) }.boxed()
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        panic!("Unexpected new HTTP lane.");
    }
}
