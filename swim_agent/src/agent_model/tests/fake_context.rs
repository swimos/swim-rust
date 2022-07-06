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

use std::{num::NonZeroUsize, sync::Arc};

use futures::{
    future::{ready, BoxFuture},
    FutureExt,
};
use parking_lot::Mutex;
use swim_api::{
    agent::{AgentContext, LaneConfig, UplinkKind},
    downlink::{Downlink, DownlinkConfig},
    error::AgentRuntimeError,
};
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
};

use super::{MAP_LANE, VAL_LANE};

#[derive(Debug, Default, Clone)]
pub struct TestAgentContext {
    inner: Arc<Mutex<Inner>>,
}

impl TestAgentContext {
    pub fn take_lane_io(&self) -> (Io, Io) {
        let mut guard = self.inner.lock();
        let Inner {
            value_lane_io,
            map_lane_io,
        } = &mut *guard;
        (
            value_lane_io.take().expect("Value lane not registered."),
            map_lane_io.take().expect("Map lane not registered."),
        )
    }
}

type Io = (ByteWriter, ByteReader);

#[derive(Debug, Default)]
struct Inner {
    value_lane_io: Option<Io>,
    map_lane_io: Option<Io>,
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

impl AgentContext for TestAgentContext {
    fn add_lane<'a>(
        &'a self,
        name: &str,
        uplink_kind: UplinkKind,
        _config: Option<LaneConfig>,
    ) -> BoxFuture<'a, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        match (name, uplink_kind) {
            (VAL_LANE, UplinkKind::Value) => {
                let (tx_in, rx_in) = byte_channel(BUFFER_SIZE);
                let (tx_out, rx_out) = byte_channel(BUFFER_SIZE);
                let mut guard = self.inner.lock();
                guard.value_lane_io = Some((tx_in, rx_out));
                ready(Ok((tx_out, rx_in))).boxed()
            }
            (MAP_LANE, UplinkKind::Map) => {
                let (tx_in, rx_in) = byte_channel(BUFFER_SIZE);
                let (tx_out, rx_out) = byte_channel(BUFFER_SIZE);
                let mut guard = self.inner.lock();
                guard.map_lane_io = Some((tx_in, rx_out));
                ready(Ok((tx_out, rx_in))).boxed()
            }
            ow => panic!("Unexpected lane registration: {:?}", ow),
        }
    }

    fn open_downlink(
        &self,
        _config: DownlinkConfig,
        _downlink: Box<dyn Downlink + Send>,
    ) -> BoxFuture<'_, Result<(), AgentRuntimeError>> {
        panic!("Opening downlinks from agents not yet supported.")
    }
}
