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
    downlink::DownlinkKind,
    error::AgentRuntimeError,
};
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
};

use super::{CMD_LANE, MAP_LANE, VAL_LANE};

#[derive(Debug, Default, Clone)]
pub struct TestAgentContext {
    inner: Arc<Mutex<Inner>>,
}

impl TestAgentContext {
    pub fn take_lane_io(&self) -> (Option<Io>, Option<Io>, Option<Io>) {
        let mut guard = self.inner.lock();
        let Inner {
            value_lane_io,
            map_lane_io,
            cmd_lane_io,
        } = &mut *guard;
        (value_lane_io.take(), map_lane_io.take(), cmd_lane_io.take())
    }
}

type Io = (ByteWriter, ByteReader);

#[derive(Debug, Default)]
struct Inner {
    value_lane_io: Option<Io>,
    map_lane_io: Option<Io>,
    cmd_lane_io: Option<Io>,
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

impl AgentContext for TestAgentContext {
    fn add_lane(
        &self,
        name: &str,
        uplink_kind: UplinkKind,
        _config: Option<LaneConfig>,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
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
            (CMD_LANE, UplinkKind::Value) => {
                let (tx_in, rx_in) = byte_channel(BUFFER_SIZE);
                let (tx_out, rx_out) = byte_channel(BUFFER_SIZE);
                let mut guard = self.inner.lock();
                guard.cmd_lane_io = Some((tx_in, rx_out));
                ready(Ok((tx_out, rx_in))).boxed()
            }
            ow => panic!("Unexpected lane registration: {:?}", ow),
        }
    }

    fn open_downlink(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Opening downlinks from agents not yet supported.")
    }
}
