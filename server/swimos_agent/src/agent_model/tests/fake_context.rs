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

use std::{num::NonZeroUsize, sync::Arc};

use futures::{
    future::{ready, BoxFuture},
    FutureExt,
};
use parking_lot::Mutex;
use swimos_api::{
    agent::DownlinkKind,
    agent::{
        AgentContext, HttpLaneRequest, HttpLaneRequestChannel, LaneConfig, StoreKind, UplinkKind,
        WarpLaneKind,
    },
    error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError},
};
use swimos_utilities::{
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    non_zero_usize,
};
use tokio::sync::{mpsc, oneshot};

use super::{CMD_LANE, HTTP_LANE, MAP_LANE, VAL_LANE};

#[derive(Debug, Default, Clone)]
pub struct TestAgentContext {
    inner: Arc<Mutex<Inner>>,
}

impl TestAgentContext {
    pub fn new(promise: oneshot::Sender<ByteReader>) -> Self {
        TestAgentContext {
            inner: Arc::new(Mutex::new(Inner {
                ad_hoc_consumer: Some(promise),
                ..Default::default()
            })),
        }
    }
}

impl TestAgentContext {
    pub fn take_lane_io(&self) -> (Option<Io>, Option<Io>, Option<Io>) {
        let mut guard = self.inner.lock();
        let Inner {
            value_lane_io,
            map_lane_io,
            cmd_lane_io,
            ..
        } = &mut *guard;
        (value_lane_io.take(), map_lane_io.take(), cmd_lane_io.take())
    }

    pub fn take_http_io(&self) -> Option<mpsc::Sender<HttpLaneRequest>> {
        self.inner.lock().http_sender.take()
    }
}

type Io = (ByteWriter, ByteReader);

#[derive(Debug, Default)]
struct Inner {
    value_lane_io: Option<Io>,
    map_lane_io: Option<Io>,
    cmd_lane_io: Option<Io>,
    http_sender: Option<mpsc::Sender<HttpLaneRequest>>,
    ad_hoc_consumer: Option<oneshot::Sender<ByteReader>>,
    ad_hoc_rx: Option<ByteReader>,
}

const CHAN_SIZE: NonZeroUsize = non_zero_usize!(8);
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

impl AgentContext for TestAgentContext {
    fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        let mut guard = self.inner.lock();
        let (tx, rx) = byte_channel(BUFFER_SIZE);
        if let Some(sender) = guard.ad_hoc_consumer.take() {
            sender.send(rx).expect("Registering ad hoc channel failed.");
        } else {
            guard.ad_hoc_rx = Some(rx);
        }
        ready(Ok(tx)).boxed()
    }

    fn add_lane(
        &self,
        name: &str,
        lane_kind: WarpLaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        match (name, lane_kind.uplink_kind()) {
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
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        panic!("Unexpected downlink request.")
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), swimos_api::error::OpenStoreError>>
    {
        ready(Err(OpenStoreError::StoresNotSupported)).boxed()
    }

    fn add_http_lane(
        &self,
        name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        if name == HTTP_LANE {
            let mut guard = self.inner.lock();
            let (tx, rx) = mpsc::channel(CHAN_SIZE.get());
            guard.http_sender = Some(tx);
            ready(Ok(rx)).boxed()
        } else {
            panic!("Unexpected lane registration: {:?}", name);
        }
    }
}
