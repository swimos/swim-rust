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

use futures::{future::BoxFuture, FutureExt};
use swim_api::{
    agent::{AgentContext, LaneConfig, UplinkKind},
    downlink::{Downlink, DownlinkConfig},
    error::AgentRuntimeError,
};
use swim_model::Text;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio::sync::{mpsc, oneshot};

use std::fmt::Debug;

pub mod task;

pub struct AgentRuntimeContext {
    tx: mpsc::Sender<AgentRuntimeRequest>,
}

impl AgentContext for AgentRuntimeContext {
    fn add_lane<'a>(
        &'a self,
        name: &str,
        uplink_kind: UplinkKind,
        config: Option<LaneConfig>,
    ) -> BoxFuture<'a, Result<Io, AgentRuntimeError>> {
        let name = Text::new(name);
        async move {
            let (tx, rx) = oneshot::channel();
            self.tx
                .send(AgentRuntimeRequest::AddLane {
                    name,
                    kind: uplink_kind,
                    config,
                    promise: tx,
                })
                .await?;
            rx.await?
        }
        .boxed()
    }

    fn open_downlink<'a>(
        &'a self,
        config: DownlinkConfig,
        downlink: Box<dyn Downlink + Send>,
    ) -> BoxFuture<'a, Result<(), AgentRuntimeError>> {
        async move {
            let (tx, rx) = oneshot::channel();
            self.tx
                .send(AgentRuntimeRequest::OpenDownlink {
                    config,
                    downlink,
                    promise: tx,
                })
                .await?;
            rx.await?
        }
        .boxed()
    }
}

type Io = (ByteReader, ByteWriter);

enum AgentRuntimeRequest {
    AddLane {
        name: Text,
        kind: UplinkKind,
        config: Option<LaneConfig>,
        promise: oneshot::Sender<Result<Io, AgentRuntimeError>>,
    },
    OpenDownlink {
        config: DownlinkConfig,
        downlink: Box<dyn Downlink + Send>,
        promise: oneshot::Sender<Result<(), AgentRuntimeError>>,
    },
}

impl Debug for AgentRuntimeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AddLane {
                name,
                kind,
                config,
                promise,
            } => f
                .debug_struct("AddLane")
                .field("name", name)
                .field("kind", kind)
                .field("config", config)
                .field("promise", promise)
                .finish(),
            Self::OpenDownlink {
                config,
                promise,
                ..
            } => f
                .debug_struct("OpenDownlink")
                .field("config", config)
                .field("downlink", &"[[dyn Downlink]]")
                .field("promise", promise)
                .finish(),
        }
    }
}

#[derive(Debug)]
struct AgentAttachmentRequest {
    pub io: Io,
}

impl AgentAttachmentRequest {
    pub fn new(io: Io) -> Self {
        AgentAttachmentRequest { io }
    }
}
