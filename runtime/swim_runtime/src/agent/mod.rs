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
    agent::{AgentContext, UplinkKind},
    downlink::{Downlink, DownlinkConfig},
    error::AgentRuntimeError,
};
use swim_model::Text;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio::sync::{mpsc, oneshot};

pub struct AgentRuntimeContext {
    tx: mpsc::Sender<AgentRuntimeRequest>,
}

impl AgentContext for AgentRuntimeContext {
    fn add_lane<'a>(
        &'a self,
        name: &str,
        uplink_kind: UplinkKind,
    ) -> BoxFuture<'a, Result<Io, AgentRuntimeError>> {
        let name = Text::new(name);
        async move {
            let (tx, rx) = oneshot::channel();
            self.tx
                .send(AgentRuntimeRequest::AddLane {
                    name,
                    kind: uplink_kind,
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
        promise: oneshot::Sender<Result<Io, AgentRuntimeError>>,
    },
    OpenDownlink {
        config: DownlinkConfig,
        downlink: Box<dyn Downlink + Send>,
        promise: oneshot::Sender<Result<(), AgentRuntimeError>>,
    },
}
