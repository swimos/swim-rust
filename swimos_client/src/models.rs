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

use fnv::FnvHashMap;
use std::fmt::{Display, Formatter};
use std::future::Future;
use swimos_api::{address::RelativeAddress, agent::DownlinkKind};
use swimos_messages::remote_protocol::AttachClient;
use swimos_model::Text;
use swimos_runtime::downlink::failure::{
    AlwaysAbortStrategy, AlwaysIgnoreStrategy, ReportStrategy,
};
use swimos_runtime::downlink::{
    AttachAction, DownlinkRuntimeConfig, IdentifiedAddress, MapDownlinkRuntime, NoInterpretation,
    ValueDownlinkRuntime,
};
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use swimos_utilities::trigger;
use tokio::sync::mpsc;
use uuid::Uuid;

pub type Key = (RelativeAddress<Text>, DownlinkKind);

pub struct RuntimeView {
    stop: trigger::Sender,
    attach: mpsc::Sender<AttachAction>,
}

impl RuntimeView {
    pub fn attach(&self) -> mpsc::Sender<AttachAction> {
        self.attach.clone()
    }
}

pub struct Peer {
    attach: mpsc::Sender<AttachClient>,
    downlinks: FnvHashMap<Key, RuntimeView>,
}

impl Peer {
    pub fn new(attach: mpsc::Sender<AttachClient>) -> Peer {
        Peer {
            attach,
            downlinks: Default::default(),
        }
    }

    pub fn attach(&self) -> mpsc::Sender<AttachClient> {
        self.attach.clone()
    }

    pub fn insert_runtime(
        &mut self,
        key: Key,
        stop: trigger::Sender,
        tx: mpsc::Sender<AttachAction>,
    ) {
        self.downlinks.insert(key, RuntimeView { stop, attach: tx });
    }

    pub fn get_view(&self, key: &Key) -> Option<&RuntimeView> {
        self.downlinks.get(key)
    }

    pub fn remove(&mut self, key: &Key) -> bool {
        if let Some(view) = self.downlinks.remove(key) {
            view.stop.trigger();
        }

        self.downlinks.is_empty()
    }

    pub fn stop_all(&mut self) {
        let Peer { downlinks, .. } = self;
        for (_, view) in downlinks.drain() {
            view.stop.trigger();
        }
    }
}

#[derive(Debug)]
pub struct IdIssuer {
    count: u64,
}

impl IdIssuer {
    pub const fn new() -> IdIssuer {
        IdIssuer { count: 0 }
    }

    pub fn next_id(&mut self) -> Uuid {
        let IdIssuer { count } = self;
        let c = *count;
        *count += 1;
        Uuid::from_u128(c as u128)
    }
}

#[derive(Debug)]
pub struct DownlinkRuntime {
    identity: Uuid,
    path: RelativeAddress<Text>,
    attachment_rx: mpsc::Receiver<AttachAction>,
    kind: DownlinkKind,
    io: (ByteWriter, ByteReader),
    config: DownlinkRuntimeConfig,
}

impl DownlinkRuntime {
    pub fn new(
        identity: Uuid,
        path: RelativeAddress<Text>,
        attachment_rx: mpsc::Receiver<AttachAction>,
        kind: DownlinkKind,
        io: (ByteWriter, ByteReader),
        config: DownlinkRuntimeConfig,
    ) -> Self {
        DownlinkRuntime {
            identity,
            path,
            attachment_rx,
            kind,
            io,
            config,
        }
    }

    pub fn run(
        self,
        stopping: trigger::Receiver,
        interpret_frame_data: bool,
    ) -> impl Future<Output = ()> + Send + 'static {
        let DownlinkRuntime {
            identity,
            path,
            attachment_rx,
            kind,
            io,
            config,
        } = self;
        async move {
            match kind {
                DownlinkKind::Event | DownlinkKind::Value => {
                    let runtime = ValueDownlinkRuntime::new(
                        attachment_rx,
                        io,
                        stopping,
                        IdentifiedAddress {
                            identity,
                            address: path,
                        },
                        config,
                    );
                    runtime.run().await;
                }
                DownlinkKind::Map | DownlinkKind::MapEvent => {
                    let strategy = if config.abort_on_bad_frames {
                        ReportStrategy::new(AlwaysAbortStrategy).boxed()
                    } else {
                        ReportStrategy::new(AlwaysIgnoreStrategy).boxed()
                    };

                    if interpret_frame_data {
                        let runtime = MapDownlinkRuntime::new(
                            attachment_rx,
                            io,
                            stopping,
                            IdentifiedAddress {
                                identity,
                                address: path,
                            },
                            config,
                            strategy,
                        );
                        runtime.run().await;
                    } else {
                        let runtime = MapDownlinkRuntime::with_interpretation(
                            attachment_rx,
                            io,
                            stopping,
                            IdentifiedAddress {
                                identity,
                                address: path,
                            },
                            config,
                            ReportStrategy::new(AlwaysIgnoreStrategy).boxed(),
                            NoInterpretation,
                        );
                        runtime.run().await;
                    }
                }
            }
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemotePath {
    pub host: Text,
    pub node: Text,
    pub lane: Text,
}

impl RemotePath {
    pub fn new<H, N, L>(host: H, node: N, lane: L) -> RemotePath
    where
        H: Into<Text>,
        N: Into<Text>,
        L: Into<Text>,
    {
        RemotePath {
            host: host.into(),
            node: node.into(),
            lane: lane.into(),
        }
    }
}

impl Display for RemotePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let RemotePath { host, node, lane } = self;
        write!(f, "{}/{}/{}", host, node, lane)
    }
}
