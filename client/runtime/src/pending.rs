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

use crate::error::DownlinkRuntimeError;
use crate::models::{Key, RemotePath};
use crate::runtime::{BoxedDownlink, DownlinkCallback};
use fnv::FnvHashMap;
use futures::Stream;
use futures_util::future::{BoxFuture, Either};
use futures_util::stream::FuturesUnordered;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use swimos_api::address::RelativeAddress;
use swimos_client_api::DownlinkConfig;
use swimos_messages::remote_protocol::AttachClient;
use swimos_model::Text;
use swimos_remote::Scheme;
use swimos_runtime::downlink::{DownlinkOptions, DownlinkRuntimeConfig};
use tokio::sync::mpsc;

type PendingDns = (Scheme, Text, Result<Vec<SocketAddr>, DownlinkRuntimeError>);
type PendingHandshake = (
    Text,
    Result<(SocketAddr, mpsc::Sender<AttachClient>), DownlinkRuntimeError>,
);

pub struct PendingDownlink {
    pub callback: DownlinkCallback,
    pub downlink: BoxedDownlink,
    pub address: RemotePath,
    pub runtime_config: DownlinkRuntimeConfig,
    pub downlink_config: DownlinkConfig,
    pub options: DownlinkOptions,
}

#[derive(Eq, PartialEq, Hash, Debug)]
enum WaiterKey {
    Connection(Text),
    Runtime(SocketAddr),
}

#[derive(Default)]
pub struct PendingConnections<'f> {
    waiters: FnvHashMap<WaiterKey, FnvHashMap<Key, Vec<PendingDownlink>>>,
    tasks: FuturesUnordered<BoxFuture<'f, Either<PendingDns, PendingHandshake>>>,
}

pub enum Waiting {
    Connection {
        host: Text,
        downlink: PendingDownlink,
    },
    Runtime {
        addr: SocketAddr,
        downlink: PendingDownlink,
    },
}

impl<'f> PendingConnections<'f> {
    fn key(of: &PendingDownlink) -> Key {
        let PendingDownlink {
            downlink, address, ..
        } = of;
        (
            RelativeAddress::new(address.node.clone(), address.lane.clone()),
            downlink.kind(),
        )
    }

    fn map_waiters(
        col: Option<FnvHashMap<Key, Vec<PendingDownlink>>>,
    ) -> impl Iterator<Item = (Key, PendingDownlink)> {
        col.unwrap_or_default()
            .into_iter()
            .flat_map(|(key, downlinks)| downlinks.into_iter().map(move |dl| (key.clone(), dl)))
    }

    pub fn feed_task(&self, task: BoxFuture<'f, Either<PendingDns, PendingHandshake>>) {
        self.tasks.push(task)
    }

    pub fn feed_waiter(&mut self, on: Waiting) {
        let PendingConnections { waiters, .. } = self;
        let (entry, downlink) = match on {
            Waiting::Connection { host, downlink } => {
                (waiters.entry(WaiterKey::Connection(host)), downlink)
            }
            Waiting::Runtime { addr, downlink } => {
                (waiters.entry(WaiterKey::Runtime(addr)), downlink)
            }
        };
        entry
            .or_default()
            .insert(Self::key(&downlink), vec![downlink]);
    }

    pub fn drain_connection_queue(
        &mut self,
        host: Text,
    ) -> impl Iterator<Item = (Key, PendingDownlink)> {
        Self::map_waiters(self.waiters.remove(&WaiterKey::Connection(host)))
    }

    pub fn drain_runtime_queue(
        &mut self,
        addr: SocketAddr,
    ) -> impl Iterator<Item = (Key, PendingDownlink)> {
        Self::map_waiters(self.waiters.remove(&WaiterKey::Runtime(addr)))
    }

    pub fn waiting_on(&self, addr: SocketAddr, key: &Key) -> bool {
        match self.waiters.get(&WaiterKey::Runtime(addr)) {
            Some(entry) => entry.get(key).is_some(),
            None => false,
        }
    }
}

impl<'f> Stream for PendingConnections<'f> {
    type Item = Either<PendingDns, PendingHandshake>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let PendingConnections { tasks, .. } = &mut *self;
        if tasks.is_empty() {
            Poll::Pending
        } else {
            Pin::new(tasks).poll_next(cx)
        }
    }
}
