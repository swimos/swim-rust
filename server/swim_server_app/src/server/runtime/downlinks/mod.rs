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

use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
};

use swim_api::downlink::DownlinkKind;
use swim_model::{
    address::{Address, RelativeAddress},
    Text,
};
use swim_runtime::agent::DownlinkRequest;

pub type DlKey = (RelativeAddress<Text>, DownlinkKind);

fn key_of(abs_key: &(Address<Text>, DownlinkKind)) -> DlKey {
    let (Address { node, lane, .. }, kind) = abs_key;
    (RelativeAddress::new(node.clone(), lane.clone()), *kind)
}

#[derive(Default)]
pub struct PendingDownlinks {
    awaiting_remote: HashMap<Text, HashMap<DlKey, Vec<DownlinkRequest>>>,
    awaiting_dl: HashMap<SocketAddr, HashMap<DlKey, Vec<DownlinkRequest>>>,
    local: HashMap<DlKey, Vec<DownlinkRequest>>,
}

impl PendingDownlinks {
    pub fn push_remote(&mut self, remote: Text, request: DownlinkRequest) {
        let PendingDownlinks {
            awaiting_remote: awaiting_socket,
            ..
        } = self;
        let key = key_of(&request.key);
        awaiting_socket
            .entry(remote)
            .or_default()
            .entry(key)
            .or_default()
            .push(request);
    }

    pub fn push_relative(&mut self, remote: SocketAddr, request: DownlinkRequest) {
        let PendingDownlinks { awaiting_dl, .. } = self;
        let key = key_of(&request.key);
        awaiting_dl
            .entry(remote)
            .or_default()
            .entry(key)
            .or_default()
            .push(request);
    }

    pub fn push_local(&mut self, request: DownlinkRequest) {
        let PendingDownlinks { local, .. } = self;
        let key = key_of(&request.key);
        local.entry(key).or_default().push(request);
    }

    pub fn socket_ready<'a>(
        &'a mut self,
        host: Text,
        addr: SocketAddr,
    ) -> Option<impl Iterator<Item = &'a DlKey> + 'a> {
        let PendingDownlinks {
            awaiting_remote,
            awaiting_dl,
            ..
        } = self;
        if let Some(map) = awaiting_remote.remove(&host) {
            Some(match awaiting_dl.entry(addr) {
                Entry::Occupied(entry) => {
                    let existing = entry.into_mut();
                    existing.extend(map.into_iter());
                    existing.keys()
                }
                Entry::Vacant(entry) => entry.insert(map).keys(),
            })
        } else {
            None
        }
    }

    pub fn open_client_failed(&mut self, host: &Text) -> impl Iterator<Item = DownlinkRequest> {
        self.awaiting_remote
            .remove(host)
            .map(|map| map.into_iter().map(|(_, v)| v).into_iter().flatten())
            .into_iter()
            .flatten()
    }

    pub fn dl_ready(&mut self, addr: Option<SocketAddr>, key: &DlKey) -> Vec<DownlinkRequest> {
        let PendingDownlinks {
            awaiting_dl, local, ..
        } = self;
        if let Some(addr) = addr {
            match awaiting_dl.entry(addr) {
                Entry::Occupied(mut entry) => {
                    let results = entry.get_mut().remove(key).unwrap_or_default();
                    if entry.get().is_empty() {
                        entry.remove();
                    }
                    results
                }
                Entry::Vacant(_k) => vec![],
            }
        } else {
            local.remove(key).unwrap_or_default()
        }
    }
}
