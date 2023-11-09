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

use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
};

use swim_api::downlink::DownlinkKind;
use swim_model::{address::RelativeAddress, Text};
use swim_runtime::agent::{CommanderRequest, DownlinkRequest};
use tracing::debug;

pub type DlKey = (RelativeAddress<Text>, DownlinkKind);

#[derive(Default, Debug)]
pub struct Waiting {
    pub dl_requests: HashMap<DlKey, Vec<DownlinkRequest>>,
    pub cmd_requests: Vec<CommanderRequest>,
}

#[derive(Default, Debug)]
pub struct PendingDownlinks {
    awaiting_remote: HashMap<Text, Waiting>,
    awaiting_dl: HashMap<SocketAddr, HashMap<DlKey, Vec<DownlinkRequest>>>,
    local: HashMap<DlKey, Vec<DownlinkRequest>>,
}

impl PendingDownlinks {
    #[must_use]
    pub fn push_remote(&mut self, remote: Text, request: DownlinkRequest) -> bool {
        let PendingDownlinks {
            awaiting_remote, ..
        } = self;
        debug!(remote = %remote, address = %request.address, "Adding pending downlink request.");
        let remote_pending = awaiting_remote.contains_key(&remote);
        let key = (request.address.clone(), request.kind);
        awaiting_remote
            .entry(remote)
            .or_default()
            .dl_requests
            .entry(key)
            .or_default()
            .push(request);
        !remote_pending
    }

    #[must_use]
    pub fn push_remote_cmd(&mut self, remote: Text, request: CommanderRequest) -> bool {
        let PendingDownlinks {
            awaiting_remote, ..
        } = self;
        debug!(remote = %remote, key = ?request.key, "Adding pending commander request.");
        let remote_pending = awaiting_remote.contains_key(&remote);
        awaiting_remote
            .entry(remote)
            .or_default()
            .cmd_requests
            .push(request);
        !remote_pending
    }

    pub fn push_for_socket(
        &mut self,
        remote: SocketAddr,
        key: DlKey,
        requests: Vec<DownlinkRequest>,
    ) {
        debug!(remote = %remote, key = ?key, "Adding {} downlink requests for socket.", requests.len());
        let PendingDownlinks { awaiting_dl, .. } = self;
        awaiting_dl
            .entry(remote)
            .or_default()
            .entry(key)
            .or_default()
            .extend(requests);
    }

    pub fn push_local(&mut self, request: DownlinkRequest) {
        let PendingDownlinks { local, .. } = self;
        let key = (request.address.clone(), request.kind);
        debug!(key = ?key, "Adding a request for a local downlink.");
        local.entry(key).or_default().push(request);
    }

    pub fn take_socket_ready(&mut self, host: &Text) -> Waiting {
        debug!(host = %host, "Removing downlink requests for a remote.");
        let PendingDownlinks {
            awaiting_remote, ..
        } = self;
        awaiting_remote.remove(host).unwrap_or_default()
    }

    pub fn socket_ready(
        &mut self,
        host: &Text,
        addr: SocketAddr,
    ) -> Option<(impl Iterator<Item = &DlKey> + '_, Vec<CommanderRequest>)> {
        debug!(host = %host, addr = %addr, "Socket ready for host.");
        let PendingDownlinks {
            awaiting_remote,
            awaiting_dl,
            ..
        } = self;
        awaiting_remote.remove(host).map(
            |Waiting {
                 dl_requests,
                 cmd_requests,
             }| match awaiting_dl.entry(addr) {
                Entry::Occupied(entry) => {
                    let existing = entry.into_mut();
                    existing.extend(dl_requests);
                    (existing.keys(), cmd_requests)
                }
                Entry::Vacant(entry) => (entry.insert(dl_requests).keys(), cmd_requests),
            },
        )
    }

    pub fn open_client_failed(
        &mut self,
        host: &Text,
    ) -> (impl Iterator<Item = DownlinkRequest>, Vec<CommanderRequest>) {
        debug!(host = %host, "Removing pending downlinks for failed host connection.");
        let (dl_requests, cmd_requests) = if let Some(Waiting {
            dl_requests,
            cmd_requests,
        }) = self.awaiting_remote.remove(host)
        {
            (Some(dl_requests), cmd_requests)
        } else {
            (None, vec![])
        };
        (
            dl_requests
                .map(|map| map.into_values().flatten())
                .into_iter()
                .flatten(),
            cmd_requests,
        )
    }

    pub fn dl_ready(&mut self, addr: Option<SocketAddr>, key: &DlKey) -> Vec<DownlinkRequest> {
        debug!(key = ?key, addr = ?addr, "Taking requests for established downlink connection.");
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
