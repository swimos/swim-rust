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

use std::collections::HashMap;

use bytes::BytesMut;
use swim_model::Text;
use swim_utilities::{io::byte_channel::ByteWriter, trigger::promise};
use tracing::debug;
use uuid::Uuid;

use crate::{agent::DisconnectionReason, error::InvalidKey};
pub use sender::RemoteSender;
pub use uplink::UplinkResponse;

mod registry;
mod sender;
mod uplink;

pub use registry::LaneRegistry;

use self::uplink::Uplinks;

use super::write_fut::{SpecialAction, WriteTask};

#[cfg(test)]
mod tests;

/// Tracks the set of uplinks for each remote attached to the write task of the agent runtime.
#[derive(Debug)]
pub struct RemoteTracker {
    node: Text,
    identity: Uuid,
    registry: LaneRegistry,
    remotes: HashMap<Uuid, Uplinks>,
}

impl RemoteTracker {
    /// #Arguments
    /// * `identity` - The routing address of the agent to be included in outgoing messages.
    /// * `node` - The node URI of the agent to be included in outgoing messages.
    pub fn new(identity: Uuid, node: Text) -> Self {
        RemoteTracker {
            node,
            identity,
            registry: Default::default(),
            remotes: Default::default(),
        }
    }

    /// Remove a remote, giving the specified reason.
    pub fn remove_remote(&mut self, remote_id: Uuid, reason: DisconnectionReason) {
        if let Some(existing) = self.remotes.remove(&remote_id) {
            existing.complete(reason);
        }
    }

    /// Determine if a specified remote is attached.
    pub fn has_remote(&self, remote_id: Uuid) -> bool {
        self.remotes.contains_key(&remote_id)
    }

    /// Remove a lane from the registry.
    pub fn remove_lane(&mut self, id: u64) -> Option<Text> {
        self.lane_registry().remove(id)
    }

    /// Get a reference to the lane registry.
    pub fn lane_registry(&mut self) -> &mut LaneRegistry {
        &mut self.registry
    }

    /// Insert a new remote.
    /// #Arguments
    /// * `remote_id` - The routing ID of the remote.
    /// * `writer` - The channel for the remote.
    /// * `completion` - Promise that must be satisfied if the remote is closed.
    pub fn insert(
        &mut self,
        remote_id: Uuid,
        writer: ByteWriter,
        completion: promise::Sender<DisconnectionReason>,
    ) {
        debug!("Registering remote with ID {}.", remote_id);
        let RemoteTracker {
            identity,
            node,
            remotes,
            ..
        } = self;
        if let Some(existing) = remotes.insert(
            remote_id,
            Uplinks::new(node.clone(), *identity, remote_id, writer, completion),
        ) {
            existing.complete(DisconnectionReason::DuplicateRegistration(remote_id));
        }
    }

    /// Push a special action into the queue for the specified remote.
    #[must_use]
    pub fn push_special(&mut self, response: SpecialAction, target: &Uuid) -> Option<WriteTask> {
        let RemoteTracker {
            registry, remotes, ..
        } = self;
        remotes
            .get_mut(target)
            .and_then(|uplink| uplink.push_special(response, registry))
    }

    /// Push an event for a lane into the queue for the specified remote.
    pub fn push_write(
        &mut self,
        lane_id: u64,
        response: UplinkResponse,
        target: &Uuid,
    ) -> Result<Option<WriteTask>, InvalidKey> {
        let RemoteTracker {
            registry, remotes, ..
        } = self;
        if let Some(uplink) = remotes.get_mut(target) {
            uplink.push(lane_id, response, registry)
        } else {
            Ok(None)
        }
    }

    /// Unlink a lane from the specified remote.
    #[must_use]
    pub fn unlink_lane(&mut self, remote_id: Uuid, lane_id: u64) -> Option<WriteTask> {
        let RemoteTracker {
            registry, remotes, ..
        } = self;
        remotes.get_mut(&remote_id).and_then(|uplinks| {
            uplinks.push_special(SpecialAction::unlinked(lane_id, Text::empty()), registry)
        })
    }

    /// Return a sender and associated buffer, from a completed write, to the appropriate remote.
    #[must_use]
    pub fn replace_and_pop(&mut self, writer: RemoteSender, buffer: BytesMut) -> Option<WriteTask> {
        let RemoteTracker {
            registry, remotes, ..
        } = self;
        let id = writer.remote_id();
        remotes
            .get_mut(&id)
            .and_then(|uplinks| uplinks.replace_and_pop(writer, buffer, registry))
    }

    pub fn is_empty(&self) -> bool {
        self.remotes.is_empty()
    }

    /// Close all remote with the specified reason.
    pub fn dispose_of_remotes(self, reason: DisconnectionReason) {
        let RemoteTracker { remotes, .. } = self;
        remotes
            .into_iter()
            .for_each(|(_, uplinks)| uplinks.complete(reason));
    }
}
