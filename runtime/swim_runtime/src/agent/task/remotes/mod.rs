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
use swim_utilities::io::byte_channel::ByteWriter;
use tracing::debug;
use uuid::Uuid;

use crate::{error::InvalidKey, routing::RoutingAddr};
pub use sender::RemoteSender;
pub use uplink::UplinkResponse;

mod registry;
mod sender;
mod uplink;

pub use registry::LaneRegistry;

use self::uplink::Uplinks;

use super::write_fut::{SpecialAction, WriteTask};

#[derive(Debug, Default)]
pub struct RemoteTracker {
    registry: LaneRegistry,
    remotes: HashMap<Uuid, Uplinks>,
}

impl RemoteTracker {
    pub fn remove_remote(&mut self, remote_id: Uuid) {
        self.remotes.remove(&remote_id);
    }

    pub fn has_remote(&self, remote_id: Uuid) -> bool {
        self.remotes.contains_key(&remote_id)
    }

    pub fn remove_lane(&mut self, id: u64) -> Option<Text> {
        self.lane_registry().remove(id)
    }

    pub fn lane_registry(&mut self) -> &mut LaneRegistry {
        &mut self.registry
    }

    pub fn insert(
        &mut self,
        remote_id: Uuid,
        node: Text,
        identity: RoutingAddr,
        writer: ByteWriter,
    ) {
        debug!("Registering remote with ID {}.", remote_id);
        let RemoteTracker { remotes, .. } = self;
        remotes.insert(remote_id, Uplinks::new(node, identity, writer));
    }

    #[must_use]
    pub fn push_special(&mut self, response: SpecialAction, target: &Uuid) -> Option<WriteTask> {
        let RemoteTracker {
            registry, remotes, ..
        } = self;
        remotes
            .get_mut(target)
            .and_then(|uplink| uplink.push_special(response, registry))
    }

    #[must_use]
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

    #[must_use]
    pub fn unlink_lane(&mut self, remote_id: Uuid, lane_id: u64) -> Option<WriteTask> {
        let RemoteTracker {
            registry, remotes, ..
        } = self;
        remotes.get_mut(&remote_id).and_then(|uplinks| {
            uplinks.push_special(SpecialAction::unlinked(lane_id, Text::empty()), registry)
        })
    }

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
}
