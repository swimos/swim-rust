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
use futures::Future;
use swim_model::Text;
use swim_utilities::io::byte_channel::ByteWriter;
use tracing::debug;
use uuid::Uuid;

use crate::routing::RoutingAddr;

use super::uplink::{
    RemoteSender, SpecialUplinkAction, UplinkResponse, Uplinks, WriteAction, WriteResult,
};

#[derive(Debug)]
pub struct RemoteWriteTracker<F> {
    remotes: HashMap<Uuid, Uplinks<F>>,
    write_op: F,
}

impl<W, F> RemoteWriteTracker<F>
where
    F: Fn(RemoteSender, BytesMut, WriteAction) -> W + Clone,
    W: Future<Output = WriteResult> + Send + 'static,
{
    pub fn new(write_op: F) -> Self {
        Self {
            remotes: Default::default(),
            write_op,
        }
    }

    pub fn remove_remote(&mut self, remote_id: Uuid) {
        self.remotes.remove(&remote_id);
    }

    pub fn has_remote(&self, remote_id: Uuid) -> bool {
        self.remotes.contains_key(&remote_id)
    }
    pub fn insert(
        &mut self,
        remote_id: Uuid,
        node: Text,
        identity: RoutingAddr,
        writer: ByteWriter,
    ) {
        debug!("Registering remote with ID {}.", remote_id);
        let RemoteWriteTracker {
            remotes, write_op, ..
        } = self;
        remotes.insert(
            remote_id,
            Uplinks::new(node, identity, writer, write_op.clone()),
        );
    }

    #[must_use]
    pub fn push_special(
        &mut self,
        lane_names: &HashMap<u64, Text>,
        response: SpecialUplinkAction,
        target: &Uuid,
    ) -> Option<W> {
        let RemoteWriteTracker { remotes, .. } = self;
        remotes
            .get_mut(target)
            .and_then(|uplink| uplink.push_special(response, lane_names))
    }

    #[must_use]
    pub fn push_write(
        &mut self,
        lane_id: u64,
        lane_names: &HashMap<u64, Text>,
        response: UplinkResponse,
        target: &Uuid,
    ) -> Option<W> {
        let RemoteWriteTracker { remotes, .. } = self;
        if let Some(uplink) = remotes.get_mut(target) {
            match uplink.push(lane_id, response, lane_names) {
                Ok(Some(write)) => Some(write),
                Err(_) => {
                    //TODO Log error.
                    None
                }
                _ => None,
            }
        } else {
            None
        }
    }

    #[must_use]
    pub fn unlink_lane(
        &mut self,
        remote_id: Uuid,
        lane_id: u64,
        lane_names: &HashMap<u64, Text>,
    ) -> Option<W> {
        let RemoteWriteTracker { remotes, .. } = self;
        remotes.get_mut(&remote_id).and_then(|uplinks| {
            uplinks.push_special(
                SpecialUplinkAction::unlinked(lane_id, Text::empty()),
                lane_names,
            )
        })
    }

    #[must_use]
    pub fn replace_and_pop(
        &mut self,
        writer: RemoteSender,
        buffer: BytesMut,
        lane_names: &HashMap<u64, Text>,
    ) -> Option<W> {
        let RemoteWriteTracker { remotes, .. } = self;
        let id = writer.remote_id();
        remotes
            .get_mut(&id)
            .and_then(|uplinks| uplinks.replace_and_pop(writer, buffer, lane_names))
    }

    pub fn is_empty(&self) -> bool {
        self.remotes.is_empty()
    }
}
