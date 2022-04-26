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

use std::{collections::HashMap, str::Utf8Error};

use bytes::BytesMut;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use swim_model::Text;
use swim_utilities::io::byte_channel::ByteWriter;
use uuid::Uuid;

use crate::routing::RoutingAddr;

use super::uplink::{
    RemoteSender, SpecialUplinkAction, UplinkResponse, Uplinks, WriteAction, WriteResult,
};

pub struct RemoteWriteTracker<W, F> {
    remotes: HashMap<Uuid, Uplinks<F>>,
    pending_writes: FuturesUnordered<W>,
    write_op: F,
}

impl<W, F> RemoteWriteTracker<W, F>
where
    F: Fn(RemoteSender, BytesMut, WriteAction) -> W + Clone,
    W: Future<Output = WriteResult> + Send + 'static,
{
    pub fn new(write_op: F) -> Self {
        Self {
            remotes: Default::default(),
            pending_writes: Default::default(),
            write_op,
        }
    }

    pub async fn next(&mut self) -> Option<WriteResult> {
        self.pending_writes.next().await
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
        let RemoteWriteTracker {
            remotes, write_op, ..
        } = self;
        remotes.insert(
            remote_id,
            Uplinks::new(node, identity, writer, write_op.clone()),
        );
    }

    pub fn push_write(
        &mut self,
        lane_id: u64,
        lane_name: &str,
        response: UplinkResponse,
        target: &Uuid,
    ) {
        let RemoteWriteTracker {
            remotes,
            pending_writes,
            ..
        } = self;
        if let Some(uplink) = remotes.get_mut(target) {
            match uplink.push(lane_id, response, lane_name) {
                Ok(Some(write)) => {
                    pending_writes.push(write);
                }
                Err(_) => {
                    //TODO Log error.
                }
                _ => {}
            }
        }
    }

    pub fn has_pending(&self) -> bool {
        self.pending_writes.is_empty()
    }

    pub fn unlink_lane(
        &mut self,
        remote_id: Uuid,
        lane_id: u64,
        lane_name: &str,
    ) -> Result<(), Utf8Error> {
        let RemoteWriteTracker { pending_writes, .. } = self;
        if let Some(uplinks) = self.remotes.get_mut(&remote_id) {
            if let Some(write) = uplinks.push(
                lane_id,
                UplinkResponse::Special(SpecialUplinkAction::Unlinked(lane_id, Text::empty())),
                lane_name,
            )? {
                pending_writes.push(write);
            }
        }
        Ok(())
    }

    pub fn replace_and_pop(&mut self, writer: RemoteSender, buffer: BytesMut) {
        let RemoteWriteTracker {
            remotes,
            pending_writes,
            ..
        } = self;
        let id = writer.remote_id();
        if let Some(write) = remotes
            .get_mut(&id)
            .and_then(|uplinks| uplinks.replace_and_pop(writer, buffer))
        {
            pending_writes.push(write);
        }
    }
}
