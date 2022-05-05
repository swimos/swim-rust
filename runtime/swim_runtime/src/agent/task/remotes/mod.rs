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

use crate::{
    compat::Notification, error::InvalidKey, pressure::BackpressureStrategy, routing::RoutingAddr,
};
pub use sender::RemoteSender;
pub use uplink::{SpecialUplinkAction, UplinkResponse, WriteAction};

mod registry;
mod sender;
mod uplink;

pub use registry::LaneRegistry;

use self::uplink::Uplinks;

pub type WriteResult = (RemoteSender, BytesMut, Result<(), std::io::Error>);

const LANE_NOT_FOUND_BODY: &[u8] = b"@laneNotFound";

#[derive(Debug)]
pub struct RemoteWriteTracker<F> {
    registry: LaneRegistry,
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
            registry: Default::default(),
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
        let RemoteWriteTracker {
            remotes, write_op, ..
        } = self;
        remotes.insert(
            remote_id,
            Uplinks::new(node, identity, writer, write_op.clone()),
        );
    }

    #[must_use]
    pub fn push_special(&mut self, response: SpecialUplinkAction, target: &Uuid) -> Option<W> {
        let RemoteWriteTracker {
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
    ) -> Result<Option<W>, InvalidKey> {
        let RemoteWriteTracker {
            registry, remotes, ..
        } = self;
        if let Some(uplink) = remotes.get_mut(target) {
            uplink.push(lane_id, response, registry)
        } else {
            Ok(None)
        }
    }

    #[must_use]
    pub fn unlink_lane(&mut self, remote_id: Uuid, lane_id: u64) -> Option<W> {
        let RemoteWriteTracker {
            registry, remotes, ..
        } = self;
        remotes.get_mut(&remote_id).and_then(|uplinks| {
            uplinks.push_special(
                SpecialUplinkAction::unlinked(lane_id, Text::empty()),
                registry,
            )
        })
    }

    #[must_use]
    pub fn replace_and_pop(&mut self, writer: RemoteSender, buffer: BytesMut) -> Option<W> {
        let RemoteWriteTracker {
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

pub async fn perform_write(
    mut writer: RemoteSender,
    mut buffer: BytesMut,
    action: WriteAction,
) -> (RemoteSender, BytesMut, Result<(), std::io::Error>) {
    let result = perform_write_inner(&mut writer, &mut buffer, action).await;
    (writer, buffer, result)
}

async fn perform_write_inner(
    writer: &mut RemoteSender,
    buffer: &mut BytesMut,
    action: WriteAction,
) -> Result<(), std::io::Error> {
    match action {
        WriteAction::Event => {
            writer
                .send_notification(Notification::Event(&*buffer))
                .await?;
        }
        WriteAction::EventAndSynced => {
            writer
                .send_notification(Notification::Event(&*buffer))
                .await?;
            writer.send_notification(Notification::Synced).await?;
        }
        WriteAction::MapSynced(maybe_queue) => {
            if let Some(mut queue) = maybe_queue {
                while queue.has_data() {
                    queue.prepare_write(buffer);
                    writer
                        .send_notification(Notification::Event(&*buffer))
                        .await?;
                }
                writer.send_notification(Notification::Synced).await?;
            }
        }
        WriteAction::Special(SpecialUplinkAction::Linked(_)) => {
            writer.send_notification(Notification::Linked).await?;
        }
        WriteAction::Special(SpecialUplinkAction::Unlinked { message, .. }) => {
            writer
                .send_notification(Notification::Unlinked(Some(message.as_bytes())))
                .await?;
        }
        WriteAction::Special(SpecialUplinkAction::LaneNotFound { .. }) => {
            writer
                .send_notification(Notification::Unlinked(Some(LANE_NOT_FOUND_BODY)))
                .await?;
        }
    }

    Ok(())
}
