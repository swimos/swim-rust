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

use bytes::BytesMut;
use swim_messages::protocol::Notification;
use swim_model::Text;

use crate::{
    pressure::{BackpressureStrategy, MapBackpressure},
};

use super::remotes::{LaneRegistry, RemoteSender};

#[cfg(test)]
mod tests;

/// The result type of the write future (the sender that performed the write, the associated
/// buffer and the result of the operation).
pub type WriteResult = (RemoteSender, BytesMut, Result<(), std::io::Error>);

const LANE_NOT_FOUND_BODY: &[u8] = b"@laneNotFound";

/// Special actions that take priority over general lane events.
#[derive(Debug, Clone)]
pub enum SpecialAction {
    Linked(u64),
    Unlinked { lane_id: u64, message: Text },
    LaneNotFound { lane_name: Text },
}

impl SpecialAction {
    pub fn unlinked(lane_id: u64, message: Text) -> Self {
        SpecialAction::Unlinked { lane_id, message }
    }

    pub fn lane_not_found(lane_name: Text) -> Self {
        SpecialAction::LaneNotFound { lane_name }
    }

    pub fn lane_name<'a>(&'a self, registry: &'a LaneRegistry) -> &'a str {
        match self {
            SpecialAction::Linked(id) => registry.name_for(*id).unwrap_or_default(),
            SpecialAction::Unlinked { lane_id, .. } => {
                registry.name_for(*lane_id).unwrap_or_default()
            }
            SpecialAction::LaneNotFound { lane_name } => lane_name.as_str(),
        }
    }
}

/// Types of writes that can be performed by a [`WriteTask`].
#[derive(Debug)]
pub enum WriteAction {
    // A lane event (the body is stored in the associated buffer).
    Event,
    // A value lane event, to be followed by a synced message, (the body is stored in the associated buffer).
    EventAndSynced,
    // A queue of map lan events, to be followed by a synced message (the contents of the buffer are irrelevant).
    MapSynced(Option<Box<MapBackpressure>>),
    // A special action (the body will be stored in the associated buffer, where appropriate).
    Special(SpecialAction),
}

/// A task that will write one more messages to a remote attached to an agent.
#[derive(Debug)]
pub struct WriteTask {
    pub sender: RemoteSender,
    pub buffer: BytesMut,
    pub action: WriteAction,
}

impl WriteTask {
    pub fn new(sender: RemoteSender, buffer: BytesMut, action: WriteAction) -> Self {
        WriteTask {
            sender,
            buffer,
            action,
        }
    }

    /// Create a future that performs the write.
    pub async fn into_future(self) -> WriteResult {
        let WriteTask {
            mut sender,
            mut buffer,
            action,
        } = self;
        let result = perform_write(&mut sender, &mut buffer, action).await;
        (sender, buffer, result)
    }
}

async fn perform_write(
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
            }
            writer.send_notification(Notification::Synced).await?;
        }
        WriteAction::Special(SpecialAction::Linked(_)) => {
            writer.send_notification(Notification::Linked).await?;
        }
        WriteAction::Special(SpecialAction::Unlinked { message, .. }) => {
            writer
                .send_notification(Notification::Unlinked(Some(message.as_bytes())))
                .await?;
        }
        WriteAction::Special(SpecialAction::LaneNotFound { .. }) => {
            writer
                .send_notification(Notification::Unlinked(Some(LANE_NOT_FOUND_BODY)))
                .await?;
        }
    }

    Ok(())
}
