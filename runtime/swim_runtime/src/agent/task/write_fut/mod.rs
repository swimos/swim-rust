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
use swim_model::Text;

use crate::{
    compat::Notification,
    pressure::{BackpressureStrategy, MapBackpressure},
};

use super::remotes::{LaneRegistry, RemoteSender};

#[cfg(test)]
mod tests;

pub type WriteResult = (RemoteSender, BytesMut, Result<(), std::io::Error>);

const LANE_NOT_FOUND_BODY: &[u8] = b"@laneNotFound";

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
            SpecialAction::Linked(id) => registry.name_for(*id),
            SpecialAction::Unlinked { lane_id, .. } => registry.name_for(*lane_id),
            SpecialAction::LaneNotFound { lane_name } => lane_name.as_str(),
        }
    }
}

#[derive(Debug)]
pub enum WriteAction {
    Event,
    EventAndSynced,
    MapSynced(Option<MapBackpressure>),
    Special(SpecialAction),
}

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
                writer.send_notification(Notification::Synced).await?;
            }
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
