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

use bytes::BytesMut;
use futures::SinkExt;
use swimos_api::address::RelativeAddress;
use swimos_messages::protocol::RawResponseMessageEncoder;
use swimos_messages::protocol::{Notification, ResponseMessage};
use swimos_model::Text;
use swimos_utilities::byte_channel::ByteWriter;
use tokio_util::codec::FramedWrite;
use tracing::trace;
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// Sender to write outgoing frames to to remotes connected to the agent.
#[derive(Debug)]
pub struct RemoteSender {
    sender: FramedWrite<ByteWriter, RawResponseMessageEncoder>,
    identity: Uuid,
    remote_id: Uuid,
    node: Text,
    pub lane: String,
}

impl RemoteSender {
    /// # Arguments
    /// * `writer` - The underlying byte channel.
    /// * `identity` - Routing address of the agent.
    /// * `remote_id` - Routing ID of the remote.
    /// * `node` - The node URI of the agent.
    pub fn new(writer: ByteWriter, identity: Uuid, remote_id: Uuid, node: Text) -> Self {
        RemoteSender {
            sender: FramedWrite::new(writer, Default::default()),
            identity,
            remote_id,
            node,
            lane: Default::default(),
        }
    }

    pub fn remote_id(&self) -> Uuid {
        self.remote_id
    }

    /// Set the name of the lane for the next message that is sent. This is done separately from
    /// the actual write to avoid needing to move a copy of the name into the future that performs
    /// the write.
    ///
    /// # Arguments
    /// * `lane_name` - The name of the lane.
    pub fn update_lane(&mut self, lane_name: &str) {
        let RemoteSender { lane, .. } = self;
        lane.clear();
        lane.push_str(lane_name);
    }

    /// Construct a [`ResponseMessage`] for the provided notification and send it on the
    /// channel.
    ///
    /// # Arguments
    /// * `notification` - The content of the frame.
    pub async fn send_notification(
        &mut self,
        notification: Notification<&BytesMut, &[u8]>,
    ) -> Result<(), std::io::Error> {
        let RemoteSender {
            sender,
            identity,
            remote_id,
            node,
            lane,
            ..
        } = self;

        trace!(identity = %identity, remote_id = %remote_id, node = %node, lane = %lane, notification = ?notification.debug_formatter(), "Sending notification.");

        let message: ResponseMessage<&str, &BytesMut, &[u8]> = ResponseMessage {
            origin: *identity,
            path: RelativeAddress::new(node.as_str(), lane.as_str()),
            envelope: notification,
        };
        sender.send(message).await?;
        Ok(())
    }
}
