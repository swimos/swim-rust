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
use futures::SinkExt;
use swim_model::{path::RelativePath, Text};
use swim_utilities::io::byte_channel::ByteWriter;
use tokio_util::codec::FramedWrite;
use uuid::Uuid;

use crate::{
    compat::{Notification, RawResponseMessageEncoder, ResponseMessage},
    routing::RoutingAddr,
};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct RemoteSender {
    sender: FramedWrite<ByteWriter, RawResponseMessageEncoder>,
    identity: RoutingAddr,
    remote_id: Uuid,
    node: Text,
    pub lane: String,
}

impl RemoteSender {
    pub fn new(writer: ByteWriter, identity: RoutingAddr, remote_id: Uuid, node: Text) -> Self {
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

    pub fn update_lane(&mut self, lane_name: &str) {
        let RemoteSender { lane, .. } = self;
        lane.clear();
        lane.push_str(lane_name);
    }

    pub async fn send_notification(
        &mut self,
        notification: Notification<&BytesMut, &[u8]>,
    ) -> Result<(), std::io::Error> {
        let RemoteSender {
            sender,
            identity,
            node,
            lane,
            ..
        } = self;

        let message: ResponseMessage<&BytesMut, &[u8]> = ResponseMessage {
            origin: *identity,
            path: RelativePath::new(node.as_str(), lane.as_str()),
            envelope: notification,
        };
        sender.send(message).await?;
        Ok(())
    }
}
