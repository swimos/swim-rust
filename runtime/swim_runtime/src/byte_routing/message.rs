// Copyright 2015-2021 SWIM.AI inc.
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

use crate::compat::{Notification, Operation, RequestMessage, ResponseMessage};
use swim_model::path::RelativePath;
use swim_warp::envelope::{HeaderParseErr, RawEnvelopeHeader};

pub enum Message<'b> {
    Request(RequestMessage<&'b [u8]>),
    Response(ResponseMessage<&'b str>),
}

impl<'b> Message<'b> {
    fn from_parts(header: RawEnvelopeHeader, body: &str) -> Message {
        match header {
            RawEnvelopeHeader::Auth | RawEnvelopeHeader::DeAuth => {
                unimplemented!("Authentication envelopes are not supported yet")
            }
            RawEnvelopeHeader::Link { node, lane, .. } => Message::Request(RequestMessage {
                path: RelativePath::new(node, lane),
                envelope: Operation::Link,
            }),
            RawEnvelopeHeader::Sync { node, lane, .. } => Message::Request(RequestMessage {
                path: RelativePath::new(node, lane),
                envelope: Operation::Sync,
            }),
            RawEnvelopeHeader::Unlink { node, lane } => Message::Request(RequestMessage {
                path: RelativePath::new(node, lane),
                envelope: Operation::Unlink,
            }),
            RawEnvelopeHeader::Command { node, lane } => Message::Request(RequestMessage {
                path: RelativePath::new(node, lane),
                envelope: Operation::Command(body.as_ref()),
            }),
            RawEnvelopeHeader::Linked { node, lane, .. } => Message::Response(ResponseMessage {
                path: RelativePath::new(node, lane),
                envelope: Notification::Linked,
            }),
            RawEnvelopeHeader::Synced { node, lane } => Message::Response(ResponseMessage {
                path: RelativePath::new(node, lane),
                envelope: Notification::Synced,
            }),
            RawEnvelopeHeader::Unlinked { node, lane } => Message::Response(ResponseMessage {
                path: RelativePath::new(node, lane),
                envelope: Notification::Unlinked,
            }),
            RawEnvelopeHeader::Event { node, lane } => Message::Response(ResponseMessage {
                path: RelativePath::new(node, lane),
                envelope: Notification::Event(body),
            }),
        }
    }

    pub fn path(&self) -> RelativePath {
        match self {
            Message::Request(message) => message.path.clone(),
            Message::Response(message) => message.path.clone(),
        }
    }

    pub fn is_request(&self) -> bool {
        matches!(self, Message::Request(_))
    }
}

pub fn read_raw_header(repr: &str) -> Result<Message, HeaderParseErr> {
    let (header, body) = RawEnvelopeHeader::parse_from(repr)?;
    Ok(Message::from_parts(header, body))
}
