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

use crate::compat::{RequestMessage, ResponseMessage};
use crate::routing::RoutingAddr;
use swim_model::path::RelativePath;
use swim_warp::envelope::{HeaderParseErr, RawEnvelopeHeader};

pub struct SplitMessage<'b> {
    pub header: RawEnvelopeHeader,
    pub body: &'b str,
}

impl<'b> SplitMessage<'b> {
    pub fn tag(self, tag: RoutingAddr) -> TaggedMessage<'b> {
        let SplitMessage { header, body } = self;

        match header {
            RawEnvelopeHeader::Auth | RawEnvelopeHeader::DeAuth => {
                unimplemented!("Authentication envelopes are not supported yet")
            }
            RawEnvelopeHeader::Link { node, lane, .. } => {
                TaggedMessage::Request(RequestMessage::link(tag, RelativePath::new(node, lane)))
            }
            RawEnvelopeHeader::Sync { node, lane, .. } => {
                TaggedMessage::Request(RequestMessage::sync(tag, RelativePath::new(node, lane)))
            }
            RawEnvelopeHeader::Unlink { node, lane } => {
                TaggedMessage::Request(RequestMessage::unlink(tag, RelativePath::new(node, lane)))
            }
            RawEnvelopeHeader::Command { node, lane } => TaggedMessage::Request(
                RequestMessage::command(tag, RelativePath::new(node, lane), body.as_ref()),
            ),
            RawEnvelopeHeader::Linked { node, lane, .. } => {
                TaggedMessage::Response(ResponseMessage::linked(tag, RelativePath::new(node, lane)))
            }
            RawEnvelopeHeader::Synced { node, lane } => {
                TaggedMessage::Response(ResponseMessage::synced(tag, RelativePath::new(node, lane)))
            }
            RawEnvelopeHeader::Unlinked { node, lane } => TaggedMessage::Response(
                ResponseMessage::unlinked(tag, RelativePath::new(node, lane)),
            ),
            RawEnvelopeHeader::Event { node, lane } => TaggedMessage::Response(
                ResponseMessage::event(tag, RelativePath::new(node, lane), body),
            ),
        }
    }
}

pub enum TaggedMessage<'b> {
    Request(RequestMessage<&'b [u8]>),
    Response(ResponseMessage<&'b str>),
}

impl<'b> TaggedMessage<'b> {
    pub fn path(&self) -> RelativePath {
        match self {
            TaggedMessage::Request(message) => message.path.clone(),
            TaggedMessage::Response(message) => message.path.clone(),
        }
    }

    pub fn is_request(&self) -> bool {
        matches!(self, TaggedMessage::Request(_))
    }
}

pub fn read_raw_header(repr: &str) -> Result<SplitMessage, HeaderParseErr> {
    let (header, body) = RawEnvelopeHeader::parse_from(repr)?;
    Ok(SplitMessage { header, body })
}
