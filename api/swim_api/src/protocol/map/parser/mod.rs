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

use std::num::ParseIntError;

use bytes::Bytes;
use swim_recon::parser::{try_extract_header, HeaderPeeler, MessageExtractError, Span};

use super::{MapMessage, MapOperation};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy)]
enum MessageKind {
    Update,
    Remove,
    Clear,
    Take,
    Drop,
}

/// Description of a sub-string of the input.
#[derive(Debug, Clone, Copy)]
struct Chunk {
    offset: usize,
    len: usize,
}

impl<'a> From<Span<'a>> for Chunk {
    fn from(span: Span<'a>) -> Self {
        Chunk {
            offset: span.location_offset(),
            len: span.len(),
        }
    }
}

/// Implementaton of [`HeaderPeeler`] that recognizes the header attribute of a warp map message.
/// Rather than parsing the header, this recognizer simply keeps track of the kind of the message
/// and where any key or value is located within the message so these can be extracted as
/// substrings by the caller.
#[derive(Debug, Clone, Copy, Default)]
struct MapMessagePeeler {
    kind: Option<MessageKind>,
    key: Option<Chunk>,
    num: Option<u64>,
}

#[derive(Clone)]
enum MessagePeelError {
    InvalidTag,
    UnknownSlot,
    UnexpectedValue,
    DuplicateKey,
    DuplicateSize,
    BadSize,
    Incomplete,
}

impl From<ParseIntError> for MessagePeelError {
    fn from(_: ParseIntError) -> Self {
        MessagePeelError::BadSize
    }
}

impl<'a> HeaderPeeler<'a> for MapMessagePeeler {
    type Output = MapMessage<Chunk, usize>;

    type Error = MessagePeelError;

    fn tag(mut self, name: &str) -> Result<Self, Self::Error> {
        let MapMessagePeeler { kind, .. } = &mut self;
        *kind = Some(match name {
            "update" => MessageKind::Update,
            "remove" => MessageKind::Remove,
            "clear" => MessageKind::Clear,
            "take" => MessageKind::Take,
            "drop" => MessageKind::Drop,
            _ => {
                return Err(MessagePeelError::InvalidTag);
            }
        });
        Ok(self)
    }

    fn feed_header_slot(mut self, name: &str, value: Span<'a>) -> Result<Self, Self::Error> {
        let MapMessagePeeler { kind, key, .. } = &mut self;
        if name == "key" && matches!(kind, Some(MessageKind::Update | MessageKind::Remove)) {
            if key.is_some() {
                Err(MessagePeelError::DuplicateKey)
            } else {
                *key = Some(value.into());
                Ok(self)
            }
        } else {
            Err(MessagePeelError::UnknownSlot)
        }
    }

    fn feed_header_value(mut self, value: Span<'a>) -> Result<Self, Self::Error> {
        let MapMessagePeeler { kind, num, .. } = &mut self;
        if matches!(kind, Some(MessageKind::Take | MessageKind::Drop)) {
            let n = value.parse::<u64>()?;
            if num.is_some() {
                Err(MessagePeelError::DuplicateSize)
            } else {
                *num = Some(n);
                Ok(self)
            }
        } else {
            Err(MessagePeelError::UnexpectedValue)
        }
    }

    fn feed_header_extant(self) -> Result<Self, Self::Error> {
        Ok(self)
    }

    fn done(self, body: Span<'a>) -> Result<Self::Output, Self::Error> {
        match self {
            MapMessagePeeler {
                kind: Some(MessageKind::Update),
                key: Some(k),
                ..
            } => Ok(MapOperation::Update {
                key: k,
                value: body.location_offset(),
            }
            .into()),
            MapMessagePeeler {
                kind: Some(MessageKind::Remove),
                key: Some(k),
                ..
            } => Ok(MapOperation::Remove { key: k }.into()),
            MapMessagePeeler {
                kind: Some(MessageKind::Clear),
                ..
            } => Ok(MapOperation::Clear.into()),
            MapMessagePeeler {
                kind: Some(MessageKind::Take),
                num: Some(n),
                ..
            } => Ok(MapMessage::Take(n)),
            MapMessagePeeler {
                kind: Some(MessageKind::Drop),
                num: Some(n),
                ..
            } => Ok(MapMessage::Drop(n)),
            _ => Err(MessagePeelError::Incomplete),
        }
    }
}

fn make_raw(bytes: &Bytes, peeled: MapMessage<Chunk, usize>) -> MapMessage<Bytes, Bytes> {
    match peeled {
        MapMessage::Update { key, value } => {
            let Chunk { offset, len } = key;
            let key_bytes = bytes.slice(offset..(offset + len));
            let value_bytes = bytes.slice(value..);
            MapMessage::Update {
                key: key_bytes,
                value: value_bytes,
            }
        }
        MapMessage::Remove { key } => {
            let Chunk { offset, len } = key;
            let key_bytes = bytes.slice(offset..(offset + len));
            MapMessage::Remove { key: key_bytes }
        }
        MapMessage::Clear => MapMessage::Clear,
        MapMessage::Take(n) => MapMessage::Take(n),
        MapMessage::Drop(n) => MapMessage::Drop(n),
    }
}

/// Attempt to interpet the header tag of a warp map message.
pub fn extract_header(bytes: &Bytes) -> Result<MapMessage<Bytes, Bytes>, MessageExtractError> {
    let offsets = try_extract_header(bytes, MapMessagePeeler::default())?;
    Ok(make_raw(bytes, offsets))
}
