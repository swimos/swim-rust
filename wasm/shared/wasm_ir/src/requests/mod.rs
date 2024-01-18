// Copyright 2015-2023 Swim Inc.
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

use std::mem::size_of;

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

pub use guest::*;
pub use host::*;
use swim_protocol::agent::LaneResponse;

/// Guest -> Host requests
mod guest;
/// Host -> Guest requests
mod host;

#[derive(Debug, Serialize, Deserialize)]
pub struct GuestLaneResponses {
    pub responses: BytesMut,
}

#[derive(Debug)]
pub struct IdentifiedLaneResponse {
    pub id: u64,
    pub response: BytesMut,
}

pub struct IdentifiedLaneResponseEncoder<E> {
    id: u64,
    inner: E,
}

impl<E> IdentifiedLaneResponseEncoder<E> {
    pub fn new(id: u64, inner: E) -> IdentifiedLaneResponseEncoder<E> {
        IdentifiedLaneResponseEncoder { id, inner }
    }
}

impl<E, T> Encoder<LaneResponse<T>> for IdentifiedLaneResponseEncoder<E>
where
    E: Encoder<LaneResponse<T>>,
{
    type Error = E::Error;

    fn encode(&mut self, item: LaneResponse<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let IdentifiedLaneResponseEncoder { id, inner } = self;

        dst.put_u64(*id);
        let remaining = dst.remaining();
        dst.put_u64(0);
        inner.encode(item, dst)?;

        let len = dst.remaining() - remaining - size_of::<u64>();
        let mut rewound = &mut dst.as_mut()[remaining..];
        rewound.put_u64(len as u64);

        Ok(())
    }
}

pub struct IdentifiedLaneResponseDecoder;

impl Decoder for IdentifiedLaneResponseDecoder {
    type Item = IdentifiedLaneResponse;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() < 2 * size_of::<u64>() {
            Ok(None)
        } else {
            let mut source = src.as_ref();

            let id = source.get_u64();
            let len = source.get_u64();

            if src.remaining() >= len as usize {
                src.advance(2 * size_of::<u64>());
                let response = src.split_to(len as usize);
                Ok(Some(IdentifiedLaneResponse { id, response }))
            } else {
                Ok(None)
            }
        }
    }
}
