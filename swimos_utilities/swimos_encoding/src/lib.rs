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

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// Codec that will encode a type as a UTF-8 string, writing the length (as a 64 bit unsigned
/// integer) as a header.
#[derive(Debug, Clone, Copy, Default)]
pub struct WithLengthBytesCodec;

const LEN_SIZE: usize = std::mem::size_of::<u64>();

impl<B: AsRef<[u8]>> Encoder<B> for WithLengthBytesCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: B, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = item.as_ref();
        dst.reserve(LEN_SIZE + bytes.len());
        dst.put_u64(bytes.len() as u64);
        dst.put(bytes);
        Ok(())
    }
}

impl Decoder for WithLengthBytesCodec {
    type Item = BytesMut;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() < LEN_SIZE {
            Ok(None)
        } else {
            let mut bytes = src.as_ref();
            let len = bytes.get_u64() as usize;
            if src.remaining() >= LEN_SIZE + len {
                src.advance(LEN_SIZE);
                Ok(Some(src.split_to(len)))
            } else {
                Ok(None)
            }
        }
    }
}

pub type DecoderResult<D> = Result<Option<<D as Decoder>::Item>, <D as Decoder>::Error>;

pub fn consume_bounded<D: Decoder>(
    remaining: usize,
    src: &mut BytesMut,
    decoder: &mut D,
) -> (usize, DecoderResult<D>) {
    let to_split = remaining.min(src.remaining());
    let rem = src.split_off(to_split);
    let buf_remaining = src.remaining();
    let end_of_message = remaining <= buf_remaining;
    let decode_result = if end_of_message {
        decoder.decode_eof(src)
    } else {
        decoder.decode(src)
    };
    let consumed = buf_remaining - src.remaining();
    if remaining == consumed {
        *src = rem;
    } else {
        src.unsplit(rem);
    }
    (consumed, decode_result)
}
