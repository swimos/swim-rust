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

mod protocol;

use bytes::{Buf, BytesMut};
pub use protocol::{agent, downlink, map};
use tokio_util::codec::Decoder;

const TAG_SIZE: usize = std::mem::size_of::<u8>();
const LEN_SIZE: usize = std::mem::size_of::<u64>();

type DecoderResult<D> = Result<Option<<D as Decoder>::Item>, <D as Decoder>::Error>;

fn consume_bounded<D: Decoder>(
    remaining: &mut usize,
    src: &mut BytesMut,
    decoder: &mut D,
) -> (usize, BytesMut, DecoderResult<D>) {
    let to_split = (*remaining).min(src.remaining());
    let rem = src.split_off(to_split);
    let buf_remaining = src.remaining();
    let end_of_message = *remaining <= buf_remaining;
    let decode_result = if end_of_message {
        decoder.decode_eof(src)
    } else {
        decoder.decode(src)
    };
    let new_remaining = src.remaining();
    let consumed = buf_remaining - new_remaining;
    *remaining -= consumed;
    (new_remaining, rem, decode_result)
}
