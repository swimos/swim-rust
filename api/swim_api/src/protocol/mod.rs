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

use bytes::{Buf, BufMut, BytesMut};
use std::fmt::Write;
use swim_form::structural::write::StructuralWritable;
use swim_recon::printer::print_recon_compact;
use tokio_util::codec::Decoder;

pub mod agent;
pub mod downlink;
pub mod map;

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

const RESERVE_INIT: usize = 256;
const RESERVE_MULT: usize = 2;
const TAG_SIZE: usize = std::mem::size_of::<u8>();
const LEN_SIZE: usize = std::mem::size_of::<u64>();

fn write_recon_body<T: StructuralWritable>(dst: &mut BytesMut, body: &T) -> usize {
    let mut next_res = RESERVE_INIT.max(dst.remaining_mut().saturating_mul(RESERVE_MULT));
    let body_offset = dst.remaining();
    loop {
        if write!(dst, "{}", print_recon_compact(body)).is_err() {
            dst.truncate(body_offset);
            dst.reserve(next_res);
            next_res = next_res.saturating_mul(RESERVE_MULT);
        } else {
            break;
        }
    }
    body_offset
}

fn write_recon<T: StructuralWritable>(dst: &mut BytesMut, body: &T) {
    dst.reserve(LEN_SIZE);
    let body_len_offset = dst.remaining();
    dst.put_u64(0);
    let body_offset = write_recon_body(dst, body);
    let body_len = (dst.remaining() - body_offset) as u64;
    let mut rewound = &mut dst.as_mut()[body_len_offset..];
    rewound.put_u64(body_len);
}

fn write_recon_kv<K: StructuralWritable, V: StructuralWritable>(
    dst: &mut BytesMut,
    key: &K,
    value: &V,
) {
    dst.reserve(2 * LEN_SIZE);
    let header_offset = dst.remaining();
    dst.put_u64(0);
    dst.put_u64(0);
    let key_offset = write_recon_body(dst, key);
    let key_len = (dst.remaining() - key_offset) as u64;
    let value_offset = write_recon_body(dst, value);
    let value_len = (dst.remaining() - value_offset) as u64;
    let mut rewound = &mut dst.as_mut()[header_offset..];
    rewound.put_u64(key_len);
    rewound.put_u64(value_len);
}
