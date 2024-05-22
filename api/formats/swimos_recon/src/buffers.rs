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
use std::fmt::Write;
use swimos_form::structural::write::StructuralWritable;

use crate::print_recon_compact;

const RESERVE_INIT: usize = 256;
const RESERVE_MULT: usize = 2;

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

pub fn write_recon<T: StructuralWritable>(dst: &mut BytesMut, body: &T) -> usize {
    let body_offset = write_recon_body(dst, body);
    dst.remaining() - body_offset
}
