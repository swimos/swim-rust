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

const WORD_SIZE: usize = std::mem::size_of::<usize>() * 2;

#[inline]
fn apply_mask_unoptimised(buf: &mut [u8], mask: [u8; 4]) {
    for (i, byte) in buf.iter_mut().enumerate() {
        *byte ^= mask[i & 3];
    }
}

#[inline]
pub fn apply_mask(mask: u32, bytes: &mut [u8]) {
    let mask = mask.to_ne_bytes();
    if bytes.len() < WORD_SIZE {
        apply_mask_unoptimised(bytes, mask);
    } else {
        apply_mask_fast(bytes, mask)
    }
}

#[inline]
pub fn apply_mask_fast(bytes: &mut [u8], mask: [u8; 4]) {
    let mut mask_u32 = u32::from_ne_bytes(mask);
    let (mut prefix, shorts, mut suffix) = unsafe { bytes.align_to_mut::<u32>() };
    let prefix_len = prefix.len();

    if prefix_len > 0 {
        apply_mask_unoptimised(&mut prefix, mask);

        if cfg!(target_endian = "big") {
            mask_u32 = mask_u32.rotate_left(8 * prefix_len as u32)
        } else {
            mask_u32 = mask_u32.rotate_right(8 * prefix_len as u32)
        }
    }

    for word in shorts.iter_mut() {
        *word ^= mask_u32;
    }

    if suffix.len() > 0 {
        apply_mask_unoptimised(&mut suffix, mask_u32.to_ne_bytes());
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::mask::{apply_mask_fast, apply_mask_unoptimised};

    // Tests that the fast masking produces the same results an the unoptimised version against
    // different alignments
    #[test]
    fn apply_mask_unaligned() {
        let mask = [0x1, 0x2, 0x3, 0x3];
        let payload = (0..100u8).collect::<Vec<_>>();

        // So the offset isn't out of bounds
        let start = 3;

        for idx in start..=payload.len() {
            let unmasked = &payload[0..idx];
            for offset in 0..=3 {
                let mut masked_unoptimised = unmasked.to_vec();
                apply_mask_unoptimised(&mut masked_unoptimised[offset..], mask);

                let mut masked_fast = unmasked.to_vec();
                apply_mask_fast(&mut masked_fast[offset..], mask);

                assert_eq!(masked_unoptimised, masked_fast);
            }
        }
    }

    #[test]
    fn apply_mask_aligned() {
        let mask = [0x1, 0x2, 0x3, 0x3];
        let payload = (0..100u8).collect::<Vec<_>>();

        let unmasked = &payload[..];

        let mut masked_unoptimised = unmasked.to_vec();
        apply_mask_unoptimised(&mut masked_unoptimised, mask);

        let mut masked_fast = unmasked.to_vec();
        apply_mask_fast(&mut masked_fast, mask);

        assert_eq!(masked_unoptimised, masked_fast);
    }
}
