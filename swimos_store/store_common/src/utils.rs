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

use crate::StoreError;
use integer_encoding::VarInt;

pub const MAX_ID_SIZE: usize = 10;

pub fn serialize_u64(n: u64, target: &mut [u8]) -> &[u8] {
    let len = n.encode_var(target);
    &target[..len]
}

pub fn serialize_u64_vec(n: u64) -> Vec<u8> {
    n.encode_var_vec()
}

pub fn deserialize_u64<B: AsRef<[u8]>>(bytes: B) -> Result<u64, StoreError> {
    let slice = bytes.as_ref();
    match u64::decode_var(slice) {
        Some((n, num_bytes)) if num_bytes == slice.len() => Ok(n),
        _ => Err(StoreError::InvalidKey),
    }
}

#[cfg(test)]
mod tests {
    use crate::{deserialize_u64, serialize_u64, MAX_ID_SIZE};

    fn round_trip(n: u64) {
        let mut buf = [0u8; MAX_ID_SIZE];
        let slice = serialize_u64(n, &mut buf);
        let restored = deserialize_u64(slice).unwrap();
        assert_eq!(n, restored);
    }

    #[test]
    fn serialization_round_trips() {
        round_trip(0);
        round_trip(1);
        round_trip(10);
        round_trip(u8::MAX as u64);
        round_trip(u8::MAX as u64 - 1);
        round_trip(u16::MAX as u64);
        round_trip(u16::MAX as u64 - 1);
        round_trip(u32::MAX as u64);
        round_trip(u32::MAX as u64 - 1);
        round_trip(u64::MAX);
        round_trip(u64::MAX - 1);
    }
}
