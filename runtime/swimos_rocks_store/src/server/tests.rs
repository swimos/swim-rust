// Copyright 2015-2024 Swim Inc.
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

use crate::server::{StoreKey, KEY, MAP_TAG, UBOUND, VAL_TAG};
use integer_encoding::FixedInt;

#[test]
fn serialize_value_key() {
    let lane_id = 84837272;
    let key = StoreKey::Value { lane_id };

    let bytes = key.serialize_as_bytes();
    assert_eq!(bytes.len(), 9);
    assert_eq!(bytes[0], VAL_TAG);

    assert_eq!(u64::decode_fixed(&bytes[1..]), Some(lane_id))
}

#[test]
fn serialize_map_key() {
    let lane_id = 84837272;
    let key_bytes: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8];
    let key = StoreKey::Map {
        lane_id,
        key: Some(key_bytes.to_owned()),
    };

    let bytes = key.serialize_as_bytes();
    assert_eq!(bytes.len(), 26);
    assert_eq!(bytes[0], MAP_TAG);

    assert_eq!(u64::decode_fixed(&bytes[1..9]), Some(lane_id));
    assert_eq!(bytes[9], KEY);
    assert_eq!(u64::decode_fixed(&bytes[10..18]), Some(8));

    assert_eq!(&bytes[18..], key_bytes);
}

#[test]
fn serialize_map_lbound() {
    let lane_id = 84837272;
    let key = StoreKey::Map { lane_id, key: None };

    let bytes = key.serialize_as_bytes();
    assert_eq!(bytes.len(), 9);
    assert_eq!(bytes[0], MAP_TAG);

    assert_eq!(u64::decode_fixed(&bytes[1..9]), Some(lane_id));
}

#[test]
fn serialize_map_ubound() {
    let lane_id = 84837272;
    let bytes = StoreKey::map_ubound_bytes(lane_id);

    assert_eq!(bytes.len(), 10);
    assert_eq!(bytes[0], MAP_TAG);

    assert_eq!(u64::decode_fixed(&bytes[1..9]), Some(lane_id));
    assert_eq!(bytes[9], UBOUND);
}
