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

use crate::engine::{RocksEngine, RocksOpts};
use crate::keyspaces::{Keyspace, KeyspaceByteEngine, KeyspaceDef, Keyspaces};
use crate::store::StoreBuilder;
use crate::utils::{deserialize_u64, serialize_u64_vec};
use rocksdb::{MergeOperands, Options, SliceTransform};
use std::mem::size_of;
use std::ops::Deref;
use tempdir::TempDir;

impl Deref for TransientDatabase {
    type Target = RocksEngine;

    fn deref(&self) -> &Self::Target {
        &self.delegate
    }
}

pub struct TransientDatabase {
    _dir: TempDir,
    delegate: RocksEngine,
}

impl TransientDatabase {
    fn new(keyspaces: Keyspaces<RocksOpts>) -> TransientDatabase {
        let dir = TempDir::new("test").expect("Failed to create temporary directory");
        let delegate = RocksOpts::default()
            .build(dir.path(), &keyspaces)
            .expect("Failed to build delegate store");

        TransientDatabase {
            _dir: dir,
            delegate,
        }
    }
}

fn default_lane_opts() -> Options {
    let mut opts = rocksdb::Options::default();
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<u64>()));
    opts.set_memtable_prefix_bloom_ratio(0.2);

    opts
}

pub(crate) fn incrementing_merge_operator(
    _new_key: &[u8],
    existing_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut value = match existing_value {
        Some(bytes) => deserialize_u64(bytes).unwrap(),
        None => 0,
    };

    for op in operands.iter() {
        let deserialized = deserialize_u64(op).unwrap();
        value += deserialized;
    }
    Some(serialize_u64_vec(value))
}

fn default_db() -> TransientDatabase {
    let mut lane_opts = rocksdb::Options::default();
    lane_opts.set_merge_operator_associative("lane_id_counter", incrementing_merge_operator);

    let keyspaces = vec![
        KeyspaceDef::new(KeyspaceName::Value.name(), RocksOpts(default_lane_opts())),
        KeyspaceDef::new(KeyspaceName::Map.name(), RocksOpts(default_lane_opts())),
        KeyspaceDef::new(KeyspaceName::Lane.name(), RocksOpts(lane_opts)),
    ];

    TransientDatabase::new(Keyspaces::new(keyspaces))
}

#[derive(Debug, Clone, Copy)]
enum KeyspaceName {
    Value,
    Map,
    Lane,
}

impl Keyspace for KeyspaceName {
    fn name(&self) -> &str {
        match self {
            KeyspaceName::Value => "value",
            KeyspaceName::Map => "map",
            KeyspaceName::Lane => "default",
        }
    }
}

#[test]
fn get_keyspace() {
    let db = default_db();

    let key = b"test_key";
    let value = b"test_value";

    assert!(db.put_keyspace(KeyspaceName::Value, key, value).is_ok());
}

#[test]
pub fn crud() {
    let db = default_db();

    let key = b"key_a";
    let value_1 = b"value_a";
    let value_2 = b"value_b";

    assert!(db.put_keyspace(KeyspaceName::Value, key, value_1).is_ok());

    let get_result = db.get_keyspace(KeyspaceName::Value, key);
    assert!(matches!(get_result, Ok(Some(_))));
    let get_value = get_result.unwrap().unwrap();
    assert_eq!(value_1, String::from_utf8(get_value).unwrap().as_bytes());

    let update_result = db.put_keyspace(KeyspaceName::Value, key, value_2);
    assert!(update_result.is_ok());

    let get_result = db.get_keyspace(KeyspaceName::Value, key);
    assert!(matches!(get_result, Ok(Some(_))));
    let get_value = get_result.unwrap().unwrap();
    assert_eq!(value_2, String::from_utf8(get_value).unwrap().as_bytes());

    let delete_result = db.delete_keyspace(KeyspaceName::Value, key);
    assert!(matches!(delete_result, Ok(())));
}

#[test]
pub fn get_missing() {
    let db = default_db();
    let get_result = db.get_keyspace(KeyspaceName::Value, b"key_a");
    assert!(matches!(get_result, Ok(None)));
}

#[test]
pub fn delete_missing() {
    let db = default_db();
    let get_result = db.delete_keyspace(KeyspaceName::Value, b"key_a");
    assert!(matches!(get_result, Ok(())));
}
