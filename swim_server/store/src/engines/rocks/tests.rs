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

use crate::engines::keyspaces::{KeyspaceByteEngine, KeyspaceName, KeyspaceResolver};
use crate::engines::RangedSnapshotLoad;
use crate::iterator::EngineIterator;
use crate::mock::TransientDatabase;
use crate::stores::lane::{deserialize, serialize};
use crate::{EngineRefIterator, Store, StoreError};
use crate::{RocksDatabase, RocksOpts};
use std::collections::HashMap;
use std::ops::Range;

fn default_db() -> TransientDatabase<RocksDatabase> {
    TransientDatabase::<RocksDatabase>::new(RocksOpts::keyspace_options())
}

fn assert_keyspaces_empty<D>(db: &TransientDatabase<D>, spaces: &[KeyspaceName])
where
    D: Store,
{
    for key_space in spaces {
        let resolved = db.resolve_keyspace(&key_space).unwrap();
        let iter = db.iterator(resolved).unwrap();
        assert_eq!(Ok(false), iter.valid())
    }
}

#[test]
fn get_keyspace() {
    let db = default_db();

    let key = b"test_key";
    let value = b"test_value";

    assert!(db.put_keyspace(KeyspaceName::Value, key, value).is_ok());
    assert_keyspaces_empty(&db, &[KeyspaceName::Lane, KeyspaceName::Map]);
}

fn format_key(id: i32) -> String {
    format!("key/{}", id)
}

fn populate_keyspace<D: Store>(
    db: &TransientDatabase<D>,
    space: KeyspaceName,
    range: Range<i32>,
    clone_to: &mut HashMap<String, i32>,
) {
    for i in range {
        let key = format_key(i);
        clone_to.insert(key.clone(), i);
        assert!(db
            .put_keyspace(space, key.as_bytes(), serialize(&i).unwrap().as_slice())
            .is_ok());
    }
}

#[test]
fn engine_iterator() {
    let db = default_db();
    let range = 0..100;
    let mut expected = HashMap::new();

    populate_keyspace(&db, KeyspaceName::Value, range.clone(), &mut expected);

    let resolved = db.resolve_keyspace(&KeyspaceName::Value).unwrap();
    let mut iter = db.iterator(resolved).unwrap();

    assert_eq!(iter.seek_first(), Ok(true));

    for _ in range {
        let valid = iter.valid().expect("Invalid iterator");
        if valid {
            match (iter.key(), iter.value()) {
                (Some(key), Some(value)) => {
                    let key = String::from_utf8(key.to_vec()).unwrap();
                    let value = deserialize::<i32>(value).unwrap();

                    match expected.remove(&key) {
                        Some(expected_value) => {
                            assert_eq!(expected_value, value)
                        }
                        None => {
                            panic!("Unexpected key: {:?}", key)
                        }
                    }
                }
                e => {
                    panic!("Inconsistent state: {:?}", e);
                }
            }
            iter.seek_next();
        } else {
            panic!("Invalid iterator");
        }
    }

    assert!(expected.is_empty());
    assert_keyspaces_empty(&db, &[KeyspaceName::Lane, KeyspaceName::Map]);
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

fn map_fn<'a>(key: &'a [u8], value: &'a [u8]) -> Result<(String, String), StoreError> {
    let k = String::from_utf8(key.to_vec()).unwrap();
    let v = String::from_utf8(value.to_vec()).unwrap();

    Ok((k, v))
}

#[test]
pub fn empty_snapshot() {
    let db = default_db();
    let result = db.load_ranged_snapshot(KeyspaceName::Value, b"prefix", map_fn);
    match result {
        Ok(ss) => {
            assert!(ss.is_none());
        }
        Err(e) => panic!("{:?}", e),
    }
}

#[test]
pub fn ranged_snapshot() {
    let db = default_db();
    let prefix = "/foo/bar";
    let limit = 256;

    let format = |i| format!("{}/{}", prefix, i);
    let mut expected = HashMap::with_capacity(limit);

    // In range records
    for i in 0..limit {
        let key = format(i);
        let value = i.to_string();
        let result = db.put_keyspace(
            KeyspaceName::Value,
            key.as_bytes(),
            i.to_string().as_bytes(),
        );

        assert!(result.is_ok());

        expected.insert(key, value);
    }

    // Out of range records
    for i in 0..limit {
        let key = format!("/foo/{}", i);
        let value = i.to_string();
        let result = db.put_keyspace(KeyspaceName::Value, key.as_bytes(), value.as_bytes());

        assert!(result.is_ok());
    }

    let snapshot_result = db.load_ranged_snapshot(KeyspaceName::Value, prefix.as_bytes(), map_fn);
    assert!(matches!(snapshot_result, Ok(Some(_))));

    let snapshot = snapshot_result.unwrap().unwrap();
    let mut iter = snapshot.into_iter();

    for i in 0..limit {
        match iter.next() {
            Some((key, value)) => match expected.remove(&key) {
                Some(expected_value) => {
                    assert_eq!(expected_value, value);
                }
                None => {
                    panic!("Missing key: `{}`", format(i));
                }
            },
            None => {
                panic!("Missing key: `{}`", format(i));
            }
        }
    }

    assert!(iter.next().is_none());
    assert!(expected.is_empty());
}
