// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KINDither express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::ops::Deref;

use tempdir::TempDir;

use crate::engines::{ByteEngine, RangedSnapshotLoad, RocksOpts};
use crate::{RocksDatabase, Store, StoreError};

pub struct TransientDatabase<D> {
    _dir: TempDir,
    delegate: D,
}

impl<D> TransientDatabase<D>
where
    D: Store,
{
    pub fn new(opts: D::Opts) -> TransientDatabase<D> {
        let dir = temp_dir();
        let delegate = D::from_opts(dir.path(), &opts).expect("Failed to build delegate store");

        TransientDatabase {
            _dir: dir,
            delegate,
        }
    }
}

impl<D> Deref for TransientDatabase<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.delegate
    }
}

pub fn temp_dir() -> TempDir {
    TempDir::new("test").expect("Failed to create temporary directory")
}

#[test]
pub fn crud() {
    let db = TransientDatabase::<RocksDatabase>::new(RocksOpts::default());

    let key = b"key_a";
    let value_1 = b"value_a";
    let value_2 = b"value_b";

    assert!(db.put(key, value_1).is_ok());

    let get_result = db.get(key);
    assert!(matches!(get_result, Ok(Some(_))));
    let get_value = get_result.unwrap().unwrap();
    assert_eq!(value_1, String::from_utf8(get_value).unwrap().as_bytes());

    let update_result = db.put(key, value_2);
    assert!(update_result.is_ok());

    let get_result = db.get(key);
    assert!(matches!(get_result, Ok(Some(_))));
    let get_value = get_result.unwrap().unwrap();
    assert_eq!(value_2, String::from_utf8(get_value).unwrap().as_bytes());

    let delete_result = db.delete(key);
    assert!(matches!(delete_result, Ok(())));
}

#[test]
pub fn get_missing() {
    let db = TransientDatabase::<RocksDatabase>::new(RocksOpts::default());
    let get_result = db.get(b"key_a");
    assert!(matches!(get_result, Ok(None)));
}

#[test]
pub fn delete_missing() {
    let db = TransientDatabase::<RocksDatabase>::new(RocksOpts::default());
    let get_result = db.delete(b"key_a");
    assert!(matches!(get_result, Ok(())));
}

fn map_fn<'a>(key: &'a [u8], value: &'a [u8]) -> Result<(String, String), StoreError> {
    let k = String::from_utf8(key.to_vec()).unwrap();
    let v = String::from_utf8(value.to_vec()).unwrap();

    Ok((k, v))
}

#[test]
pub fn empty_snapshot() {
    let db = TransientDatabase::<RocksDatabase>::new(RocksOpts::default());
    let result = db.load_ranged_snapshot(b"prefix", map_fn);
    assert!(matches!(result, Ok(None)));
}

#[test]
pub fn ranged_snapshot() {
    let db = TransientDatabase::<RocksDatabase>::new(RocksOpts::default());
    let prefix = "/foo/bar";
    let limit = 256;

    let format = |i| format!("{}/{}", prefix, i);
    let mut expected = HashMap::with_capacity(limit);

    // In range records
    for i in 0..limit {
        let key = format(i);
        let value = i.to_string();
        let result = db.put(key.as_bytes(), i.to_string().as_bytes());

        assert!(result.is_ok());

        expected.insert(key, value);
    }

    // Out of range records
    for i in 0..limit {
        let key = format!("/foo/{}", i);
        let value = i.to_string();
        let result = db.put(key.as_bytes(), value.as_bytes());

        assert!(result.is_ok());
    }

    let snapshot_result = db.load_ranged_snapshot(prefix.as_bytes(), map_fn);
    assert!(matches!(snapshot_result, Ok(Some(_))));

    let snapshot = snapshot_result.unwrap().unwrap();
    let mut iter = snapshot.into_iter().peekable();

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

    assert!(iter.peek().is_none());
    assert!(expected.is_empty());
}
