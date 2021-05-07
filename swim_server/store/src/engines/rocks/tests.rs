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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::engines::keyspaces::{
    KeyspaceByteEngine, KeyspaceName, KeyspaceOptions, KeyspaceResolver, Keyspaces,
};
use crate::iterator::EngineIterator;
use crate::stores::lane::{deserialize, serialize};
use crate::{EngineRefIterator, Store};
use crate::{RocksDatabase, RocksOpts};
use std::collections::HashMap;
use std::ops::{Deref, Range};
use tempdir::TempDir;

fn temp_dir() -> TempDir {
    TempDir::new("test").expect("Failed to create temporary directory")
}

impl<D> Deref for TransientDatabase<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.delegate
    }
}

pub struct TransientDatabase<D> {
    _dir: TempDir,
    delegate: D,
}

impl<D> TransientDatabase<D>
where
    D: Store,
{
    fn new(keyspace_opts: KeyspaceOptions<D::KeyspaceOpts>) -> TransientDatabase<D> {
        let dir = temp_dir();
        let delegate = D::from_keyspaces(
            dir.path(),
            &Default::default(),
            &Keyspaces::from(keyspace_opts),
        )
        .expect("Failed to build delegate store");

        TransientDatabase {
            _dir: dir,
            delegate,
        }
    }
}

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
            match iter.seek_next() {
                Ok(true) => continue,
                Ok(false) => break,
                Err(e) => panic!("Inconsistent state: {:?}", e),
            }
        } else {
            panic!("Invalid iterator");
        }
    }

    assert!(expected.is_empty());
    assert_keyspaces_empty(&db, &[KeyspaceName::Lane, KeyspaceName::Map]);
}
