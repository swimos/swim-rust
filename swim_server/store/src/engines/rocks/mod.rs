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

#[cfg(test)]
mod tests;

mod iterator;

use crate::engines::keyspaces::{
    incrementing_merge_operator, KeyType, KeyspaceByteEngine, KeyspaceName, KeyspaceOptions,
    KeyspaceResolver, Keyspaces, LANE_KS, MAP_LANE_KS, VALUE_LANE_KS,
};
use crate::engines::{KeyedSnapshot, RangedSnapshotLoad};
use crate::iterator::{EnginePrefixIterator, EngineRefIterator};
use crate::stores::lane::serialize;
use crate::stores::INCONSISTENT_DB;
use crate::{FromKeyspaces, Store, StoreError, StoreInfo};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, SliceTransform};
use rocksdb::{Error, Options, DB};
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

impl From<rocksdb::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Delegate(Box::new(e))
    }
}

/// A Rocks database engine.
///
/// See https://github.com/facebook/rocksdb/wiki for details about the features and limitations.
#[derive(Debug)]
pub struct RocksDatabase {
    pub(crate) delegate: Arc<DB>,
}

fn default_lane_opts() -> Options {
    let mut opts = rocksdb::Options::default();
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<KeyType>()));
    opts.set_memtable_prefix_bloom_ratio(0.2);

    opts
}

fn build_cf(name: &str) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(name, default_lane_opts())
}

impl RocksDatabase {
    pub fn new(delegate: DB) -> RocksDatabase {
        RocksDatabase {
            delegate: Arc::new(delegate),
        }
    }

    pub fn open_default<P>(path: P) -> Result<RocksDatabase, StoreError>
    where
        P: AsRef<Path>,
    {
        let mut rock_opts = rocksdb::Options::default();
        rock_opts.create_if_missing(true);
        rock_opts.create_missing_column_families(true);

        let mut lane_opts = rocksdb::Options::default();
        lane_opts.set_merge_operator_associative("lane_id_counter", incrementing_merge_operator);
        let lane_table_cf = ColumnFamilyDescriptor::new(LANE_KS, lane_opts);

        let value_cf = build_cf(VALUE_LANE_KS);
        let map_cf = build_cf(MAP_LANE_KS);
        let db = DB::open_cf_descriptors(&rock_opts, path, vec![lane_table_cf, value_cf, map_cf])?;

        Ok(RocksDatabase {
            delegate: Arc::new(db),
        })
    }
}

impl Store for RocksDatabase {
    fn path(&self) -> &Path {
        &self.delegate.path()
    }

    fn store_info(&self) -> StoreInfo {
        StoreInfo {
            path: self.path().to_string_lossy().to_string(),
            kind: "RocksDB".to_string(),
        }
    }
}

impl KeyspaceResolver for RocksDatabase {
    type ResolvedKeyspace = ColumnFamily;

    fn resolve_keyspace(&self, space: &KeyspaceName) -> Option<&Self::ResolvedKeyspace> {
        self.delegate.cf_handle(space.as_ref())
    }
}

impl FromKeyspaces for RocksDatabase {
    type EnvironmentOpts = RocksOpts;
    type KeyspaceOpts = RocksOpts;

    fn from_keyspaces<I: AsRef<Path>>(
        path: I,
        db_opts: &Self::KeyspaceOpts,
        keyspaces: &Keyspaces<Self>,
    ) -> Result<Self, StoreError> {
        let Keyspaces { lane, value, map } = keyspaces;
        let descriptors = vec![
            ColumnFamilyDescriptor::new(lane.name.to_string(), lane.opts.0.clone()),
            ColumnFamilyDescriptor::new(value.name.to_string(), value.opts.0.clone()),
            ColumnFamilyDescriptor::new(map.name.to_string(), map.opts.0.clone()),
        ];

        let db = DB::open_cf_descriptors(&db_opts.0, path, descriptors)?;
        Ok(RocksDatabase::new(db))
    }
}

/// Configuration wrapper for a Rocks database used by `FromOpts`.
pub struct RocksOpts(pub Options);

impl RocksOpts {
    pub fn keyspace_options() -> KeyspaceOptions<Self> {
        let mut lane_opts = rocksdb::Options::default();
        lane_opts.set_merge_operator_associative("lane_id_counter", incrementing_merge_operator);

        KeyspaceOptions {
            lane: RocksOpts(lane_opts),
            value: RocksOpts(default_lane_opts()),
            map: RocksOpts(default_lane_opts()),
        }
    }
}

impl Default for RocksOpts {
    fn default() -> Self {
        let mut rock_opts = rocksdb::Options::default();
        rock_opts.create_if_missing(true);
        rock_opts.create_missing_column_families(true);

        RocksOpts(rock_opts)
    }
}

impl RangedSnapshotLoad for RocksDatabase {
    fn load_ranged_snapshot<F, K, V>(
        &self,
        keyspace: KeyspaceName,
        prefix: &[u8],
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let resolved = self
            .resolve_keyspace(&keyspace)
            .ok_or(StoreError::KeyspaceNotFound)?;
        let mut it = self.prefix_iterator(resolved, prefix)?;
        let mut data = Vec::new();

        loop {
            match it.valid() {
                Ok(true) => {
                    match it.next_pair() {
                        (Some(key), Some(value)) => {
                            if !key.starts_with(prefix) {
                                // At this point we've hit the next set of keys
                                break;
                            } else {
                                let mapped = map_fn(key, value)?;
                                data.push(mapped);
                                it.seek_next();
                            }
                        }
                        _ => return Err(StoreError::Decoding(INCONSISTENT_DB.to_string())),
                    }
                }
                Ok(false) => break,
                Err(e) => {
                    return Err(StoreError::Delegate(Box::new(e)));
                }
            }
        }

        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(KeyedSnapshot::new(data.into_iter())))
        }
    }
}

fn exec_keyspace<F, O>(delegate: &Arc<DB>, keyspace: KeyspaceName, f: F) -> Result<O, StoreError>
where
    F: Fn(&Arc<DB>, &ColumnFamily) -> Result<O, rocksdb::Error>,
{
    match delegate.cf_handle(keyspace.as_ref()) {
        Some(cf) => f(delegate, cf).map_err(Into::into),
        None => Err(StoreError::KeyspaceNotFound),
    }
}

impl KeyspaceByteEngine for RocksDatabase {
    fn put_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError> {
        exec_keyspace(&self.delegate, keyspace, |delegate, keyspace| {
            delegate.put_cf(keyspace, key, value)
        })
    }

    fn get_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        exec_keyspace(&self.delegate, keyspace, |delegate, keyspace| {
            delegate.get_cf(keyspace, key)
        })
    }

    fn delete_keyspace(&self, keyspace: KeyspaceName, key: &[u8]) -> Result<(), StoreError> {
        exec_keyspace(&self.delegate, keyspace, |delegate, keyspace| {
            delegate.delete_cf(keyspace, key)
        })
    }

    fn merge_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
        value: KeyType,
    ) -> Result<(), StoreError> {
        let value = serialize(&value)?;
        exec_keyspace(&self.delegate, keyspace, move |delegate, keyspace| {
            delegate.merge_cf(keyspace, key, value.as_slice())
        })
    }
}
