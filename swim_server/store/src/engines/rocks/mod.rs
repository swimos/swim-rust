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

pub use crate::engines::rocks::iterator::{RocksIterator, RocksPrefixIterator};
use crate::engines::KeyedSnapshot;
use crate::iterator::{EnginePrefixIterator, EngineRefIterator};
use crate::keyspaces::{
    KeyType, Keyspace, KeyspaceByteEngine, KeyspaceRangedSnapshotLoad, KeyspaceResolver, Keyspaces,
};
use crate::{serialize, FromKeyspaces, Store, StoreError, StoreInfo};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor};
use rocksdb::{Error, Options, DB};
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
#[derive(Debug, Clone)]
pub struct RocksEngine {
    pub(crate) delegate: Arc<DB>,
}

impl RocksEngine {
    pub fn new(delegate: DB) -> RocksEngine {
        RocksEngine {
            delegate: Arc::new(delegate),
        }
    }
}

impl Store for RocksEngine {
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

impl KeyspaceResolver for RocksEngine {
    type ResolvedKeyspace = ColumnFamily;

    fn resolve_keyspace<K: Keyspace>(&self, space: &K) -> Option<&Self::ResolvedKeyspace> {
        self.delegate.cf_handle(space.name())
    }
}

impl FromKeyspaces for RocksEngine {
    type Opts = RocksOpts;

    fn from_keyspaces<I: AsRef<Path>>(
        path: I,
        db_opts: &Self::Opts,
        keyspaces: Keyspaces<Self>,
    ) -> Result<Self, StoreError> {
        let Keyspaces { keyspaces } = keyspaces;
        let descriptors = keyspaces.into_iter().fold(Vec::new(), |mut vec, def| {
            let cf_descriptor = ColumnFamilyDescriptor::new(def.name.to_string(), def.opts.0);
            vec.push(cf_descriptor);
            vec
        });

        let db = DB::open_cf_descriptors(&db_opts.0, path, descriptors)?;
        Ok(RocksEngine::new(db))
    }
}

/// Configuration wrapper for a Rocks database used by `FromOpts`.
#[derive(Clone)]
pub struct RocksOpts(pub Options);

impl Default for RocksOpts {
    fn default() -> Self {
        let mut rock_opts = rocksdb::Options::default();
        rock_opts.create_if_missing(true);
        rock_opts.create_missing_column_families(true);

        RocksOpts(rock_opts)
    }
}

impl KeyspaceRangedSnapshotLoad for RocksEngine {
    fn keyspace_load_ranged_snapshot<F, K, V, S>(
        &self,
        keyspace: &S,
        prefix: &[u8],
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
        S: Keyspace,
    {
        let resolved = self
            .resolve_keyspace(keyspace)
            .ok_or(StoreError::KeyspaceNotFound)?;
        let mut it = self.prefix_iterator(resolved, prefix)?;
        let mut data = Vec::new();

        loop {
            match it.valid() {
                Ok(true) => match it.next() {
                    Some((key, value)) => {
                        let mapped = map_fn(key.as_ref(), value.as_ref())?;
                        data.push(mapped);
                    }
                    None => break,
                },
                Ok(false) => break,
                Err(e) => return Err(e),
            }
        }

        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(KeyedSnapshot::new(data.into_iter())))
        }
    }
}

fn exec_keyspace<F, O, K>(delegate: &Arc<DB>, keyspace: K, f: F) -> Result<O, StoreError>
where
    F: Fn(&Arc<DB>, &ColumnFamily) -> Result<O, rocksdb::Error>,
    K: Keyspace,
{
    match delegate.cf_handle(keyspace.name()) {
        Some(cf) => f(delegate, cf).map_err(Into::into),
        None => Err(StoreError::KeyspaceNotFound),
    }
}

impl KeyspaceByteEngine for RocksEngine {
    fn put_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError> {
        exec_keyspace(&self.delegate, keyspace, |delegate, keyspace| {
            delegate.put_cf(keyspace, key, value)
        })
    }

    fn get_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        exec_keyspace(&self.delegate, keyspace, |delegate, keyspace| {
            delegate.get_cf(keyspace, key)
        })
    }

    fn delete_keyspace<K: Keyspace>(&self, keyspace: K, key: &[u8]) -> Result<(), StoreError> {
        exec_keyspace(&self.delegate, keyspace, |delegate, keyspace| {
            delegate.delete_cf(keyspace, key)
        })
    }

    fn merge_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
        value: KeyType,
    ) -> Result<(), StoreError> {
        let value = serialize(&value)?;
        exec_keyspace(&self.delegate, keyspace, move |delegate, keyspace| {
            delegate.merge_cf(keyspace, key, value.as_slice())
        })
    }
}
