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

#[cfg(test)]
mod tests;

mod iterator;

pub use iterator::RocksRawPrefixIterator;

use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, ReadOptions};
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::Arc;
use swimos_api::error::StoreError;

use crate::keyspaces::{Keyspace, KeyspaceByteEngine, KeyspaceResolver, Keyspaces};
use crate::store::{Store, StoreBuilder};
use crate::utils::{serialize_u64, MAX_ID_SIZE};

/// A Rocks database engine.
///
/// See <https://github.com/facebook/rocksdb/wiki> for details about the features and limitations.
#[derive(Debug, Clone)]
pub struct RocksEngine {
    pub(crate) delegate: Arc<DB>,
}

impl Store for RocksEngine {}

impl RocksEngine {
    pub fn new(delegate: DB) -> RocksEngine {
        RocksEngine {
            delegate: Arc::new(delegate),
        }
    }
}

impl KeyspaceResolver for RocksEngine {
    type ResolvedKeyspace = ColumnFamily;

    fn resolve_keyspace<K: Keyspace>(&self, space: &K) -> Option<&Self::ResolvedKeyspace> {
        self.delegate.cf_handle(space.name())
    }
}

/// Configuration wrapper for a Rocks database used by `FromOpts`.
#[derive(Clone)]
pub struct RocksOpts(pub Options);

impl StoreBuilder for RocksOpts {
    type Store = RocksEngine;

    fn build<I>(self, path: I, keyspaces: &Keyspaces<Self>) -> Result<Self::Store, StoreError>
    where
        I: AsRef<Path>,
    {
        let Keyspaces { keyspaces } = keyspaces;
        let descriptors =
            keyspaces
                .iter()
                .fold(Vec::with_capacity(keyspaces.len()), |mut vec, def| {
                    let cf_descriptor =
                        ColumnFamilyDescriptor::new(def.name.to_string(), def.opts.0.clone());
                    vec.push(cf_descriptor);
                    vec
                });

        let db = DB::open_cf_descriptors(&self.0, path, descriptors)
            .map_err(|e| StoreError::Delegate(Box::new(e)))?;
        Ok(RocksEngine::new(db))
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

fn exec_keyspace<F, O, K>(delegate: &Arc<DB>, keyspace: K, f: F) -> Result<O, StoreError>
where
    F: Fn(&Arc<DB>, &ColumnFamily) -> Result<O, rocksdb::Error>,
    K: Keyspace,
{
    match delegate.cf_handle(keyspace.name()) {
        Some(cf) => f(delegate, cf).map_err(|e| StoreError::Delegate(Box::new(e))),
        None => Err(StoreError::KeyspaceNotFound),
    }
}

impl KeyspaceByteEngine for RocksEngine {
    type RangeCon<'a> = RocksRawPrefixIterator<'a>
    where
        Self: 'a;

    fn get_prefix_range_consumer<'a, S>(
        &'a self,
        keyspace: S,
        prefix: &[u8],
    ) -> Result<Self::RangeCon<'a>, StoreError>
    where
        S: Keyspace,
    {
        let resolved = self
            .resolve_keyspace(&keyspace)
            .ok_or(StoreError::KeyspaceNotFound)?;

        let mut read_opts = ReadOptions::default();
        read_opts.set_prefix_same_as_start(true);
        let mut iter = self.delegate.raw_iterator_cf_opt(resolved, read_opts);
        iter.seek(prefix);
        Ok(RocksRawPrefixIterator::new(iter))
    }

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
        value: u64,
    ) -> Result<(), StoreError> {
        let mut buf = [0u8; MAX_ID_SIZE];
        let value = serialize_u64(value, &mut buf);
        exec_keyspace(&self.delegate, keyspace, move |delegate, keyspace| {
            delegate.merge_cf(keyspace, key, value)
        })
    }

    fn delete_key_range<S>(
        &self,
        keyspace: S,
        start: &[u8],
        ubound: &[u8],
    ) -> Result<(), StoreError>
    where
        S: Keyspace,
    {
        exec_keyspace(&self.delegate, keyspace, move |delegate, keyspace| {
            delegate.delete_range_cf(keyspace, start, ubound)
        })
    }
}
