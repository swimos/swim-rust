// Copyright 2015-2021 Swim Inc.
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

use std::borrow::Borrow;
use std::path::{Path, PathBuf};

use store_common::{
    ByteEngine, EngineInfo, EngineIterOpts, EngineIterator, EnginePrefixIterator,
    EngineRefIterator, IteratorKey, Keyspace, KeyspaceByteEngine, KeyspaceResolver, Keyspaces,
    KvBytes, PrefixRangeByteEngine, RangeConsumer, KeyValue, Store, StoreBuilder, StoreError,
};

/// A delegate store database that does nothing.
#[derive(Debug, Clone)]
pub struct NoStore {
    path: PathBuf,
}

impl Store for NoStore {
    fn path(&self) -> &Path {
        self.path.borrow()
    }

    fn engine_info(&self) -> EngineInfo {
        EngineInfo {
            path: "No store".to_string(),
            kind: "NoStore".to_string(),
        }
    }
}

impl KeyspaceResolver for NoStore {
    type ResolvedKeyspace = ();

    fn resolve_keyspace<K: Keyspace>(&self, _space: &K) -> Option<&Self::ResolvedKeyspace> {
        None
    }
}

pub struct NoRange;

impl RangeConsumer for NoRange {
    fn consume_next(&mut self) -> Result<Option<KeyValue<'_>>, StoreError> {
        Ok(None)
    }
}

impl<'a> PrefixRangeByteEngine<'a> for NoStore {
    type RangeCon = NoRange;

    fn get_prefix_range_consumer<S>(
        &'a self,
        _keyspace: S,
        _prefix: &[u8],
    ) -> Result<Self::RangeCon, StoreError>
    where
        S: Keyspace,
    {
        Ok(NoRange)
    }
}

impl KeyspaceByteEngine for NoStore {
    fn put_keyspace<K: Keyspace>(
        &self,
        _keyspace: K,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn get_keyspace<K: Keyspace>(
        &self,
        _keyspace: K,
        _key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete_keyspace<K: Keyspace>(&self, _keyspace: K, _key: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn merge_keyspace<K: Keyspace>(
        &self,
        _keyspace: K,
        _key: &[u8],
        _value: u64,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn get_prefix_range<F, K, V, S>(
        &self,
        _keyspace: S,
        _prefix: &[u8],
        _map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
        S: Keyspace,
    {
        Ok(None)
    }
}

#[derive(Default, Clone)]
pub struct NoStoreOpts;
impl StoreBuilder for NoStoreOpts {
    type Store = NoStore;

    fn build<I>(self, _path: I, _keyspaces: &Keyspaces<Self>) -> Result<Self::Store, StoreError>
    where
        I: AsRef<Path>,
    {
        Ok(NoStore {
            path: PathBuf::from("No store".to_string()),
        })
    }
}

impl ByteEngine for NoStore {
    /// Put operation does nothing and that always succeeds.
    fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    /// Get operation does nothing and always returns `Ok(None)`.
    fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    /// Delete operation does nothing and always returns `Ok(())`.
    fn delete(&self, _key: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }
}

pub struct NoStoreEngineIterator;
impl EngineIterator for NoStoreEngineIterator {
    fn seek_to(&mut self, _key: IteratorKey) -> Result<bool, StoreError> {
        Ok(true)
    }

    fn seek_next(&mut self) {}

    fn key(&self) -> Option<&[u8]> {
        None
    }

    fn value(&self) -> Option<&[u8]> {
        None
    }

    fn valid(&self) -> Result<bool, StoreError> {
        Ok(true)
    }
}

pub struct NoStoreEnginePrefixIterator;
impl EnginePrefixIterator for NoStoreEnginePrefixIterator {
    fn next(&mut self) -> Option<Result<KvBytes, StoreError>> {
        None
    }
}

impl<'a: 'b, 'b> EngineRefIterator<'a, 'b> for NoStore {
    type EngineIterator = NoStoreEngineIterator;
    type EnginePrefixIterator = NoStoreEnginePrefixIterator;

    fn iterator_opt(
        &'a self,
        _space: &'b Self::ResolvedKeyspace,
        _pts: EngineIterOpts,
    ) -> Result<Self::EngineIterator, StoreError> {
        Ok(NoStoreEngineIterator)
    }

    fn prefix_iterator_opt(
        &'a self,
        _space: &'b Self::ResolvedKeyspace,
        _opts: EngineIterOpts,
        _prefix: &'b [u8],
    ) -> Result<Self::EnginePrefixIterator, StoreError> {
        Ok(NoStoreEnginePrefixIterator)
    }
}
