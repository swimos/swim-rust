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

use swimos_api::error::StoreError;
use swimos_api::persistence::RangeConsumer;

/// A handle to a portion of logically partitioned data.
pub trait Keyspace {
    /// The name of the keyspace.
    fn name(&self) -> &str;
}

/// A keyspace definition for persisting logically related data.
///
/// Definitions of a keyspace will depend on the underlying delegate store implementation used to
/// run a store with. For a RocksDB engine this will correspond to a column family and for libmdbx
/// this will correspond to a sub-database that is keyed by `name`.
#[derive(Debug, Clone)]
pub struct KeyspaceDef<O> {
    /// The name of the keyspace.
    pub name: &'static str,
    /// The configuration options that will be used to open the keyspace.
    pub opts: O,
}

impl<O> KeyspaceDef<O> {
    pub fn new(name: &'static str, opts: O) -> Self {
        KeyspaceDef { name, opts }
    }
}

/// A list of keyspace definitions to initialise a store with.
#[derive(Clone)]
pub struct Keyspaces<O> {
    pub keyspaces: Vec<KeyspaceDef<O>>,
}

impl<O> Keyspaces<O> {
    pub fn new(keyspaces: Vec<KeyspaceDef<O>>) -> Self {
        Keyspaces { keyspaces }
    }
}

/// A trait for abstracting over database engines and partitioning data by a logical keyspace.
pub trait KeyspaceByteEngine: Send + Sync + 'static {
    type RangeCon<'a>: RangeConsumer + Send + 'a
    where
        Self: 'a;

    /// Read a range of records from a specific keyspace, with a shared prefix.
    /// # Arguments
    ///
    /// * `keyspace` - The keyspace to query.
    /// * `prefix` - The shared keyspace.
    fn get_prefix_range_consumer<'a, S>(
        &'a self,
        keyspace: S,
        prefix: &[u8],
    ) -> Result<Self::RangeCon<'a>, StoreError>
    where
        S: Keyspace;

    /// Put a key-value pair into the specified keyspace.
    fn put_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError>;

    /// Get an entry from the specified keyspace.
    fn get_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a value from the specified keyspace.
    fn delete_keyspace<K: Keyspace>(&self, keyspace: K, key: &[u8]) -> Result<(), StoreError>;

    /// Perform a merge operation on the specified keyspace and key, incrementing by `step`.
    fn merge_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
        step: u64,
    ) -> Result<(), StoreError>;

    /// Remove all entries from a keyspace with keys in the specified range [start, ubound).
    fn delete_key_range<S>(
        &self,
        keyspace: S,
        start: &[u8],
        ubound: &[u8],
    ) -> Result<(), StoreError>
    where
        S: Keyspace;
}

/// A trait for converting an abstract keyspace name to a reference to a handle of one in a delegate
/// engine; such as RocksDB's Column Families.
pub trait KeyspaceResolver {
    /// The concrete type of the keyspace.
    type ResolvedKeyspace;

    /// Resolve `space` in to a handle that can be used to make direct queries to a delegate engine.
    fn resolve_keyspace<K: Keyspace>(&self, space: &K) -> Option<&Self::ResolvedKeyspace>;
}
