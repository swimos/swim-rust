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

use crate::engines::FromKeyspaces;
use crate::StoreError;

/// The type to use for the storing unique lane identifiers.
///
/// Note: It is not recommended to change this after a store has already been initialised.
pub type KeyType = u64;

pub trait Keyspace: AsRef<str> {}

/// A keyspace definition for persisting logically related data.
///
/// Definitions of a keyspace will depend on the underlying delegate store implementation used to
/// run a store with. For a RocksDB engine this will correspond to a column family and for libmdbx
/// this will correspond to a sub-database that is keyed by `name`.
pub struct KeyspaceDef<O> {
    /// The name of the keyspace.
    pub(crate) name: &'static str,
    /// The configuration options that will be used to open the keyspace.
    pub(crate) opts: O,
}

pub struct Keyspaces<O: FromKeyspaces> {
    pub keyspaces: Vec<KeyspaceDef<O::KeyspaceOpts>>,
}

pub struct KeyspaceOptions<O> {
    pub opts: O,
}

impl<O> KeyspaceDef<O> {
    fn new(name: &'static str, opts: O) -> Self {
        KeyspaceDef { name, opts }
    }
}

fn deserialize_key<B: AsRef<[u8]>>(bytes: B) -> Result<KeyType, ()> {
    bincode::deserialize::<KeyType>(bytes.as_ref()).map_err(|_| ())
}

/// A trait for abstracting over database engines and partitioning data by a logical keyspace.
pub trait KeyspaceByteEngine: Send + Sync + 'static {
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
        step: KeyType,
    ) -> Result<(), StoreError>;
}

/// A trait for converting an abstract keyspace name to a reference to a handle of one in a delegate
/// engine; such as RocksDB's Column Families.
pub trait KeyspaceResolver {
    /// The concrete type of the keyspace.
    type ResolvedKeyspace;

    /// Resolve `space` in to a handle that can be used to make direct queries to a delegate engine.
    fn resolve_keyspace<K: Keyspace>(&self, space: &K) -> Option<&Self::ResolvedKeyspace>;
}
