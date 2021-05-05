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

use std::path::Path;
use std::vec::IntoIter;

#[cfg(feature = "libmdbx")]
pub use lmdbx::{LmdbOpts, LmdbxDatabase};
pub use nostore::{NoStore, NoStoreOpts};
#[cfg(feature = "rocks-db")]
pub use rocks::{RocksDatabase, RocksOpts};

use crate::engines::keyspaces::{KeyspaceName, Keyspaces};
use crate::StoreError;

#[cfg(test)]
mod test_suite;

pub mod keyspaces;
#[cfg(feature = "libmdbx")]
mod lmdbx;
mod nostore;
#[cfg(feature = "rocks-db")]
mod rocks;

/// A storage engine for server stores that handles byte arrays.
pub trait ByteEngine: 'static {
    /// Put a key-value pair into this store.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StoreError>;

    /// Get an entry from this store by its key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a value from this store by its key.
    fn delete(&self, key: &[u8]) -> Result<(), StoreError>;
}

/// A trait for building stores from their keyspace definitions..
pub trait FromKeyspaces: Sized {
    /// Store environment open options. For some delegates, this may not be used - such as libmdbx.
    type EnvironmentOpts: StoreOpts;
    /// The type of options to open a keyspace with.
    type KeyspaceOpts;

    /// Build a store from options.
    ///
    /// Errors if there was an issue opening the store.
    ///
    /// # Arguments:
    /// `path`: the path that this store should open in.
    /// `opts`: the options.
    /// `keyspaces`: a set of keyspaces to open.
    fn from_keyspaces<I: AsRef<Path>>(
        path: I,
        db_opts: &Self::EnvironmentOpts,
        keyspaces: &Keyspaces<Self>,
    ) -> Result<Self, StoreError>;
}

pub trait StoreOpts: Default {}

/// A trait for executing ranged snapshot reads on stores.
// Todo: implement borrowed streaming snapshots.
pub trait RangedSnapshotLoad {
    /// Execute a ranged snapshot read on the store, seeking by `prefix` and deserializing results
    /// with `map_fn`.
    ///
    /// Returns `Ok(None)` if no records matched `prefix` or `Ok(Some)` if matches were found.
    ///
    /// # Example:
    /// Given a store engine that stores records for map lanes where the format of
    /// `/node_uri/lane_uri/key` is used as the key. One could execute a ranged snapshot on the
    /// store engine with a prefix of `/node_1/lane_1/` to load all of the keys and values for that
    /// lane.
    ///
    /// # Errors
    /// Errors if an error is encountered when attempting to execute the ranged snapshot on the
    /// store engine or if the `map_fn` fails to deserialize a key or value.
    fn load_ranged_snapshot<F, K, V>(
        &self,
        keyspace: KeyspaceName,
        prefix: &[u8],
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>;
}

/// An owned snapshot of deserialized keys and values produced by `RangedSnapshot`.
pub struct KeyedSnapshot<K, V> {
    data: IntoIter<(K, V)>,
}

impl<K, V> KeyedSnapshot<K, V> {
    pub(crate) fn new(data: IntoIter<(K, V)>) -> Self {
        KeyedSnapshot { data }
    }
}

impl<K, V> Iterator for KeyedSnapshot<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next()
    }
}
