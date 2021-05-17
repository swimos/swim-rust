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

use std::fmt::{Debug, Formatter};
use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use keystore::KeystoreTask;
use store::keyspaces::{KeyType, Keyspace, Keyspaces};
use store::{Store, StoreError};
use utilities::fs::Dir;

use crate::plane::store::{PlaneStore, SwimPlaneStore};
use crate::store::rocks::RocksDatabase;
use crate::store::rocks::{default_db_opts, default_keyspaces};

#[cfg(test)]
pub mod mock;
#[cfg(test)]
mod tests;

pub mod keystore;
mod nostore;
mod rocks;

/// Unique lane identifier keyspace. The name is `default` as either the Rust RocksDB crate or
/// Rocks DB itself has an issue in using merge operators under a non-default column family.
///
/// See: https://github.com/rust-rocksdb/rust-rocksdb/issues/29
pub(crate) const LANE_KS: &str = "default";
/// Value lane store keyspace.
pub(crate) const VALUE_LANE_KS: &str = "value_lanes";
/// Map lane store keyspace.
pub(crate) const MAP_LANE_KS: &str = "map_lanes";

/// An enumeration over the keyspaces that exist in a store.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum KeyspaceName {
    Lane,
    Value,
    Map,
}

impl Keyspace for KeyspaceName {
    fn name(&self) -> &str {
        match self {
            KeyspaceName::Lane => LANE_KS,
            KeyspaceName::Value => VALUE_LANE_KS,
            KeyspaceName::Map => MAP_LANE_KS,
        }
    }
}

/// A Swim server store which will create plane stores on demand.
///
/// When a new plane store is requested, then the implementor is expected to either load the plane
/// from a delegate database or create a new database on demand.
pub trait SwimStore {
    /// The type of plane stores that are created.
    type PlaneStore: PlaneStore;

    /// Create a plane store with `plane_name`.
    ///
    /// # Errors
    /// Errors if the delegate database could not be created.
    fn plane_store<I>(&mut self, plane_name: I) -> Result<Self::PlaneStore, StoreError>
    where
        I: ToString;
}

/// A Swim server store that will open plane stores on request.
pub struct ServerStore<D: Store + KeystoreTask> {
    /// The directory that this store is operating from.
    dir: Dir,
    /// Database environment open options
    db_opts: D::Opts,
    /// The keyspaces that all stores will be opened with.
    keyspaces: Keyspaces<D>,
}

impl<D> Debug for ServerStore<D>
where
    D: Store + KeystoreTask,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerStore")
            .field("directory", &self.dir.path())
            .finish()
    }
}

impl<D> ServerStore<D>
where
    D: Store + KeystoreTask,
{
    /// Constructs a new server store that will open stores using `opts` and will use the directory
    /// `base_path` for opening all new stores.
    ///
    /// # Panics
    /// Panics if the directory cannot be created.
    pub fn new(
        db_opts: D::Opts,
        keyspaces: Keyspaces<D>,
        base_path: PathBuf,
    ) -> io::Result<ServerStore<D>> {
        Ok(ServerStore {
            dir: Dir::persistent(base_path)?,
            db_opts,
            keyspaces,
        })
    }

    /// Constructs a new transient server store that will clear the directory (prefixed by `prefix`)
    /// when dropped and open stores using `db_opts`.
    ///
    /// # Panics
    /// Panics if the directory cannot be created.
    pub fn transient(
        db_opts: D::Opts,
        keyspaces: Keyspaces<D>,
        prefix: &str,
    ) -> io::Result<ServerStore<D>> {
        Ok(ServerStore {
            dir: Dir::transient(prefix)?,
            db_opts,
            keyspaces,
        })
    }
}

impl ServerStore<RocksDatabase> {
    pub fn transient_default(prefix: &str) -> io::Result<ServerStore<RocksDatabase>> {
        let db_opts = default_db_opts();
        let keyspaces = default_keyspaces();

        Self::transient(db_opts, keyspaces, prefix)
    }
}

impl<D> SwimStore for ServerStore<D>
where
    D: Store + KeystoreTask,
{
    type PlaneStore = SwimPlaneStore<D>;

    fn plane_store<I: ToString>(&mut self, plane_name: I) -> Result<Self::PlaneStore, StoreError> {
        let ServerStore {
            db_opts,
            keyspaces,
            dir,
            ..
        } = self;
        let plane_name = plane_name.to_string();

        SwimPlaneStore::open(dir.path(), &plane_name, db_opts, keyspaces.clone())
    }
}

/// A lane key that is either a map lane key or a value lane key.
#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub enum StoreKey {
    /// A map lane key.
    ///
    /// Within plane stores, map lane keys are defined in the format of `/lane_id/key` where `key`
    /// is the key of a lane's map data structure.
    Map {
        /// The lane ID.
        lane_id: KeyType,
        /// An optional, serialized, key. This is optional as ranged snapshots to not require the
        /// key.
        #[serde(skip_serializing_if = "Option::is_none")]
        key: Option<Vec<u8>>,
    },
    /// A value lane key.
    Value {
        /// The lane ID.
        lane_id: KeyType,
    },
}

pub trait StoreEngine {
    /// Put a key-value pair into the delegate store.
    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError>;

    /// Get a value keyed by a store key from the delegate store.
    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a key-value pair by its store key from the delegate store.
    fn delete(&self, key: StoreKey) -> Result<(), StoreError>;
}
