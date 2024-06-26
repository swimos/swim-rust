// Copyright 2015-2023 Swim Inc.
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

pub mod keystore;
#[cfg(test)]
pub mod mock;
#[cfg(test)]
mod tests;

pub mod rocks;

use crate::agent::StoreWrapper;
use crate::engine::RocksOpts;
use crate::keyspaces::{Keyspace, Keyspaces};
use crate::store::{KeyspaceName, StoreBuilder};
use rocks::default_keyspaces;
use std::fmt::{Debug, Formatter};
use std::io::{self, Write};
use std::path::PathBuf;
use swimos_api::error::StoreError;
use swimos_api::persistence::ServerPersistence;

use crate::plane::{open_plane, PlaneStore, SwimPlaneStore};
use integer_encoding::FixedInt;
use swimos_utilities::fs::Dir;

pub use rocks::default_db_opts;

use crate::engine::RocksEngine;

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
    fn plane_store<I>(&self, plane_name: I) -> Result<Self::PlaneStore, StoreError>
    where
        I: ToString;
}

/// A Swim server store that will open plane stores on request.
pub struct ServerStore<D>
where
    D: StoreBuilder,
{
    /// The directory that this store is operating from.
    dir: Dir,
    /// Database environment open options
    builder: D,
    /// The keyspaces that all stores will be opened with.
    keyspaces: Keyspaces<D>,
}

impl<D> Debug for ServerStore<D>
where
    D: StoreBuilder,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerStore")
            .field("directory", &self.dir.path())
            .finish()
    }
}

impl<D> ServerStore<D>
where
    D: StoreBuilder,
{
    /// Constructs a new server store that will open stores using `opts` and will use the directory
    /// `base_path` for opening all new stores.
    ///
    /// # Panics
    /// Panics if the directory cannot be created.
    pub fn new(
        builder: D,
        keyspaces: Keyspaces<D>,
        base_path: PathBuf,
    ) -> io::Result<ServerStore<D>> {
        Ok(ServerStore {
            dir: Dir::persistent(base_path)?,
            builder,
            keyspaces,
        })
    }

    /// Constructs a new transient server store that will clear the directory (prefixed by `prefix`)
    /// when dropped and open stores using `db_opts`.
    ///
    /// # Panics
    /// Panics if the directory cannot be created.
    pub fn transient(
        builder: D,
        keyspaces: Keyspaces<D>,
        prefix: &str,
    ) -> io::Result<ServerStore<D>> {
        Ok(ServerStore {
            dir: Dir::transient(prefix)?,
            builder,
            keyspaces,
        })
    }
}

impl<D> SwimStore for ServerStore<D>
where
    D: StoreBuilder,
{
    type PlaneStore = SwimPlaneStore<D::Store>;

    fn plane_store<I: ToString>(&self, plane_name: I) -> Result<Self::PlaneStore, StoreError> {
        let ServerStore {
            builder,
            keyspaces,
            dir,
            ..
        } = self;
        let plane_name = plane_name.to_string();

        open_plane(dir.path(), plane_name, builder.clone(), keyspaces.clone())
    }
}

/// A lane key that is either a map lane key or a value lane key.
#[derive(Clone, Debug, PartialOrd, PartialEq, Eq)]
pub enum StoreKey {
    /// A map lane key.
    ///
    /// Within plane stores, map lane keys are defined in the format of `/lane_id/key` where `key`
    /// is the key of a lane's map data structure.
    Map {
        /// The lane ID.
        lane_id: u64,
        /// An optional, serialized, key. This is optional as ranged snapshots to not require the
        /// key.
        key: Option<Vec<u8>>,
    },
    /// A value lane key.
    Value {
        /// The lane ID.
        lane_id: u64,
    },
}

const ID_LEN: usize = 8;
const SIZE_LEN: usize = 8;
const TAG_LEN: usize = 1;

const VAL_TAG: u8 = 0;
const MAP_TAG: u8 = 1;

const KEY: u8 = 1;
const UBOUND: u8 = 2;

/// Unique lane identifier keyspace. The name is `default` as either the Rust RocksDB crate or
/// Rocks DB itself has an issue in using merge operators under a non-default column family.
///
/// See: https://github.com/rust-rocksdb/rust-rocksdb/issues/29
const LANE_KS: &str = "default";
/// Value lane store keyspace.
const VALUE_LANE_KS: &str = "value_lanes";
/// Map lane store keyspace.
const MAP_LANE_KS: &str = "map_lanes";

impl Keyspace for KeyspaceName {
    fn name(&self) -> &str {
        match self {
            KeyspaceName::Lane => LANE_KS,
            KeyspaceName::Value => VALUE_LANE_KS,
            KeyspaceName::Map => MAP_LANE_KS,
        }
    }
}

impl StoreKey {
    pub const MAP_KEY_PREFIX_SIZE: usize = ID_LEN + 2 * TAG_LEN + SIZE_LEN;

    pub fn write_into<W>(&self, mut writer: W) -> Result<(), std::io::Error>
    where
        W: Write,
    {
        match self {
            StoreKey::Map { lane_id, key } => {
                writer.write_all(&[MAP_TAG])?;
                writer.write_all(&lane_id.encode_fixed_light())?;
                if let Some(key) = key {
                    writer.write_all(&[KEY])?;
                    let len = u64::try_from(key.len()).expect("Length does not fit into u64");
                    writer.write_all(&len.encode_fixed_light())?;
                    writer.write_all(key)?;
                }
            }
            StoreKey::Value { lane_id } => {
                writer.write_all(&[VAL_TAG])?;
                writer.write_all(&lane_id.encode_fixed_light())?;
            }
        }
        Ok(())
    }

    fn ser_len(&self) -> usize {
        match self {
            StoreKey::Map {
                key: Some(key_bytes),
                ..
            } => key_bytes.len() + ID_LEN + 2 * TAG_LEN + SIZE_LEN,
            Self::Map { .. } => ID_LEN + 2 * TAG_LEN,
            StoreKey::Value { .. } => ID_LEN + TAG_LEN,
        }
    }

    pub fn serialize_as_bytes(&self) -> Vec<u8> {
        let mut target = Vec::with_capacity(self.ser_len());
        self.write_into(&mut target)
            .expect("Writing into a Vec should be infallible.");
        target
    }

    pub fn write_map_ubound<W>(lane_id: u64, mut writer: W) -> Result<(), std::io::Error>
    where
        W: Write,
    {
        writer.write_all(&[MAP_TAG])?;
        writer.write_all(&lane_id.encode_fixed_light())?;
        writer.write_all(&[UBOUND])?;
        Ok(())
    }

    pub fn map_ubound_bytes(lane_id: u64) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ID_LEN + 2 * TAG_LEN);
        Self::write_map_ubound(lane_id, &mut bytes)
            .expect("Writing into a Vec should be infallible.");
        bytes
    }
}

pub trait StoreEngine {
    /// Put a key-value pair into the delegate store.
    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError>;

    /// Get a value keyed by a store key from the delegate store.
    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a key-value pair by its store key from the delegate store.
    fn delete(&self, key: StoreKey) -> Result<(), StoreError>;
}

struct RocksServerPersistence {
    inner: ServerStore<RocksOpts>,
}

const PREFIX: &str = "swimos_store";

impl ServerPersistence for RocksServerPersistence {
    type PlaneStore = StoreWrapper<SwimPlaneStore<RocksEngine>>;

    fn open_plane(&self, name: &str) -> Result<Self::PlaneStore, StoreError> {
        let RocksServerPersistence { inner } = self;
        let store = inner.plane_store(name)?;
        Ok(StoreWrapper(store))
    }
}

/// Open a RocksDB persistence store from a path in the local filesystem. If the specified database does
/// not exist it will be crated.
///
/// # Arguments
/// * `path` - The filesystem path to the database. If none is specified, a new database will be created in a temporary directory.
/// * `options` - Configuration options for the database.
pub fn open_rocks_store(
    path: Option<PathBuf>,
    options: RocksOpts,
) -> Result<impl ServerPersistence + Send + Sync + 'static, StoreError> {
    let keyspaces = default_keyspaces();

    let server_store = match path {
        Some(base_path) => ServerStore::new(options, keyspaces, base_path),
        _ => ServerStore::transient(options, keyspaces, PREFIX),
    }?;
    Ok(RocksServerPersistence {
        inner: server_store,
    })
}
