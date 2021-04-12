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

#[cfg(feature = "mock")]
pub mod mock;

pub mod engines;
pub mod stores;

use crate::engines::db::StoreDelegateConfig;
use crate::stores::plane::{PlaneStore, PlaneStoreInner, SwimPlaneStore};
use bincode::Error;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::vec::IntoIter;
use std::{fs, io};

pub use rocksdb::Options;
use tempdir::TempDir;

/// A directory on the file system used for sever stores.
#[derive(Debug)]
pub enum StoreDir {
    /// A transient directory on the filesystem that is automatically deleted when it is dropped.
    Transient(TempDir),
    /// A persistent directory on the filesystem.
    Persistent(PathBuf),
}

impl StoreDir {
    const TEMP_DIR_ERR: &'static str = "Failed to open temporary directory";

    /// Attempts to create a directory at the provided path. If the path already exists, then
    /// this will return successfully and the directory will be used for stores.
    ///
    /// # Errors
    /// Errors if the directory cannot be created.
    pub fn persistent<I: AsRef<Path>>(path: I) -> io::Result<StoreDir> {
        match fs::create_dir(&path) {
            Ok(_) => Ok(StoreDir::Persistent(path.as_ref().to_path_buf())),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                Ok(StoreDir::Persistent(path.as_ref().to_path_buf()))
            }
            Err(e) => Err(e),
        }
    }

    /// Creates a temporary directory on the filesystem prefixed by `prefix` that will be deleted
    /// when it is dropped.
    ///
    /// This is the preferred approach to stores that are used for testing purposes as databases
    /// such as RocksDB only allow once handle to the database.
    ///
    /// # Panics
    /// Panics if the temporary directory cannot be opened.
    pub fn transient(prefix: &str) -> StoreDir {
        let temp_dir = TempDir::new(prefix).expect(StoreDir::TEMP_DIR_ERR);
        StoreDir::Transient(temp_dir)
    }

    /// Returns path of the temporary directory.
    pub fn path(&self) -> &Path {
        match self {
            StoreDir::Transient(dir) => dir.path(),
            StoreDir::Persistent(path) => path.as_path(),
        }
    }
}

/// Store errors.
#[derive(Debug)]
pub enum StoreError {
    /// An entity with the provided key could not be found.
    KeyNotFound,
    /// Generic error with its cause as a string.
    // todo: remove
    Error(String),
}

impl From<bincode::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Error(e.to_string())
    }
}

impl From<StoreInitialisationError> for StoreError {
    fn from(e: StoreInitialisationError) -> Self {
        StoreError::Error(format!("{:?}", e))
    }
}

/// Engine options for both map and value lanes.
#[derive(Debug, Clone, Default)]
pub struct StoreEngineOpts {
    /// Options that are used to instantiate map stores.
    map_opts: MapStoreEngineOpts,
    /// Options that are used to instantiate value stores.
    value_opts: ValueStoreEngineOpts,
}

impl StoreEngineOpts {
    pub fn rocks_default() -> StoreEngineOpts {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);

        StoreEngineOpts {
            map_opts: MapStoreEngineOpts {
                config: StoreDelegateConfig::Rocksdb(opts.clone()),
            },
            value_opts: ValueStoreEngineOpts {
                config: StoreDelegateConfig::Rocksdb(opts),
            },
        }
    }
}

/// Options that are used to instantiate map stores.
#[derive(Default, Debug, Clone)]
pub struct MapStoreEngineOpts {
    pub config: StoreDelegateConfig,
}

/// Options that are used to instantiate value stores.
#[derive(Default, Debug, Clone)]
pub struct ValueStoreEngineOpts {
    pub config: StoreDelegateConfig,
}

/// An error that was produced when attempting to instantiate a store.
#[derive(Debug)]
pub enum StoreInitialisationError {
    /// Generic error with its cause as a string.
    // todo: remove
    Error(String),
}

/// An error that was produced when attempting to snapshot a store.
pub enum SnapshotError {
    // todo add variants
}

/// A Swim server store.
///
/// This trait only serves to compose the multiple traits that are required for the store.
pub trait Store:
    for<'a> StoreEngine<'a> + FromOpts + RangedSnapshot + Send + Sync + 'static
{
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

/// A trait for executing ranged snapshots on stores.
// Todo: implement borrowed streaming snapshots.
pub trait RangedSnapshot {
    /// The prefix to seek for.
    type Prefix;

    /// Execute a ranged snapshot on the store, seeking by `prefix` and deserializing results with
    /// `map_fn`.
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
    fn ranged_snapshot<F, K, V>(
        &self,
        prefix: Self::Prefix,
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
    pub fn new(data: IntoIter<(K, V)>) -> Self {
        KeyedSnapshot { data }
    }
}

impl<K, V> Iterator for KeyedSnapshot<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next()
    }
}

/// A trait for defining snapshots.
///
/// Typically used by node stores and will delegate the snapshot to a ranged snapshot that uses a
/// stored prefix owned by the lane.
pub trait Snapshot<K, V>: RangedSnapshot {
    /// The type of the snapshot. An iterator that will yield a deserialized key-value pair.
    type Snapshot: IntoIterator<Item = (K, V)>;

    /// Execute a snapshot on the store engine.
    ///
    /// Returns `Ok(None)` if no records matched `prefix` or `Ok(Some)` if matches were found.
    ///
    /// # Errors
    /// Errors if an error is encountered when attempting to execute the snapshot on the store
    /// engine or if deserializing a key or value fails.
    fn snapshot(&self) -> Result<Option<Self::Snapshot>, StoreError>;
}

/// A trait for building stores from their options.
pub trait FromOpts: Sized {
    /// The type of options this store accepts.
    type Opts;

    /// Build a store from options.
    ///
    /// Errors if there was an issue opening the store.
    ///
    /// # Arguments:
    /// `path`: the path that this store should open in.
    /// `opts`: the options.
    ///
    fn from_opts<I: AsRef<Path>>(
        path: I,
        opts: &Self::Opts,
    ) -> Result<Self, StoreInitialisationError>;
}

/// A key-value store.
///
/// These may be either a delegate store or a concrete implementation.
pub trait StoreEngine<'i>: 'static {
    /// The key type that this store accepts.
    type Key: 'i;

    /// The value type that this store accepts.
    type Value: 'i;

    /// The error type that this store will return.
    type Error: Into<StoreError>;

    /// Put a key-value pair into this store.
    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error>;

    /// Get an entry from this store by its key.
    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Delete a value from this store by its key.
    fn delete(&self, key: Self::Key) -> Result<(), Self::Error>;
}

/// A Swim server store that will open plane stores on request.
pub struct ServerStore {
    /// The directory that this store is operating from.
    dir: StoreDir,
    /// Weak pointers to all open plane stores.
    refs: HashMap<String, Weak<PlaneStoreInner>>,
    /// The options that all stores will be opened with.
    opts: StoreEngineOpts,
}

impl ServerStore {
    /// Constructs a new server store that will open stores using `opts` and will use the directory
    /// `base_path` for opening all new stores.
    ///
    /// # Panics
    /// Panics if the directory cannot be created.
    pub fn new(opts: StoreEngineOpts, base_path: PathBuf) -> ServerStore {
        ServerStore {
            dir: StoreDir::persistent(base_path).expect("Failed to create server store"),
            refs: HashMap::new(),
            opts,
        }
    }

    /// Constructs a new transient server store that will clear the directory (prefixed by `prefix`
    /// when dropped and open stores using `opts`.
    ///
    /// # Panics
    /// Panics if the directory cannot be created.
    pub fn transient(opts: StoreEngineOpts, prefix: &str) -> ServerStore {
        ServerStore {
            dir: StoreDir::transient(prefix),
            refs: HashMap::new(),
            opts,
        }
    }
}

impl SwimStore for ServerStore {
    type PlaneStore = SwimPlaneStore;

    fn plane_store<I: ToString>(&mut self, plane_name: I) -> Result<Self::PlaneStore, StoreError> {
        let ServerStore { refs, opts, dir } = self;
        let plane_name = plane_name.to_string();

        let store = match refs.get(&plane_name) {
            Some(store) => match store.upgrade() {
                Some(store) => return Ok(SwimPlaneStore::from_inner(store)),
                None => Arc::new(PlaneStoreInner::open(dir.path(), &plane_name, opts)?),
            },
            None => Arc::new(PlaneStoreInner::open(dir.path(), &plane_name, opts)?),
        };

        let weak = Arc::downgrade(&store);
        refs.insert(plane_name, weak);
        Ok(SwimPlaneStore::from_inner(store))
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::db::StoreDelegateConfig;
    use crate::stores::node::NodeStore;
    use crate::stores::plane::PlaneStore;
    use crate::stores::{StoreKey, ValueStorageKey};
    use crate::{
        MapStoreEngineOpts, ServerStore, Snapshot, StoreEngine, StoreEngineOpts, SwimStore,
        ValueStoreEngineOpts,
    };
    use std::sync::Arc;

    #[test]
    fn simple_put_get() {
        let server_opts = StoreEngineOpts {
            map_opts: MapStoreEngineOpts {
                config: StoreDelegateConfig::lmdbx(),
            },
            value_opts: ValueStoreEngineOpts {
                config: StoreDelegateConfig::rocksdb(),
            },
        };

        let mut store = ServerStore::transient(server_opts, "target".into());
        let plane_store = store.plane_store("unit").unwrap();

        let node_key = StoreKey::Value(ValueStorageKey {
            node_uri: Arc::new("node".to_string()),
            lane_uri: Arc::new("lane".to_string()),
        });

        assert!(plane_store.put(node_key.clone(), b"test".to_vec()).is_ok());
        let value = plane_store.get(node_key).unwrap().unwrap();
        println!("{:?}", String::from_utf8(value));
    }

    #[tokio::test]
    async fn lane() {
        let server_opts = StoreEngineOpts {
            map_opts: MapStoreEngineOpts {
                config: StoreDelegateConfig::rocksdb(),
            },
            value_opts: ValueStoreEngineOpts {
                config: StoreDelegateConfig::rocksdb(),
            },
        };

        let mut store = ServerStore::transient(server_opts, "target".into());
        let plane_store = store.plane_store("unit").unwrap();
        let node_store = plane_store.node_store("node");
        let map_store = node_store.map_lane_store("map", false);

        assert!(map_store
            .put(&"a".to_string(), &"a".to_string())
            .await
            .is_ok());
        // let val = map_store.get(&"a".to_string());
        // println!("{}", val.unwrap().unwrap());

        let ss = map_store.snapshot().unwrap().unwrap();
        let iter = ss.into_iter();
        for i in iter {
            println!("Iter: {:?}", i);
        }

        // let value_store1 = node_store.value_lane_store("value");
        // assert!(value_store1.store(&"a".to_string()).is_ok());
        // let val = value_store1.load();
        // println!("{:?}", val);
        //
        // let value_store2: ValueLaneStore<String> = node_store.value_lane_store("value");
        // let val = value_store2.load();
        // println!("{:?}", val);
        //
        // let value_store3: ValueLaneStore<String> = node_store.value_lane_store("value2");
        // // assert!(value_store3.store(&"a".to_string()).is_ok());
        // let val = value_store3.load();
        // println!("{:?}", val);
    }
}
