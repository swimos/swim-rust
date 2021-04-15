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

use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::{fs, io};

use tempdir::TempDir;

use crate::engines::{ByteEngine, FromOpts, RangedSnapshotLoad};
pub use engines::{LmdbxDatabase, RocksDatabase};
pub use stores::lane::value::ValueDataModel;
pub use stores::node::{NodeStore, SwimNodeStore};
pub use stores::plane::{PlaneStore, SwimPlaneStore};

pub mod mock;

mod engines;
mod stores;

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
    /// The provided key was not found in the store.
    KeyNotFound,
    /// An error produced when attempting to execute a snapshot read.
    Snapshot(String),
    /// The delegate byte engine failed to initialised.
    InitialisationFailure(String),
    /// An IO error produced by the delegate byte engine.
    Io(io::Error),
    /// An error produced when attempting to encode a value.
    Encoding(String),
    /// An error produced when attempting to decode a value.
    Decoding(String),
    /// A raw error produced by the delegate byte engine.
    Delegate(Box<dyn Error>),
    /// An operation was attempted on the byte engine when it was closing.
    Closing,
}

/// A Swim server store.
///
/// This trait only serves to compose the multiple traits that are required for a store.
pub trait Store:
    FromOpts + RangedSnapshotLoad + Send + Sync + Debug + ByteEngine + 'static
{
    /// Returns a reference to the path that the delegate byte engine is operating from.
    fn path(&self) -> &Path;
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

/// A trait for defining snapshots.
///
/// Typically used by node stores and will delegate the snapshot to a ranged snapshot that uses a
/// stored prefix owned by the lane.
pub trait Snapshot<K, V> {
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

/// A Swim server store that will open plane stores on request.
pub struct ServerStore<D: Store> {
    /// The directory that this store is operating from.
    dir: StoreDir,
    /// The options that all stores will be opened with.
    opts: D::Opts,
    _delegate_pd: PhantomData<D>,
}

impl<D: Store> Debug for ServerStore<D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerStore")
            .field("directory", &self.dir.path())
            .finish()
    }
}

impl<D: Store> ServerStore<D> {
    /// Constructs a new server store that will open stores using `opts` and will use the directory
    /// `base_path` for opening all new stores.
    ///
    /// # Panics
    /// Panics if the directory cannot be created.
    pub fn new(opts: D::Opts, base_path: PathBuf) -> ServerStore<D> {
        ServerStore {
            dir: StoreDir::persistent(base_path).expect("Failed to create server store"),
            opts,
            _delegate_pd: Default::default(),
        }
    }

    /// Constructs a new transient server store that will clear the directory (prefixed by `prefix`
    /// when dropped and open stores using `opts`.
    ///
    /// # Panics
    /// Panics if the directory cannot be created.
    pub fn transient(opts: D::Opts, prefix: &str) -> ServerStore<D> {
        ServerStore {
            dir: StoreDir::transient(prefix),
            opts,
            _delegate_pd: Default::default(),
        }
    }
}

impl<D: Store> SwimStore for ServerStore<D> {
    type PlaneStore = SwimPlaneStore<D>;

    fn plane_store<I: ToString>(&mut self, plane_name: I) -> Result<Self::PlaneStore, StoreError> {
        let ServerStore { opts, dir, .. } = self;
        let plane_name = plane_name.to_string();

        SwimPlaneStore::open(dir.path(), &plane_name, opts)
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::{RocksDatabase, RocksOpts};
    use crate::stores::{StoreKey, ValueStorageKey};
    use crate::{PlaneStore, ServerStore, SwimStore};
    use std::borrow::Cow;

    #[test]
    fn put_get() {
        let server_opts = RocksOpts::default();
        let mut store = ServerStore::<RocksDatabase>::transient(server_opts, "target".into());
        let plane_store = store.plane_store("unit").unwrap();

        let node_key = StoreKey::Value(ValueStorageKey {
            node_uri: Cow::Owned("node".into()),
            lane_uri: Cow::Owned("lane".into()),
        });

        let test_data = "test";

        assert!(plane_store
            .put(node_key.clone(), test_data.as_bytes())
            .is_ok());
        let value = plane_store.get(node_key).unwrap().unwrap();
        assert_eq!(Ok(test_data.to_string()), String::from_utf8(value));
    }
}
