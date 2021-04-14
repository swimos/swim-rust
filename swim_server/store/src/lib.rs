use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::vec::IntoIter;
use std::{fs, io};

use tempdir::TempDir;

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
    KeyNotFound,
    Snapshot(String),
    InitialisationFailure(String),
    Io(io::Error),
    Encoding(String),
    Decoding(String),
    Delegate(Box<dyn Error>),
    Closing,
}

/// A Swim server store.
///
/// This trait only serves to compose the multiple traits that are required for a store.
pub trait Store:
    FromOpts + RangedSnapshotLoad + Send + Sync + Debug + ByteEngine + 'static
{
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

/// A trait for executing ranged snapshot reads on stores.
// Todo: implement borrowed streaming snapshots.
pub trait RangedSnapshotLoad {
    /// The prefix to seek for.
    type Prefix;

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
pub trait Snapshot<K, V>: RangedSnapshotLoad {
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
    type Opts: StoreOpts;

    /// Build a store from options.
    ///
    /// Errors if there was an issue opening the store.
    ///
    /// # Arguments:
    /// `path`: the path that this store should open in.
    /// `opts`: the options.
    ///
    fn from_opts<I: AsRef<Path>>(path: I, opts: &Self::Opts) -> Result<Self, StoreError>;
}

pub trait StoreOpts: Default {}

/// A key-value store.
///
/// These may be either a delegate store or a concrete implementation.
pub trait StoreEngine<'i>: 'static {
    /// The key type that this store accepts.
    type Key: 'i;

    /// Put a key-value pair into this store.
    fn put(&self, key: Self::Key, value: Vec<u8>) -> Result<(), StoreError>;

    /// Get an entry from this store by its key.
    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a value from this store by its key.
    fn delete(&self, key: Self::Key) -> Result<(), StoreError>;
}

pub trait ByteEngine: 'static {
    /// Put a key-value pair into this store.
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), StoreError>;

    /// Get an entry from this store by its key.
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a value from this store by its key.
    fn delete(&self, key: Vec<u8>) -> Result<(), StoreError>;
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
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
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

impl<D: Store<Prefix = Vec<u8>>> SwimStore for ServerStore<D> {
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
    use crate::{ServerStore, StoreEngine, SwimStore};

    #[test]
    fn put_get() {
        let server_opts = RocksOpts::default();
        let mut store = ServerStore::<RocksDatabase>::transient(server_opts, "target".into());
        let plane_store = store.plane_store("unit").unwrap();

        let node_key = StoreKey::Value(ValueStorageKey {
            node_uri: "node".into(),
            lane_uri: "lane".into(),
        });

        let test_data = "test";

        assert!(plane_store
            .put(node_key.clone(), test_data.as_bytes().to_vec())
            .is_ok());
        let value = plane_store.get(node_key).unwrap().unwrap();
        assert_eq!(Ok(test_data.to_string()), String::from_utf8(value));
    }
}
