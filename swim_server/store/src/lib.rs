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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::vec::IntoIter;
use tempdir::TempDir;

#[derive(Debug)]
pub enum StoreDir {
    Transient(TempDir),
    Persistent(PathBuf),
}

impl StoreDir {
    const DIR_ERR: &'static str = "Failed to open temporary directory";

    pub fn persistent<I: AsRef<Path>>(path: I) -> StoreDir {
        StoreDir::Persistent(path.as_ref().to_path_buf())
    }

    pub fn transient(prefix: &str) -> StoreDir {
        let temp_dir = TempDir::new(prefix).expect(StoreDir::DIR_ERR);
        StoreDir::Transient(temp_dir)
    }

    pub fn path(&self) -> &Path {
        match self {
            StoreDir::Transient(dir) => dir.path(),
            StoreDir::Persistent(path) => path.as_path(),
        }
    }
}

#[derive(Debug)]
pub enum StoreError {
    KeyNotFound,
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

#[derive(Debug, Clone)]
pub struct StoreEngineOpts {
    map_opts: MapStoreEngineOpts,
    value_opts: ValueStoreEngineOpts,
}

impl Default for StoreEngineOpts {
    fn default() -> Self {
        StoreEngineOpts {
            map_opts: MapStoreEngineOpts::default(),
            value_opts: ValueStoreEngineOpts::default(),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct MapStoreEngineOpts {
    config: StoreDelegateConfig,
}

#[derive(Default, Debug, Clone)]
pub struct ValueStoreEngineOpts {
    config: StoreDelegateConfig,
}

#[derive(Debug)]
pub enum StoreInitialisationError {
    Error(String),
}

pub enum SnapshotError {}

pub trait SwimStore {
    type PlaneStore: PlaneStore;

    fn plane_store<I>(&mut self, path: I) -> Result<Self::PlaneStore, StoreError>
    where
        I: ToString;
}

pub trait Store:
    for<'a> StoreEngine<'a> + FromOpts + RangedSnapshot + Send + Sync + Destroy + 'static
{
}

pub trait Destroy {
    fn destroy(self);
}

pub trait RangedSnapshot {
    type Prefix;

    fn ranged_snapshot<F, K, V>(
        &self,
        prefix: Self::Prefix,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>;
}

pub struct KeyedSnapshot<K, V> {
    data: IntoIter<(K, V)>,
}

impl<K, V> Iterator for KeyedSnapshot<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next()
    }
}

impl<K, V> KeyedSnapshot<K, V> {
    pub fn new(data: IntoIter<(K, V)>) -> Self {
        KeyedSnapshot { data }
    }
}

pub trait Snapshot<K, V>: RangedSnapshot {
    type Snapshot: IntoIterator<Item = (K, V)>;

    fn snapshot(&self) -> Result<Option<Self::Snapshot>, StoreError>;
}

pub trait FromOpts: Sized {
    type Opts;

    fn from_opts<I: AsRef<Path>>(
        path: I,
        opts: &Self::Opts,
    ) -> Result<Self, StoreInitialisationError>;
}

pub trait OwnedStoreEngine: for<'s> StoreEngine<'s> {}
impl<S> OwnedStoreEngine for S where S: for<'s> StoreEngine<'s> {}

pub trait StoreEngine<'i>: 'static {
    type Key: 'i;
    type Value: 'i;
    type Error: Into<StoreError>;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error>;

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error>;

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error>;
}

pub struct ServerStore {
    dir: StoreDir,
    refs: HashMap<String, Weak<PlaneStoreInner>>,
    opts: StoreEngineOpts,
}

impl ServerStore {
    pub fn new(opts: StoreEngineOpts, base_path: PathBuf) -> ServerStore {
        ServerStore {
            dir: StoreDir::persistent(base_path),
            refs: HashMap::new(),
            opts,
        }
    }

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
    use crate::engines::db::lmdbx::LmdbxOpts;
    use crate::engines::db::StoreDelegateConfig;
    use crate::stores::node::NodeStore;
    use crate::stores::plane::PlaneStore;
    use crate::stores::{StoreKey, ValueStorageKey};
    use crate::{
        MapStoreEngineOpts, ServerStore, Snapshot, StoreEngine, StoreEngineOpts, SwimStore,
        ValueStoreEngineOpts,
    };
    use heed::EnvOpenOptions;
    use rocksdb::Options;
    use std::sync::Arc;

    #[test]
    fn simple_put_get() {
        let mut rock_opts = Options::default();
        rock_opts.create_if_missing(true);
        rock_opts.create_missing_column_families(true);

        let server_opts = StoreEngineOpts {
            map_opts: MapStoreEngineOpts {
                config: StoreDelegateConfig::Lmdbx(LmdbxOpts {
                    open_opts: EnvOpenOptions::new(),
                }),
            },
            value_opts: ValueStoreEngineOpts {
                config: StoreDelegateConfig::Rocksdb(rock_opts),
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
        let mut rock_opts = Options::default();
        rock_opts.create_if_missing(true);
        rock_opts.create_missing_column_families(true);

        let server_opts = StoreEngineOpts {
            map_opts: MapStoreEngineOpts {
                config: StoreDelegateConfig::Rocksdb(rock_opts.clone()),
            },
            value_opts: ValueStoreEngineOpts {
                config: StoreDelegateConfig::Rocksdb(rock_opts),
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
