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

use crate::engines::StoreDelegateConfig;
use crate::stores::plane::{PlaneStore, PlaneStoreInner, SwimPlaneStore};
use bincode::Error;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

pub mod engines;
pub mod stores;

#[derive(Debug)]
pub enum StoreError {
    Error(String),
}

impl From<bincode::Error> for StoreError {
    fn from(_: Error) -> Self {
        unimplemented!()
    }
}

impl From<StoreInitialisationError> for StoreError {
    fn from(e: StoreInitialisationError) -> Self {
        StoreError::Error(format!("{:?}", e))
    }
}

#[derive(Debug, Clone)]
pub struct StoreEngineOpts {
    base_path: PathBuf,
    map_opts: MapStoreEngineOpts,
    value_opts: ValueStoreEngineOpts,
}

#[derive(Debug, Clone)]
pub struct MapStoreEngineOpts {
    config: StoreDelegateConfig,
}

#[derive(Debug, Clone)]
pub struct ValueStoreEngineOpts {
    config: StoreDelegateConfig,
}

#[derive(Debug)]
pub enum StoreInitialisationError {
    Error(String),
}

pub enum SnapshotError {}

pub trait SwimStore {
    type PlaneStore: for<'a> PlaneStore<'a>;

    fn plane_store<I>(&mut self, path: I) -> Result<Self::PlaneStore, StoreError>
    where
        I: ToString;
}

pub trait Store:
    for<'a> StoreEngine<'a> + FromOpts + for<'a> Snapshot<'a> + Send + Sync + Destroy + 'static
{
    fn path(&self) -> String;
}

pub trait Destroy {
    fn destroy(self);
}

pub trait Snapshot<'a>
where
    Self: Send + Sync + Sized,
{
    type Snapshot: Iterable;

    fn snapshot(&'a self) -> Self::Snapshot;
}

pub trait Iterable {
    type Iterator: Iterator;
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
    refs: HashMap<String, Weak<PlaneStoreInner>>,
    opts: StoreEngineOpts,
}

impl ServerStore {
    pub fn new(opts: StoreEngineOpts) -> ServerStore {
        ServerStore {
            refs: HashMap::new(),
            opts,
        }
    }
}

impl SwimStore for ServerStore {
    type PlaneStore = SwimPlaneStore;

    fn plane_store<I: ToString>(&mut self, path: I) -> Result<Self::PlaneStore, StoreError> {
        let ServerStore { refs, opts } = self;
        let path = path.to_string();

        let store = match refs.get(&path) {
            Some(store) => match store.upgrade() {
                Some(store) => return Ok(SwimPlaneStore::from_inner(store)),
                None => Arc::new(PlaneStoreInner::open(&path, opts)?),
            },
            None => Arc::new(PlaneStoreInner::open(&path, opts)?),
        };

        let weak = Arc::downgrade(&store);
        refs.insert(path, weak);
        Ok(SwimPlaneStore::from_inner(store))
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::lmdbx::LmdbxOpts;
    use crate::engines::StoreDelegateConfig;
    use crate::stores::lane::map::MapStore;
    use crate::stores::lane::value::{ValueLaneStore, ValueStore};
    use crate::stores::node::NodeStore;
    use crate::stores::plane::PlaneStore;
    use crate::stores::{StoreKey, ValueStorageKey};
    use crate::{
        MapStoreEngineOpts, ServerStore, StoreEngine, StoreEngineOpts, SwimStore,
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
            base_path: "target".into(),
            map_opts: MapStoreEngineOpts {
                config: StoreDelegateConfig::Lmdbx(LmdbxOpts {
                    open_opts: EnvOpenOptions::new(),
                }),
            },
            value_opts: ValueStoreEngineOpts {
                config: StoreDelegateConfig::Rocksdb(rock_opts),
            },
        };

        let mut store = ServerStore::new(server_opts);
        let plane_store = store.plane_store("unit").unwrap();

        let node_key = StoreKey::Value(ValueStorageKey {
            node_uri: Arc::new("node".to_string()),
            lane_uri: Arc::new("lane".to_string()),
        });

        assert!(plane_store.put(node_key.clone(), b"test".to_vec()).is_ok());
        let value = plane_store.get(node_key).unwrap().unwrap();
        println!("{:?}", String::from_utf8(value));
    }

    #[test]
    fn lane() {
        let mut rock_opts = Options::default();
        rock_opts.create_if_missing(true);
        rock_opts.create_missing_column_families(true);

        let server_opts = StoreEngineOpts {
            base_path: "target".into(),
            map_opts: MapStoreEngineOpts {
                config: StoreDelegateConfig::Lmdbx(LmdbxOpts {
                    open_opts: EnvOpenOptions::new(),
                }),
            },
            value_opts: ValueStoreEngineOpts {
                config: StoreDelegateConfig::Rocksdb(rock_opts),
            },
        };

        let mut store = ServerStore::new(server_opts);
        let plane_store = store.plane_store("unit").unwrap();
        let node_store = plane_store.node_store("node");
        let map_store = node_store.map_lane_store("map");

        assert!(map_store.put(&"a".to_string(), &"a".to_string()).is_ok());
        let val = map_store.get(&"a".to_string());
        println!("{:?}", val);

        let value_store1 = node_store.value_lane_store("value");
        assert!(value_store1.store(&"a".to_string()).is_ok());
        let val = value_store1.load();
        println!("{:?}", val);

        let value_store2: ValueLaneStore<String> = node_store.value_lane_store("value");
        let val = value_store2.load();
        println!("{:?}", val);

        let value_store3: ValueLaneStore<String> = node_store.value_lane_store("value2");
        // assert!(value_store3.store(&"a".to_string()).is_ok());
        let val = value_store3.load();
        println!("{:?}", val);
    }
}
