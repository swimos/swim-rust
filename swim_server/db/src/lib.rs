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
use crate::stores::plane::{PlaneStore, PlaneStoreInner};
use bincode::Error;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

pub mod engines;
pub mod factory;
pub mod mock;
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
    type PlaneStore: Store;

    // todo replace I with AsRef<Path>?
    fn plane_store<I: ToString>(&mut self, path: I) -> Result<Self::PlaneStore, StoreError>;
}

pub trait Store: for<'a> StoreEngine<'a> + FromOpts + Snapshot + Send + Sync + Destroy {
    // type Opts: From<>
    //
    // fn from_opts(Self::Opts)->Result<Self, StoreInitialisationError>;

    fn path(&self) -> String;
}

pub trait Destroy {
    fn destroy(self);
}

pub trait Snapshot
where
    Self: Send + Sync + Sized,
{
    type Snapshot: Iterable;

    fn snapshot(&self) -> Self::Snapshot;
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

pub trait StoreEngine<'a> {
    type Key: 'a;
    type Value: 'a;
    type Error: Into<StoreError>;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error>;

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error>;

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error>;
}

pub struct ServerStore {
    inner: HashMap<String, Weak<PlaneStoreInner>>,
    opts: StoreEngineOpts,
}

impl ServerStore {
    pub fn new(opts: StoreEngineOpts) -> ServerStore {
        ServerStore {
            inner: HashMap::new(),
            opts,
        }
    }
}

impl SwimStore for ServerStore {
    type PlaneStore = PlaneStore;

    fn plane_store<I: ToString>(&mut self, path: I) -> Result<Self::PlaneStore, StoreError> {
        let ServerStore { inner, opts } = self;
        let path = path.to_string();

        let store = match inner.get(&path) {
            Some(store) => match store.upgrade() {
                Some(store) => return Ok(PlaneStore::from_inner(store)),
                None => Arc::new(PlaneStoreInner::open(&path, opts)?),
            },
            None => Arc::new(PlaneStoreInner::open(&path, opts)?),
        };

        let weak = Arc::downgrade(&store);
        inner.insert(path, weak);
        Ok(PlaneStore::from_inner(store))
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::lmdbx::LmdbxOpts;
    use crate::engines::StoreDelegateConfig;
    use crate::stores::{StoreKey, ValueStorageKey};
    use crate::{
        MapStoreEngineOpts, ServerStore, StoreEngine, StoreEngineOpts, SwimStore,
        ValueStoreEngineOpts,
    };
    use heed::EnvOpenOptions;
    use rocksdb::Options;

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
            node_uri: "node".to_string(),
            lane_uri: "lane".to_string(),
        });

        assert!(plane_store.put(&node_key, b"test").is_ok());
        let value = plane_store.get(&node_key).unwrap().unwrap();
        println!("{:?}", String::from_utf8(value));
    }
}
