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

use crate::engines::db::StoreDelegate;
use crate::stores::node::{NodeStore, SwimNodeStore};
use crate::stores::{DatabaseStore, MapStorageKey, StoreKey, ValueStorageKey};
use crate::{FromOpts, KeyedSnapshot, RangedSnapshot, StoreEngine, StoreEngineOpts, StoreError};
use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const STORE_DIR: &str = "store";
const PLANES_DIR: &str = "planes";
const MAP_DIR: &str = "map";
const VALUE_DIR: &str = "value";

/// The core of a plane store.
pub struct PlaneStoreInner {
    /// The directory that the map and value stores are located.
    path: PathBuf,
    /// A store for map lanes.
    map_store: DatabaseStore<MapStorageKey>,
    /// A store for value lanes.
    value_store: DatabaseStore<ValueStorageKey>,
}

impl Debug for PlaneStoreInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaneStoreInner")
            .field("path", &self.path.to_string_lossy().to_string())
            .finish()
    }
}

/// Creates paths for both map and value stores with a base path of `base_path` and appended by
/// `plane_name`.
fn paths_for<B, P>(base_path: &B, plane_name: &P) -> (PathBuf, PathBuf)
where
    B: AsRef<OsStr> + ?Sized,
    P: AsRef<OsStr> + ?Sized,
{
    let addr = Path::new(base_path)
        .join(STORE_DIR)
        .join(PLANES_DIR)
        .join(plane_name.as_ref());
    let map_path = addr.join(MAP_DIR);
    let value_path = addr.join(VALUE_DIR);

    (map_path, value_path)
}

/// A trait for defining plane stores which will create node stores.
pub trait PlaneStore: Debug {
    /// The type of node stores which are created.
    type NodeStore: NodeStore;

    /// Create a node store for `node_uri`.
    fn node_store<I>(&self, node_uri: I) -> Self::NodeStore
    where
        I: ToString;
}

impl PlaneStoreInner {
    pub(crate) fn open<B, P>(
        base_path: B,
        plane_name: P,
        opts: &StoreEngineOpts,
    ) -> Result<PlaneStoreInner, StoreError>
    where
        B: AsRef<Path>,
        P: AsRef<Path>,
    {
        let StoreEngineOpts {
            map_opts,
            value_opts,
        } = opts;

        let (map_path, value_path) = paths_for(base_path.as_ref(), plane_name.as_ref());
        let map_store = StoreDelegate::from_opts(&map_path, &map_opts.config)?;
        let value_store = StoreDelegate::from_opts(&value_path, &value_opts.config)?;

        Ok(PlaneStoreInner {
            path: plane_name.as_ref().into(),
            map_store: DatabaseStore::new(map_store),
            value_store: DatabaseStore::new(value_store),
        })
    }
}

/// A store engine for planes.
///
/// Backed by a value store and a map store, any operations on this store have their key variant
/// checked and the operation is delegated to the corresponding store.
#[derive(Clone, Debug)]
pub struct SwimPlaneStore {
    inner: Arc<PlaneStoreInner>,
}

impl PlaneStore for SwimPlaneStore {
    type NodeStore = SwimNodeStore;

    fn node_store<I>(&self, node: I) -> Self::NodeStore
    where
        I: ToString,
    {
        SwimNodeStore::new(self.clone(), node.to_string())
    }
}

impl SwimPlaneStore {
    /// Creates a new plane store.
    ///
    /// # Arguments
    /// `path`: the directory that `map_store` and `value_store` are located within.
    /// `map_store`: a store for delegating map lane store engine operations to.
    /// `value_store`: a store for delegating value lane store engine operations to.
    pub fn new(
        path: PathBuf,
        map_store: DatabaseStore<MapStorageKey>,
        value_store: DatabaseStore<ValueStorageKey>,
    ) -> Self {
        SwimPlaneStore {
            inner: Arc::new(PlaneStoreInner {
                path,
                map_store,
                value_store,
            }),
        }
    }

    pub fn from_inner(inner: Arc<PlaneStoreInner>) -> SwimPlaneStore {
        SwimPlaneStore { inner }
    }
}

impl<'a> StoreEngine<'a> for SwimPlaneStore {
    type Key = StoreKey;
    type Value = Vec<u8>;
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let PlaneStoreInner {
            map_store,
            value_store,
            ..
        } = &*self.inner;

        match key {
            StoreKey::Map(key) => map_store.put(&key, value),
            StoreKey::Value(key) => value_store.put(&key, value),
        }
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        let PlaneStoreInner {
            map_store,
            value_store,
            ..
        } = &*self.inner;
        match key {
            StoreKey::Map(key) => map_store.get(&key),
            StoreKey::Value(key) => value_store.get(&key),
        }
    }

    fn delete(&self, key: Self::Key) -> Result<(), Self::Error> {
        let PlaneStoreInner {
            map_store,
            value_store,
            ..
        } = &*self.inner;
        match key {
            StoreKey::Map(key) => map_store.delete(&key),
            StoreKey::Value(key) => value_store.delete(&key),
        }
    }
}

impl RangedSnapshot for SwimPlaneStore {
    type Prefix = StoreKey;

    fn ranged_snapshot<F, K, V>(
        &self,
        prefix: Self::Prefix,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        match prefix {
            p @ StoreKey::Map(..) => self.inner.map_store.ranged_snapshot(p, map_fn),
            p @ StoreKey::Value(..) => self.inner.value_store.ranged_snapshot(p, map_fn),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::db::rocks::RocksDatabase;
    use crate::stores::plane::SwimPlaneStore;
    use crate::stores::{DatabaseStore, StoreKey, ValueStorageKey};
    use crate::StoreEngine;
    use rocksdb::{Options, DB};
    use std::sync::Arc;

    #[test]
    fn t() {
        let map_path = "__test_rocks_db_map";
        let map_db = DB::open_default(map_path).unwrap();

        let value_path = "__test_rocks_db_value";
        let value_db = DB::open_default(value_path).unwrap();

        {
            let map_delegate = RocksDatabase::new(map_db);
            let map_store = DatabaseStore::new(map_delegate);

            let value_delegate = RocksDatabase::new(value_db);
            let value_store = DatabaseStore::new(value_delegate);

            let plane_store = SwimPlaneStore::new("test".into(), map_store, value_store);

            let value_key = StoreKey::Value(ValueStorageKey {
                node_uri: Arc::new("/node".to_string()),
                lane_uri: Arc::new("/lane".to_string()),
            });

            assert!(plane_store.put(value_key.clone(), b"test".to_vec()).is_ok());

            let result = plane_store.get(value_key);
            let vec = result.unwrap().unwrap();
            println!("{:?}", String::from_utf8(vec));
        }

        assert!(DB::destroy(&Options::default(), map_path).is_ok());
        assert!(DB::destroy(&Options::default(), value_path).is_ok());
    }
}
