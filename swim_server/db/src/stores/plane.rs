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

use crate::engines::StoreDelegate;
use crate::stores::{DatabaseStore, MapStorageKey, StoreKey, StoreSnapshot, ValueStorageKey};
use crate::{
    Destroy, FromOpts, Snapshot, Store, StoreEngine, StoreEngineOpts, StoreError,
    StoreInitialisationError,
};
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct PlaneStoreInner {
    path: PathBuf,
    map_store: DatabaseStore<MapStorageKey>,
    value_store: DatabaseStore<ValueStorageKey>,
}

impl Debug for PlaneStoreInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaneStoreInner")
            .field("path", &self.path.to_string_lossy().to_string())
            .finish()
    }
}

impl PlaneStoreInner {
    pub(crate) fn open<I: AsRef<Path>>(
        addr: I,
        opts: &StoreEngineOpts,
    ) -> Result<PlaneStoreInner, StoreInitialisationError> {
        let StoreEngineOpts {
            map_opts,
            value_opts,
            base_path,
        } = opts;

        let addr = Path::new(base_path).join("store").join("planes").join(addr);
        let map_path = addr.join("map");
        let value_path = addr.join("value");

        let map_store = StoreDelegate::from_opts(&map_path, &map_opts.config)?;
        let value_store = StoreDelegate::from_opts(&value_path, &value_opts.config)?;

        Ok(PlaneStoreInner {
            path: addr.into(),
            map_store: DatabaseStore::new(map_store),
            value_store: DatabaseStore::new(value_store),
        })
    }
}

#[derive(Clone, Debug)]
pub struct PlaneStore {
    inner: Arc<PlaneStoreInner>,
}

impl PlaneStore {
    pub fn new(
        path: PathBuf,
        map_store: DatabaseStore<MapStorageKey>,
        value_store: DatabaseStore<ValueStorageKey>,
    ) -> Self {
        PlaneStore {
            inner: Arc::new(PlaneStoreInner {
                path,
                map_store,
                value_store,
            }),
        }
    }

    pub fn from_inner(inner: Arc<PlaneStoreInner>) -> PlaneStore {
        PlaneStore { inner }
    }
}

impl<'a> StoreEngine<'a> for PlaneStore {
    type Key = &'a StoreKey;
    type Value = &'a [u8];
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let PlaneStoreInner {
            map_store,
            value_store,
            ..
        } = &*self.inner;
        match key {
            StoreKey::Map(key) => map_store.put(key, value),
            StoreKey::Value(key) => value_store.put(key, value),
        }
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        let PlaneStoreInner {
            map_store,
            value_store,
            ..
        } = &*self.inner;
        match key {
            StoreKey::Map(key) => map_store.get(key),
            StoreKey::Value(key) => value_store.get(key),
        }
    }

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        let PlaneStoreInner {
            map_store,
            value_store,
            ..
        } = &*self.inner;
        match key {
            StoreKey::Map(key) => map_store.delete(key),
            StoreKey::Value(key) => value_store.delete(key),
        }
    }
}

impl Store for PlaneStore {
    fn path(&self) -> String {
        unimplemented!()
    }
}

impl FromOpts for PlaneStore {
    type Opts = StoreEngineOpts;

    fn from_opts<I: AsRef<Path>>(
        path: I,
        opts: &Self::Opts,
    ) -> Result<Self, StoreInitialisationError> {
        let inner = PlaneStoreInner::open(path, opts)?;
        Ok(PlaneStore {
            inner: Arc::new(inner),
        })
    }
}

impl Destroy for PlaneStore {
    fn destroy(self) {
        unimplemented!()
    }
}

impl Snapshot for PlaneStore {
    type Snapshot = StoreSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::rocks::RocksDatabase;
    use crate::stores::plane::PlaneStore;
    use crate::stores::{DatabaseStore, StoreKey, ValueStorageKey};
    use crate::StoreEngine;
    use rocksdb::{Options, DB};

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

            let plane_store = PlaneStore::new("test".into(), map_store, value_store);

            let value_key = StoreKey::Value(ValueStorageKey {
                node_uri: "/node".into(),
                lane_uri: "/lane".into(),
            });

            assert!(plane_store.put(&value_key, b"test").is_ok());

            let result = plane_store.get(&value_key);
            let vec = result.unwrap().unwrap();
            println!("{:?}", String::from_utf8(vec));
        }

        assert!(DB::destroy(&Options::default(), map_path).is_ok());
        assert!(DB::destroy(&Options::default(), value_path).is_ok());
    }
}
