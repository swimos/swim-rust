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

use crate::stores::lane::serialize;
use crate::stores::node::{NodeStore, SwimNodeStore};
use crate::stores::StoreKey;
use crate::{KeyedSnapshot, Store, StoreError};
use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use swim_common::model::text::Text;

const STORE_DIR: &str = "store";
const PLANES_DIR: &str = "planes";

/// Creates paths for both map and value stores with a base path of `base_path` and appended by
/// `plane_name`.
fn path_for<B, P>(base_path: &B, plane_name: &P) -> PathBuf
where
    B: AsRef<OsStr> + ?Sized,
    P: AsRef<OsStr> + ?Sized,
{
    Path::new(base_path)
        .join(STORE_DIR)
        .join(PLANES_DIR)
        .join(plane_name.as_ref())
}

/// A trait for defining plane stores which will create node stores.
pub trait PlaneStore
where
    Self: Sized + Debug + Send + Sync + 'static,
{
    /// The type of node stores which are created.
    type NodeStore: NodeStore;

    /// Create a node store for `node_uri`.
    fn node_store<I>(&self, node_uri: I) -> Self::NodeStore
    where
        I: Into<Text>;

    fn load_ranged_snapshot<F, K, V>(
        &self,
        _prefix: StoreKey,
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>;

    fn put(&self, key: StoreKey<'_, '_>, value: Vec<u8>) -> Result<(), StoreError>;

    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError>;

    fn delete(&self, key: StoreKey) -> Result<(), StoreError>;
}

/// A store engine for planes.
///
/// Backed by a value store and a map store, any operations on this store have their key variant
/// checked and the operation is delegated to the corresponding store.
pub struct SwimPlaneStore<D> {
    /// The name of the plane.
    plane_name: Text,
    delegate: Arc<D>,
}

impl<D> Clone for SwimPlaneStore<D> {
    fn clone(&self) -> Self {
        SwimPlaneStore {
            plane_name: self.plane_name.clone(),
            delegate: self.delegate.clone(),
        }
    }
}

impl<D> Debug for SwimPlaneStore<D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwimPlaneStore")
            .field("plane_name", &self.plane_name)
            .finish()
    }
}

impl<D> PlaneStore for SwimPlaneStore<D>
where
    D: Store,
{
    type NodeStore = SwimNodeStore<Self>;

    fn node_store<I>(&self, node: I) -> Self::NodeStore
    where
        I: Into<Text>,
    {
        SwimNodeStore::new(self.clone(), node)
    }

    fn load_ranged_snapshot<F, K, V>(
        &self,
        prefix: StoreKey,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        self.delegate
            .load_ranged_snapshot(serialize(&prefix)?, map_fn)
    }

    fn put(&self, key: StoreKey, value: Vec<u8>) -> Result<(), StoreError> {
        self.delegate.put(serialize(&key)?, value)
    }

    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        self.delegate.get(serialize(&key)?)
    }

    fn delete(&self, key: StoreKey) -> Result<(), StoreError> {
        self.delegate.delete(serialize(&key)?)
    }
}

impl<D> SwimPlaneStore<D>
where
    D: Store,
{
    pub(crate) fn new<I: Into<Text>>(plane_name: I, delegate: D) -> SwimPlaneStore<D> {
        SwimPlaneStore {
            plane_name: plane_name.into(),
            delegate: Arc::new(delegate),
        }
    }

    pub(crate) fn open<B, P>(
        base_path: B,
        plane_name: P,
        opts: &D::Opts,
    ) -> Result<SwimPlaneStore<D>, StoreError>
    where
        B: AsRef<Path>,
        P: AsRef<Path>,
    {
        let path = path_for(base_path.as_ref(), plane_name.as_ref());
        let delegate = D::from_opts(&path, &opts)?;
        let plane_name = plane_name
            .as_ref()
            .to_str()
            .expect("Expected valid UTF-8")
            .to_string();

        Ok(Self::new(plane_name, delegate))
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::RocksDatabase;
    use crate::stores::plane::SwimPlaneStore;
    use crate::stores::{StoreKey, ValueStorageKey};
    use crate::PlaneStore;
    use rocksdb::{Options, DB};
    use std::borrow::Cow;

    #[test]
    fn put_get() {
        let path = "__test_rocks_db_map";
        let db = DB::open_default(path).unwrap();

        {
            let delegate = RocksDatabase::new(db);

            let plane_store = SwimPlaneStore::new("test", delegate);

            let value_key = StoreKey::Value(ValueStorageKey {
                node_uri: Cow::Owned("/node".into()),
                lane_uri: Cow::Owned("/lane".into()),
            });

            let test_data = "test";

            assert!(plane_store
                .put(value_key.clone(), test_data.as_bytes().to_vec())
                .is_ok());

            let result = plane_store.get(value_key);
            let vec = result.unwrap().unwrap();
            assert_eq!(Ok(test_data.to_string()), String::from_utf8(vec));
        }

        assert!(DB::destroy(&Options::default(), path).is_ok());
    }

    // #[test]
    // fn directories() {
    //     let plane_name = "test_plane";
    //     let temp_dir = TempDir::new("test").expect("Failed to create temporary directory");
    //
    //     let plane_store =
    //         SwimPlaneStore::open(temp_dir.path(), plane_name, &StoreEngineOpts::default())
    //             .expect("Failed to create plane store");
    //
    //     let SwimPlaneStore {
    //         plane_name: inner_plane_name,
    //         delegate,
    //     } = plane_store;
    //
    //     assert_eq!(*inner_plane_name, plane_name);
    //
    //     // let map_dir = delegate.path();
    //     // assert!(map_dir.ends_with("map"));
    // }
}
