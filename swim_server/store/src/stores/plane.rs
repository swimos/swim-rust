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

use crate::engines::keyspaces::{KeyStore, Keyspaces};
use crate::engines::KeyedSnapshot;
use crate::stores::lane::serialize;
use crate::stores::node::{NodeStore, SwimNodeStore};
use crate::stores::StoreKey;
use crate::{Store, StoreError, StoreInfo};
use futures::future::BoxFuture;
use futures::FutureExt;
use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
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
    Self: Sized + Debug + Send + Sync + Clone + 'static,
{
    /// The type of node stores which are created.
    type NodeStore: NodeStore;

    /// Create a node store for `node_uri`.
    fn node_store<I>(&self, node_uri: I) -> Self::NodeStore
    where
        I: Into<Text>;

    /// Executes a ranged snapshot read prefixed by a lane key and deserialize each key-value pair
    /// using `map_fn`.
    ///
    /// Returns an optional snapshot iterator if entries were found that will yield deserialized
    /// key-value pairs.
    fn load_ranged_snapshot<F, K, V>(
        &self,
        prefix: StoreKey,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>;

    /// Serialize `key` and insert the key-value pair into the delegate store.
    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError>;

    /// Gets the value associated with the provided store key if it exists.
    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete the key-value pair associated with `key`.
    fn delete(&self, key: StoreKey) -> Result<(), StoreError>;

    /// Returns information about the delegate store
    fn store_info(&self) -> StoreInfo;

    fn id_for<I>(&self, lane_id: I) -> BoxFuture<u64>
    where
        I: Into<String>;
}

/// A store engine for planes.
///
/// Backed by a value store and a map store, any operations on this store have their key variant
/// checked and the operation is delegated to the corresponding store.
pub struct SwimPlaneStore<D> {
    /// The name of the plane.
    plane_name: Text,
    /// Delegate byte engine.
    delegate: Arc<D>,
    keystore: KeyStore,
}

impl<D> Clone for SwimPlaneStore<D> {
    fn clone(&self) -> Self {
        SwimPlaneStore {
            plane_name: self.plane_name.clone(),
            delegate: self.delegate.clone(),
            keystore: self.keystore.clone(),
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
            .load_ranged_snapshot(serialize(&prefix)?.as_slice(), map_fn)
    }

    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        self.delegate.put(serialize(&key)?.as_slice(), value)
    }

    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        self.delegate.get(serialize(&key)?.as_slice())
    }

    fn delete(&self, key: StoreKey) -> Result<(), StoreError> {
        self.delegate.delete(serialize(&key)?.as_slice())
    }

    fn store_info(&self) -> StoreInfo {
        self.delegate.store_info()
    }

    fn id_for<I>(&self, lane_id: I) -> BoxFuture<u64>
    where
        I: Into<String>,
    {
        self.keystore.id_for(lane_id.into()).boxed()
    }
}

impl<D> SwimPlaneStore<D>
where
    D: Store,
{
    pub(crate) fn new<I: Into<Text>>(
        plane_name: I,
        delegate: Arc<D>,
        keystore: KeyStore,
    ) -> SwimPlaneStore<D> {
        SwimPlaneStore {
            plane_name: plane_name.into(),
            delegate,
            keystore,
        }
    }

    pub(crate) fn open<B, P>(
        base_path: B,
        plane_name: P,
        db_opts: &D::Opts,
        keyspaces: &Keyspaces<D>,
    ) -> Result<SwimPlaneStore<D>, StoreError>
    where
        B: AsRef<Path>,
        P: AsRef<Path>,
    {
        let path = path_for(base_path.as_ref(), plane_name.as_ref());
        let delegate = D::from_keyspaces(&path, db_opts, keyspaces)?;
        let plane_name = plane_name
            .as_ref()
            .to_str()
            .expect("Expected valid UTF-8")
            .to_string();

        let arcd_delegate = Arc::new(delegate);
        // todo config
        let keystore = KeyStore::new(arcd_delegate.clone(), NonZeroUsize::new(8).unwrap());

        Ok(Self::new(plane_name, arcd_delegate, keystore))
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::keyspaces::failing_keystore;
    use crate::engines::RocksDatabase;
    use crate::stores::plane::SwimPlaneStore;
    use crate::stores::StoreKey;
    use crate::PlaneStore;
    use rocksdb::{Options, DB};
    use std::sync::Arc;

    #[test]
    fn put_get() {
        let path = "__test_rocks_db_map";
        let db = DB::open_default(path).unwrap();

        {
            let delegate = RocksDatabase::new(db);

            let plane_store = SwimPlaneStore::new("test", Arc::new(delegate), failing_keystore());
            let value_key = StoreKey::Value { lane_id: 0 };
            let test_data = "test";

            assert!(plane_store
                .put(value_key.clone(), test_data.as_bytes())
                .is_ok());

            let result = plane_store.get(value_key);
            let vec = result.unwrap().unwrap();
            assert_eq!(Ok(test_data.to_string()), String::from_utf8(vec));
        }

        assert!(DB::destroy(&Options::default(), path).is_ok());
    }
}
