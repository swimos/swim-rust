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

pub mod keystore;

use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt, Stream};

use store::keyspaces::{KeyType, KeyspaceByteEngine, Keyspaces};
use store::{serialize, KeyedSnapshot, Store, StoreError, StoreInfo};
use swim_common::model::text::Text;

use crate::agent::store::{NodeStore, SwimNodeStore};
use crate::plane::store::keystore::{KeyRequest, KeyStore, KeystoreTask, PlaneStoreTask};
use crate::store::{KeyspaceName, StoreEngine, StoreKey};

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
    Self: StoreEngine + KeystoreTask + Sized + Debug + Send + Sync + Clone + 'static,
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

    /// Returns information about the delegate store
    fn store_info(&self) -> StoreInfo;

    fn lane_id_of<I>(&self, lane: I) -> BoxFuture<KeyType>
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

impl<D: Store> SwimPlaneStore<D> {
    #[cfg(test)]
    pub(crate) fn get_keyspace(
        &self,
        space: KeyspaceName,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        self.delegate.get_keyspace(space, key)
    }
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

fn exec_keyspace<F, O>(key: StoreKey, f: F) -> Result<O, StoreError>
where
    F: Fn(KeyspaceName, Vec<u8>) -> Result<O, StoreError>,
{
    match key {
        s @ StoreKey::Map { .. } => f(KeyspaceName::Map, serialize(&s)?),
        s @ StoreKey::Value { .. } => f(KeyspaceName::Value, serialize(&s)?),
    }
}

impl<D> KeystoreTask for SwimPlaneStore<D> {
    fn run<DB, S>(db: Arc<DB>, events: S) -> BoxFuture<'static, Result<(), StoreError>>
    where
        DB: KeyspaceByteEngine,
        S: Stream<Item = KeyRequest> + Unpin + Send + 'static,
    {
        PlaneStoreTask::run(db, events)
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
        let namespace = match &prefix {
            StoreKey::Map { .. } => KeyspaceName::Map,
            StoreKey::Value { .. } => KeyspaceName::Value,
        };

        self.delegate
            .load_ranged_snapshot(namespace, serialize(&prefix)?.as_slice(), map_fn)
    }

    fn store_info(&self) -> StoreInfo {
        self.delegate.store_info()
    }

    fn lane_id_of<I>(&self, lane: I) -> BoxFuture<KeyType>
    where
        I: Into<String>,
    {
        self.keystore.id_for(lane.into()).boxed()
    }
}

impl<D: Store> StoreEngine for SwimPlaneStore<D> {
    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        exec_keyspace(key, |namespace, bytes| {
            self.delegate
                .put_keyspace(namespace, bytes.as_slice(), value)
        })
    }

    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        exec_keyspace(key, |namespace, bytes| {
            self.delegate.get_keyspace(namespace, bytes.as_slice())
        })
    }

    fn delete(&self, key: StoreKey) -> Result<(), StoreError> {
        exec_keyspace(key, |namespace, bytes| {
            self.delegate.delete_keyspace(namespace, bytes.as_slice())
        })
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
        let plane_name = match plane_name.as_ref().to_str() {
            Some(path) => path.to_string(),
            None => {
                return Err(StoreError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Expected a valid UTF-8 path",
                )));
            }
        };

        let arcd_delegate = Arc::new(delegate);
        // todo config
        let keystore =
            KeyStore::new::<D, Self>(arcd_delegate.clone(), NonZeroUsize::new(8).unwrap());

        Ok(Self::new(plane_name, arcd_delegate, keystore))
    }
}
