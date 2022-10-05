// Copyright 2015-2021 Swim Inc.
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

use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use swim_model::Text;
use swim_store::{EngineInfo, PrefixRangeByteEngine, RangeConsumer, Store, StoreError};

use crate::agent::{NodeStore, SwimNodeStore};
use crate::server::keystore::KeyStore;
use crate::server::{KeyspaceName, StoreEngine, StoreKey};
use swim_store::{Keyspaces, StoreBuilder};

pub mod mock;

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

pub trait PrefixPlaneStore<'a> {
    type RangeCon: RangeConsumer + Send + 'a;

    /// Executes a ranged snapshot read prefixed by a lane key.
    ///
    /// #Arguments
    /// * `prefix` - Common prefix for the records to read.
    fn ranged_snapshot_consumer(&'a self, prefix: StoreKey) -> Result<Self::RangeCon, StoreError>;
}

/// A trait for defining plane stores which will create node stores.
pub trait PlaneStore:
    for<'a> PrefixPlaneStore<'a> + StoreEngine + Sized + Debug + Send + Sync + Clone + 'static
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
    fn get_prefix_range<F, K, V>(
        &self,
        prefix: StoreKey,
        map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>;

    /// Returns information about the delegate store
    fn engine_info(&self) -> EngineInfo;

    fn node_id_of<I>(&self, node: I) -> Result<u64, StoreError>
    where
        I: Into<String>;
}

/// A store engine for planes.
pub struct SwimPlaneStore<D> {
    /// The name of the plane.
    plane_name: Text,
    /// Delegate byte engine.
    delegate: Arc<D>,
    keystore: KeyStore<D>,
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
        s @ StoreKey::Map { .. } => f(KeyspaceName::Map, s.serialize_as_bytes()),
        s @ StoreKey::Value { .. } => f(KeyspaceName::Value, s.serialize_as_bytes()),
    }
}

impl<'a, D> PrefixPlaneStore<'a> for SwimPlaneStore<D>
where
    D: Store,
{
    type RangeCon = <D as PrefixRangeByteEngine<'a>>::RangeCon;

    fn ranged_snapshot_consumer(&'a self, prefix: StoreKey) -> Result<Self::RangeCon, StoreError> {
        let namespace = match &prefix {
            StoreKey::Map { .. } => KeyspaceName::Map,
            StoreKey::Value { .. } => KeyspaceName::Value,
        };

        self.delegate
            .get_prefix_range_consumer(namespace, prefix.serialize_as_bytes().as_slice())
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

    fn get_prefix_range<F, K, V>(
        &self,
        prefix: StoreKey,
        map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let namespace = match &prefix {
            StoreKey::Map { .. } => KeyspaceName::Map,
            StoreKey::Value { .. } => KeyspaceName::Value,
        };

        self.delegate
            .get_prefix_range(namespace, prefix.serialize_as_bytes().as_slice(), map_fn)
    }

    fn engine_info(&self) -> EngineInfo {
        self.delegate.engine_info()
    }

    fn node_id_of<I>(&self, lane: I) -> Result<u64, StoreError>
    where
        I: Into<String>,
    {
        self.keystore.id_for(lane.into())
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

pub fn open_plane<B, P, D>(
    base_path: B,
    plane_name: P,
    builder: D,
    keyspaces: Keyspaces<D>,
) -> Result<SwimPlaneStore<D::Store>, StoreError>
where
    B: AsRef<Path>,
    P: AsRef<Path>,
    D: StoreBuilder,
{
    let path = path_for(base_path.as_ref(), plane_name.as_ref());
    let delegate = builder.build(path, &keyspaces)?;
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
    let keystore = KeyStore::initialise_with(arcd_delegate.clone());

    Ok(SwimPlaneStore::new(plane_name, arcd_delegate, keystore))
}

impl<D> SwimPlaneStore<D>
where
    D: Store,
{
    pub fn new<I: Into<Text>>(
        plane_name: I,
        delegate: Arc<D>,
        keystore: KeyStore<D>,
    ) -> SwimPlaneStore<D> {
        SwimPlaneStore {
            plane_name: plane_name.into(),
            delegate,
            keystore,
        }
    }
}
