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

use crate::engines::FromKeyspaces;
use crate::stores::lane::serialize;
use crate::StoreError;
use futures::StreamExt;
use rocksdb::MergeOperands;
use std::num::NonZeroUsize;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

/// The type to use for the storing unique lane identifiers.
///
/// Note: It is not recommended to change this after a store has already been initialised.
pub type KeyType = u64;
pub type KeyRequest = (String, oneshot::Sender<KeyType>);

/// Unique lane identifier keyspace. The name is `default` as either the Rust RocksDB crate or
/// Rocks DB itself has an issue in using merge operators under a non-default column family.
///
/// See: https://github.com/rust-rocksdb/rust-rocksdb/issues/29
pub(crate) const LANE_KS: &str = "default";
/// Value lane store keyspace.
pub(crate) const VALUE_LANE_KS: &str = "value_lanes";
/// Map lane store keyspace.
pub(crate) const MAP_LANE_KS: &str = "map_lanes";

/// The lane keyspace's counter key.
pub const COUNTER_KEY: &str = "counter";
const COUNTER_BYTES: &'static [u8] = COUNTER_KEY.as_bytes();
/// The prefix that all lane identifiers in the counter keyspace will be prefixed by.
const LANE_PREFIX: &str = "lane";

const DESERIALIZATION_FAILURE: &str = "Failed to deserialize key";
const SERIALIZATION_FAILURE: &str = "Failed to serialize key";
const INCONSISTENT_KEYSPACE: &str = "Inconsistent keyspace";
const RESPOND_FAILURE: &str = "Failed to send response";

/// The initial value that the lane identifier keyspace will be initialised with if it doesn't
/// already exist.
const INITIAL: KeyType = 0;
const STEP: KeyType = 1;

/// An enumeration over the keyspaces that exist in a store.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum KeyspaceName {
    Lane,
    Value,
    Map,
}

impl AsRef<str> for KeyspaceName {
    fn as_ref(&self) -> &str {
        match self {
            KeyspaceName::Lane => LANE_KS,
            KeyspaceName::Value => VALUE_LANE_KS,
            KeyspaceName::Map => MAP_LANE_KS,
        }
    }
}

/// Keyspace open options. See `KeyspaceDef` and `Keyspaces` for more information.
///
/// If this is changed from the default implementations provided for libmdbx and RocksDB then the
/// merge operators must still be provided.
pub struct KeyspaceOptions<O> {
    pub(crate) lane: O,
    pub(crate) value: O,
    pub(crate) map: O,
}

/// A keyspace definition for persisting logically related data.
///
/// Definitions of a keyspace will depend on the underlying delegate store implementation used to
/// run a store with. For a RocksDB engine this will correspond to a column family and for libmdbx
/// this will correspond to a sub-database that is keyed by `name`.
pub struct KeyspaceDef<O> {
    /// The name of the keyspace.
    pub(crate) name: &'static str,
    /// The configuration options that will be used to open the keyspace.
    pub(crate) opts: O,
}

impl<O> KeyspaceDef<O> {
    fn new(name: &'static str, opts: O) -> Self {
        KeyspaceDef { name, opts }
    }
}

/// A set of keyspace definitions for a store to be opened with.
///
/// A Swim server store requires three keyspaces to operate with:
/// - A lane keyspace keyed by a lane address and a value type of a unique identifier. Keyed by a
/// lane address and has a unique identifier associated with it assigned by the key store task.
/// - A value lane keyspace. Keyed by the lane's unique identifier.
/// - A map lane keyspace. Map keys are prefixed by the lane's unique identifier.
///
/// Each lane on a server has a unique, fixed sized, identifier representing that URI. This key is
/// used to access data with the value and map lane stores and reduces the overhead of keys in other
/// stores. This approach allows for further optimisations in delegate stores due to the fixed size
/// key prefixes.
pub struct Keyspaces<O: FromKeyspaces> {
    pub(crate) lane: KeyspaceDef<O::KeyspaceOpts>,
    pub(crate) value: KeyspaceDef<O::KeyspaceOpts>,
    pub(crate) map: KeyspaceDef<O::KeyspaceOpts>,
}

impl<K, O> From<KeyspaceOptions<O>> for Keyspaces<K>
where
    K: FromKeyspaces<KeyspaceOpts = O>,
{
    fn from(opts: KeyspaceOptions<O>) -> Self {
        let KeyspaceOptions { lane, value, map } = opts;
        Keyspaces {
            lane: KeyspaceDef::new(LANE_KS, lane),
            value: KeyspaceDef::new(VALUE_LANE_KS, value),
            map: KeyspaceDef::new(MAP_LANE_KS, map),
        }
    }
}

/// A task for loading and assigning unique identifiers to lane addresses.
pub struct KeyStoreTask<S: KeyspaceByteEngine> {
    /// The delegate store for fetching and merging the unique identifier from.
    db: Arc<S>,
    /// A stream of incoming requests for the unique identifiers.
    rx: ReceiverStream<KeyRequest>,
}

fn deserialize_key<B: AsRef<[u8]>>(bytes: B) -> Result<KeyType, KeyspaceTaskError> {
    bincode::deserialize::<KeyType>(bytes.as_ref()).map_err(KeyspaceTaskError::Deserialization)
}

#[derive(Debug, Error)]
enum KeyspaceTaskError {
    #[error("Internal store error: {0}")]
    Store(StoreError),
    #[error("An error was produced when deserializing a store key: {0}")]
    Deserialization(Box<bincode::ErrorKind>),
}

impl From<StoreError> for KeyspaceTaskError {
    fn from(e: StoreError) -> Self {
        KeyspaceTaskError::Store(e)
    }
}

pub fn format_key<I: ToString>(uri: I) -> String {
    format!("{}/{}", LANE_PREFIX, uri.to_string())
}

impl<S: KeyspaceByteEngine> KeyStoreTask<S> {
    pub fn new(db: Arc<S>, rx: ReceiverStream<KeyRequest>) -> Self {
        KeyStoreTask { db, rx }
    }

    async fn run(self) -> Result<(), KeyspaceTaskError> {
        let KeyStoreTask { db, rx } = self;
        let mut requests = rx.fuse();

        while let Some((uri, responder)) = requests.next().await {
            let prefixed = format_key(uri);

            match db.get_keyspace(KeyspaceName::Lane, prefixed.as_bytes())? {
                Some(bytes) => {
                    let id = deserialize_key(bytes)?;
                    responder.send(id).expect(RESPOND_FAILURE);
                }
                None => {
                    db.merge_keyspace(KeyspaceName::Lane, COUNTER_BYTES, STEP)?;

                    let counter_bytes = db
                        .get_keyspace(KeyspaceName::Lane, COUNTER_BYTES)?
                        .expect(INCONSISTENT_KEYSPACE);
                    let id = deserialize_key(counter_bytes.as_slice())?;

                    db.put_keyspace(
                        KeyspaceName::Lane,
                        prefixed.as_bytes(),
                        counter_bytes.as_slice(),
                    )?;

                    responder.send(id).expect(RESPOND_FAILURE);
                }
            }
        }

        Ok(())
    }
}

/// A keystore for assigning unique identifiers to lane addresses.
#[derive(Clone)]
pub struct KeyStore {
    tx: mpsc::Sender<KeyRequest>,
    task: Arc<JoinHandle<()>>,
}

pub(crate) fn failing_keystore() -> KeyStore {
    let (tx, _rx) = mpsc::channel(1);
    let task = tokio::spawn(futures::future::ready(()));
    KeyStore {
        tx,
        task: Arc::new(task),
    }
}

impl KeyStore {
    /// Produces a new keystore which will delegate its operations to `db` and will have an internal
    /// task communication buffer size of `buffer_size`.
    pub fn new<S: KeyspaceByteEngine>(db: Arc<S>, buffer_size: NonZeroUsize) -> KeyStore {
        let (tx, rx) = mpsc::channel(buffer_size.get());
        let task = KeyStoreTask::new(db, ReceiverStream::new(rx));
        let task = async move {
            if let Err(e) = task.run().await {
                event!(Level::ERROR, "Keystore failed with: {:?}", e);
            }
        };

        KeyStore {
            tx,
            task: Arc::new(tokio::spawn(task)),
        }
    }

    /// Returns a unique identifier that has been assigned to the `lane_id`. This ID must be a
    /// well-formed String of `/node_uri/lane_uri` for this host.
    pub async fn id_for<I>(&self, lane_id: I) -> KeyType
    where
        I: Into<String>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send((lane_id.into(), tx))
            .await
            .expect("Failed to make lane ID request");
        rx.await.expect("No response received for lane ID")
    }
}

/// An incrementing merge operator for use in Rocks databases in the lane keyspace.

#[allow(clippy::unnecessary_wraps)]
pub(crate) fn incrementing_merge_operator(
    _new_key: &[u8],
    existing_value: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut value = match existing_value {
        Some(bytes) => deserialize_key(bytes).expect(DESERIALIZATION_FAILURE),
        None => INITIAL,
    };

    for op in operands {
        let deserialized = deserialize_key(op).expect(DESERIALIZATION_FAILURE);
        value += deserialized;
    }
    Some(serialize(&value).expect(SERIALIZATION_FAILURE))
}

pub trait KeyspaceByteEngine: Send + Sync + 'static {
    /// Put a key-value pair into the specified keyspace.
    fn put_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError>;

    /// Get an entry from the specified keyspace.
    fn get_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a value from the specified keyspace.
    fn delete_keyspace(&self, keyspace: KeyspaceName, key: &[u8]) -> Result<(), StoreError>;

    /// Perform a merge operation on the specified keyspace and key, incrementing by `step`.
    fn merge_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
        step: KeyType,
    ) -> Result<(), StoreError>;
}

pub trait KeyspaceResolver {
    type ResolvedKeyspace;

    fn resolve_keyspace(&self, space: &KeyspaceName) -> Option<&Self::ResolvedKeyspace>;
}

#[cfg(test)]
mod tests {
    use crate::engines::keyspaces::{format_key, KeyType, KeyspaceName, COUNTER_KEY};
    use crate::engines::keyspaces::{incrementing_merge_operator, KeyStore, LANE_KS, MAP_LANE_KS};
    use crate::stores::lane::deserialize;
    use crate::RocksDatabase;
    use crate::{NodeStore, RocksOpts, ServerStore, SwimNodeStore, SwimPlaneStore, SwimStore};
    use rocksdb::{ColumnFamilyDescriptor, DB};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use tempdir::TempDir;

    fn make_db() -> (TempDir, RocksDatabase) {
        let temp_file = TempDir::new("test").unwrap();
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let map_cf = ColumnFamilyDescriptor::new(MAP_LANE_KS, rocksdb::Options::default());

        let mut lane_opts = rocksdb::Options::default();
        lane_opts.set_merge_operator_associative("lane_id_counter", incrementing_merge_operator);
        let lane_table_cf = ColumnFamilyDescriptor::new(LANE_KS, lane_opts);

        let db = DB::open_cf_descriptors(&db_opts, temp_file.path(), vec![lane_table_cf, map_cf])
            .unwrap();

        (temp_file, RocksDatabase::new(db))
    }

    #[tokio::test]
    async fn keyspace() {
        let (_dir, arc_db) = make_db();
        let key_store = KeyStore::new(Arc::new(arc_db), NonZeroUsize::new(8).unwrap());

        assert_eq!(key_store.id_for("lane_a").await, 1);
        assert_eq!(key_store.id_for("lane_a").await, 1);

        assert_eq!(key_store.id_for("lane_b").await, 2);
        assert_eq!(key_store.id_for("lane_b").await, 2);
    }

    fn assert_counters(
        plane_store: &SwimPlaneStore<RocksDatabase>,
        node_uri: &str,
        lane_uri: &str,
        lane_count_at: KeyType,
        counter_at: KeyType,
    ) {
        let key = format_key(format!("{}/{}", node_uri, lane_uri));
        let lane_id = plane_store
            .get_keyspace(KeyspaceName::Lane, key.as_bytes())
            .unwrap()
            .expect("Missing key");
        assert_eq!(
            deserialize::<KeyType>(lane_id.as_slice()).unwrap(),
            lane_count_at
        );

        let counter = plane_store
            .get_keyspace(KeyspaceName::Lane, COUNTER_KEY.as_bytes())
            .unwrap()
            .expect("Missing counter");
        assert_eq!(
            deserialize::<KeyType>(counter.as_slice()).unwrap(),
            counter_at
        );
    }

    #[tokio::test]
    async fn lane_id() {
        let mut server_store = ServerStore::<RocksDatabase>::transient(
            RocksOpts::default(),
            RocksOpts::keyspace_options(),
            "test",
        );

        let lane_uri = "lane";
        let node_uri = "node";

        let plane_store = server_store.plane_store("plane").unwrap();
        let node_store = SwimNodeStore::new(plane_store.clone(), node_uri);
        let lane_store = node_store.value_lane_store::<_, String>(lane_uri).await;

        assert_eq!(lane_store.lane_id(), 1);

        assert_counters(&plane_store, node_uri, lane_uri, 1, 1);
    }

    #[tokio::test]
    async fn multiple_lane_ids() {
        let mut server_store = ServerStore::<RocksDatabase>::transient(
            RocksOpts::default(),
            RocksOpts::keyspace_options(),
            "test",
        );

        let node_uri = "node";
        let lane_prefix = "lane";

        let plane_store = server_store.plane_store("plane").unwrap();
        let node_store = SwimNodeStore::new(plane_store.clone(), node_uri);
        let mut lane_count = 0;

        for lane_id in 1..=10 {
            lane_count += 1;

            let lane_uri = format!("{}/{}", lane_prefix, lane_id);
            let lane_store = node_store
                .value_lane_store::<_, String>(lane_uri.clone())
                .await;

            assert_eq!(lane_count, lane_store.lane_id());
            assert_counters(
                &plane_store,
                node_uri,
                lane_uri.as_str(),
                lane_count,
                lane_count,
            );
        }
    }

    #[tokio::test]
    async fn multiple_nodes_lanes() {
        let mut server_store = ServerStore::<RocksDatabase>::transient(
            RocksOpts::default(),
            RocksOpts::keyspace_options(),
            "test",
        );

        let node_prefix = "node";
        let lane_prefix = "lane";

        let append = |left, right| format!("{}/{}", left, right);
        let plane_store = server_store.plane_store("plane").unwrap();

        let mut lane_count = 0;

        for node_id in 1..=10 {
            let start_lane_count = lane_count;

            for lane_id in 1..=10 {
                lane_count += 1;
                let node_uri = append(node_prefix, node_id);
                let lane_uri = append(lane_prefix, lane_id);

                let node_store = SwimNodeStore::new(plane_store.clone(), node_uri.clone());
                let lane_store = node_store
                    .value_lane_store::<_, String>(lane_uri.clone())
                    .await;

                assert_eq!(lane_count, lane_store.lane_id());
                assert_counters(
                    &plane_store,
                    node_uri.as_str(),
                    lane_uri.as_str(),
                    lane_count,
                    lane_count,
                );
            }

            // rerun the loop and check that the task returns the same IDs when called again
            for i in 1..=10 {
                let node_uri = append(node_prefix, node_id);
                let lane_uri = append(lane_prefix, i);
                let node_store = SwimNodeStore::new(plane_store.clone(), node_uri);
                let lane_store = node_store.value_lane_store::<_, String>(lane_uri).await;

                let lane_id = start_lane_count + i;
                assert_eq!(lane_id, lane_store.lane_id());
            }
        }
    }
}
