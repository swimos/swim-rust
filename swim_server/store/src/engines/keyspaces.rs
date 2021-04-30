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

use crate::engines::{FromKeyspaces, StoreOpts};
use crate::stores::lane::{deserialize, serialize};
use crate::StoreError;
use futures::StreamExt;
use rocksdb::MergeOperands;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

pub type KeySize = u64;
pub type KeyRequest = (String, oneshot::Sender<KeySize>);

pub(crate) const LANE_KS: &str = "default";
pub(crate) const VALUE_LANE_KS: &str = "value_lanes";
pub(crate) const MAP_LANE_KS: &str = "map_lanes";
const COUNTER_KEY: &str = "counter";
const LANE_PREFIX: &str = "lane/";

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

pub struct KeyspaceOptions<O> {
    lane: O,
    value: O,
    map: O,
}

impl<O> Default for KeyspaceOptions<O>
where
    O: StoreOpts,
{
    fn default() -> Self {
        KeyspaceOptions {
            lane: O::default(),
            value: O::default(),
            map: O::default(),
        }
    }
}

impl<O> KeyspaceOptions<O> {
    pub fn new(lane: O, value: O, map: O) -> Self {
        KeyspaceOptions { lane, value, map }
    }
}

pub struct KeyspaceDef<O> {
    pub(crate) name: &'static str,
    pub(crate) opts: O,
}

impl<O> KeyspaceDef<O> {
    fn new(name: &'static str, opts: O) -> Self {
        KeyspaceDef { name, opts }
    }
}

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

pub struct KeyStoreTask<S: KeyspaceByteEngine> {
    db: Arc<S>,
    rx: ReceiverStream<KeyRequest>,
}

impl<S: KeyspaceByteEngine> KeyStoreTask<S> {
    pub fn new(db: Arc<S>, rx: ReceiverStream<KeyRequest>) -> Self {
        KeyStoreTask { db, rx }
    }

    async fn run(self) {
        let KeyStoreTask { db, rx } = self;
        let mut requests = rx.fuse();

        while let Some((uri, responder)) = requests.next().await {
            let prefixed = format!("{}/{}", LANE_PREFIX, uri);

            match db.get_keyspace(KeyspaceName::Lane, prefixed.as_bytes()) {
                Ok(Some(bytes)) => {
                    let id = deserialize::<u64>(bytes.as_ref()).unwrap();
                    let _ = responder.send(id);
                }
                Ok(None) => {
                    db.merge_keyspace(KeyspaceName::Lane, COUNTER_KEY.as_bytes(), 1)
                        .unwrap();

                    let counter_bytes = db
                        .get_keyspace(KeyspaceName::Lane, COUNTER_KEY.as_bytes())
                        .unwrap()
                        .unwrap();
                    let id = deserialize::<u64>(counter_bytes.as_ref()).unwrap();

                    db.put_keyspace(
                        KeyspaceName::Lane,
                        prefixed.as_bytes(),
                        counter_bytes.as_slice(),
                    )
                    .unwrap();

                    let _ = responder.send(id);
                }
                Err(e) => {
                    panic!("{:?}", e)
                }
            }
        }
    }
}

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
    pub fn new<S: KeyspaceByteEngine>(db: Arc<S>, buffer_size: NonZeroUsize) -> KeyStore {
        let (tx, rx) = mpsc::channel(buffer_size.get());
        let task = KeyStoreTask::new(db, ReceiverStream::new(rx));

        KeyStore {
            tx,
            task: Arc::new(tokio::spawn(task.run())),
        }
    }

    pub async fn id_for<I>(&self, lane_id: I) -> u64
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

fn incrementing_operator(
    _new_key: &[u8],
    existing_value: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut value = match existing_value {
        Some(bytes) => deserialize::<u64>(bytes).unwrap(),
        None => 0,
    };

    for op in operands {
        let deserialized = deserialize::<u64>(op).unwrap();
        value += deserialized;
    }
    Some(serialize(&value).unwrap())
}

pub trait KeyspaceByteEngine: Send + Sync + 'static {
    /// Put a key-value pair into this store.
    fn put_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError>;

    /// Get an entry from this store by its key.
    fn get_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a value from this store by its key.
    fn delete_keyspace(&self, keyspace: KeyspaceName, key: &[u8]) -> Result<(), StoreError>;

    fn merge_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
        value: u64,
    ) -> Result<(), StoreError>;
}

#[cfg(test)]
mod tests {
    use crate::engines::keyspaces::{incrementing_operator, KeyStore, LANE_KS, MAP_LANE_KS};
    use crate::RocksDatabase;
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
        lane_opts.set_merge_operator_associative("lane_id_counter", incrementing_operator);
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
}
