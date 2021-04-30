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

use crate::engines::KeyspaceByteEngine;
use crate::stores::lane::{deserialize, serialize};
use crate::LANE_KS;
use futures::StreamExt;
use rocksdb::MergeOperands;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use utilities::sync::trigger;

pub type KeySize = u64;
pub type KeyRequest = (String, oneshot::Sender<KeySize>);

const COUNTER_KEY: &'static str = "counter";
const LANE_PREFIX: &'static str = "lane/";

pub struct KeyStoreTask<S: KeyspaceByteEngine> {
    db: Arc<S>,
    rx: ReceiverStream<KeyRequest>,
}

impl<S: KeyspaceByteEngine> KeyStoreTask<S> {
    pub fn new(db: Arc<S>, rx: ReceiverStream<KeyRequest>) -> Self {
        KeyStoreTask { db, rx }
    }

    async fn run(self, stop_on: trigger::Receiver) {
        let KeyStoreTask { db, rx } = self;
        let mut requests = rx.take_until(stop_on).fuse();

        while let Some((uri, responder)) = requests.next().await {
            let prefixed = format!("{}/{}", LANE_PREFIX, uri);

            match db.get_keyspace(LANE_KS, prefixed.as_bytes()) {
                Ok(Some(bytes)) => {
                    let id = deserialize::<u64>(bytes.as_ref()).unwrap();
                    let _ = responder.send(id);
                }
                Ok(None) => {
                    db.merge_keyspace(
                        LANE_KS,
                        COUNTER_KEY.as_bytes(),
                        serialize(&1_u64).unwrap().as_slice(),
                    )
                    .unwrap();

                    let counter_bytes = db
                        .get_keyspace(LANE_KS, COUNTER_KEY.as_bytes())
                        .unwrap()
                        .unwrap();
                    let id = deserialize::<u64>(counter_bytes.as_ref()).unwrap();

                    db.put_keyspace(LANE_KS, prefixed.as_bytes(), counter_bytes.as_slice())
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

impl KeyStore {
    pub fn new<S: KeyspaceByteEngine>(
        db: Arc<S>,
        buffer_size: NonZeroUsize,
        stop_on: trigger::Receiver,
    ) -> KeyStore {
        let (tx, rx) = mpsc::channel(buffer_size.get());
        let task = KeyStoreTask::new(db, ReceiverStream::new(rx));

        KeyStore {
            tx,
            task: Arc::new(tokio::spawn(task.run(stop_on))),
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

#[cfg(test)]
mod tests {
    use crate::stores::keystore::{incrementing_operator, KeyStore};
    use crate::{RocksDatabase, LANE_KS, MAP_LANE_KS};
    use rocksdb::{ColumnFamilyDescriptor, DB};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use tempdir::TempDir;
    use utilities::sync::trigger::trigger;

    fn lane_table_cf() -> ColumnFamilyDescriptor {
        let mut lane_opts = rocksdb::Options::default();
        lane_opts.set_merge_operator_associative("lane_id_counter", incrementing_operator);

        ColumnFamilyDescriptor::new(LANE_KS, lane_opts)
    }

    fn make_db() -> (TempDir, RocksDatabase) {
        let temp_file = TempDir::new("test").unwrap();
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let map_cf = ColumnFamilyDescriptor::new(MAP_LANE_KS, rocksdb::Options::default());

        let db = DB::open_cf_descriptors(&db_opts, temp_file.path(), vec![lane_table_cf(), map_cf])
            .unwrap();

        (temp_file, RocksDatabase::new(db))
    }

    #[tokio::test]
    async fn keyspace() {
        let (_dir, arc_db) = make_db();
        let (_trigger_tx, trigger_rx) = trigger();
        let key_store = KeyStore::new(Arc::new(arc_db), NonZeroUsize::new(8).unwrap(), trigger_rx);

        assert_eq!(key_store.id_for("lane_a").await, 1);
        assert_eq!(key_store.id_for("lane_a").await, 1);

        assert_eq!(key_store.id_for("lane_b").await, 2);
        assert_eq!(key_store.id_for("lane_b").await, 2);
    }
}
