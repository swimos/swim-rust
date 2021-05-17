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

use crate::store::KeyspaceName;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use std::num::NonZeroUsize;
use std::sync::Arc;
use store::keyspaces::{KeyType, KeyspaceByteEngine};
use store::{serialize, MergeOperands, StoreError};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

pub type KeyRequest = (String, oneshot::Sender<KeyType>);

/// The lane keyspace's counter key.
pub const COUNTER_KEY: &str = "counter";
pub const COUNTER_BYTES: &[u8] = COUNTER_KEY.as_bytes();
const DESERIALIZATION_FAILURE: &str = "Failed to deserialize key";
const SERIALIZATION_FAILURE: &str = "Failed to serialize key";
pub const INCONSISTENT_KEYSPACE: &str = "Inconsistent keyspace";
const RESPONSE_FAILURE: &str = "Failed to send response";

/// The prefix that all lane identifiers in the counter keyspace will be prefixed by.
pub const LANE_PREFIX: &str = "lane";

/// The initial value that the lane identifier keyspace will be initialised with if it doesn't
/// already exist.
pub const INITIAL: KeyType = 0;
pub const STEP: KeyType = 1;

pub trait KeystoreTask {
    fn run<DB, S>(db: Arc<DB>, events: S) -> BoxFuture<'static, Result<(), StoreError>>
    where
        DB: KeyspaceByteEngine,
        S: Stream<Item = KeyRequest> + Unpin + Send + 'static;
}

fn deserialize_key<B: AsRef<[u8]>>(bytes: B) -> Result<KeyType, StoreError> {
    bincode::deserialize::<KeyType>(bytes.as_ref()).map_err(|e| StoreError::Decoding(e.to_string()))
}

pub fn format_key<I: ToString>(uri: I) -> String {
    format!("{}/{}", LANE_PREFIX, uri.to_string())
}

/// A keystore for assigning unique identifiers to lane addresses.
#[derive(Clone)]
pub struct KeyStore {
    tx: mpsc::Sender<KeyRequest>,
    task: Arc<JoinHandle<()>>,
}

impl KeyStore {
    /// Produces a new keystore which will delegate its operations to `db` and will have an internal
    /// task communication buffer size of `buffer_size`.
    pub fn new<S>(db: Arc<S>, buffer_size: NonZeroUsize) -> KeyStore
    where
        S: KeyspaceByteEngine + KeystoreTask,
    {
        let (tx, rx) = mpsc::channel(buffer_size.get());
        let task = async move {
            if let Err(e) = S::run(db, ReceiverStream::new(rx)).await {
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

pub async fn lane_key_task<DB, S>(db: Arc<DB>, events: S) -> Result<(), StoreError>
where
    DB: KeyspaceByteEngine,
    S: Stream<Item = KeyRequest> + Unpin + Send + 'static,
{
    let mut requests = events.fuse();

    while let Some((uri, responder)) = requests.next().await {
        let prefixed = format_key(uri);

        match db.get_keyspace(KeyspaceName::Lane, prefixed.as_bytes())? {
            Some(bytes) => {
                let id = deserialize_key(bytes)?;
                responder.send(id).expect(RESPONSE_FAILURE);
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

                responder.send(id).expect(RESPONSE_FAILURE);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::store::keystore::{
        deserialize_key, format_key, KeyStore, KeyspaceName, COUNTER_BYTES, COUNTER_KEY,
        INCONSISTENT_KEYSPACE,
    };
    use crate::store::mock::MockStore;

    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use store::deserialize;
    use store::keyspaces::{KeyType, Keyspace, KeyspaceByteEngine};

    fn keyspaces() -> Vec<String> {
        vec![
            KeyspaceName::Lane.name().to_string(),
            KeyspaceName::Value.name().to_string(),
            KeyspaceName::Map.name().to_string(),
        ]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lane_id() {
        let delegate = Arc::new(MockStore::with_keyspaces(keyspaces()));
        let store = KeyStore::new(delegate.clone(), NonZeroUsize::new(8).unwrap());

        let lane_uri = "A";

        assert_eq!(store.id_for(lane_uri).await, 1);
        assert_eq!(store.id_for(lane_uri).await, 1);

        let opt = delegate
            .get_keyspace(KeyspaceName::Lane, COUNTER_BYTES)
            .unwrap()
            .expect(INCONSISTENT_KEYSPACE);

        assert_eq!(deserialize_key(opt).unwrap(), 1);
        assert_counters(&delegate, lane_uri, 1, 1);
    }

    #[tokio::test]
    async fn multiple_lane_ids() {
        let delegate = Arc::new(MockStore::with_keyspaces(keyspaces()));
        let store = KeyStore::new(delegate.clone(), NonZeroUsize::new(8).unwrap());

        let lane_prefix = "lane";
        let mut lane_count = 0;

        for lane_id in 1..=10 {
            lane_count += 1;
            let lane_uri = format!("{}/{}", lane_prefix, lane_id);
            assert_eq!(store.id_for(lane_uri.as_str()).await, lane_count);

            assert_counters(&delegate, lane_uri.as_str(), lane_count, lane_count);
        }
    }

    fn assert_counters(
        store: &Arc<MockStore>,
        lane_uri: &str,
        lane_count_at: KeyType,
        counter_at: KeyType,
    ) {
        let key = format_key(lane_uri.to_string());
        let lane_id = store
            .get_keyspace(KeyspaceName::Lane, key.as_bytes())
            .unwrap()
            .expect("Missing key");
        assert_eq!(
            deserialize::<KeyType>(lane_id.as_slice()).unwrap(),
            lane_count_at
        );

        let counter = store
            .get_keyspace(KeyspaceName::Lane, COUNTER_KEY.as_bytes())
            .unwrap()
            .expect("Missing counter");
        assert_eq!(
            deserialize::<KeyType>(counter.as_slice()).unwrap(),
            counter_at
        );
    }
}
