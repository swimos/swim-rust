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
use futures::StreamExt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use store::keyspaces::{KeyType, KeyspaceByteEngine};
use store::StoreError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

pub type KeyRequest = (String, oneshot::Sender<KeyType>);

/// The lane keyspace's counter key.
pub const COUNTER_KEY: &str = "counter";
const COUNTER_BYTES: &[u8] = COUNTER_KEY.as_bytes();
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

/// A task for loading and assigning unique identifiers to lane addresses.
pub struct KeyStoreTask<S: KeyspaceByteEngine> {
    /// The delegate store for fetching and merging the unique identifier from.
    db: Arc<S>,
    /// A stream of incoming requests for the unique identifiers.
    rx: ReceiverStream<KeyRequest>,
}

fn deserialize_key<B: AsRef<[u8]>>(bytes: B) -> Result<KeyType, StoreError> {
    bincode::deserialize::<KeyType>(bytes.as_ref()).map_err(|e| StoreError::Decoding(e.to_string()))
}

pub fn format_key<I: ToString>(uri: I) -> String {
    format!("{}/{}", LANE_PREFIX, uri.to_string())
}

impl<S: KeyspaceByteEngine> KeyStoreTask<S> {
    pub fn new(db: Arc<S>, rx: ReceiverStream<KeyRequest>) -> Self {
        KeyStoreTask { db, rx }
    }

    async fn run(self) -> Result<(), StoreError> {
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
