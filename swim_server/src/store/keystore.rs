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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::keyspaces::KeyspaceByteEngine;
use store::{deserialize, serialize, StoreError};
use tokio::sync::oneshot;

pub type KeyRequest = (String, oneshot::Sender<u64>);

/// The lane keyspace's counter key.
pub const COUNTER_KEY: &str = "counter";
pub const COUNTER_BYTES: &[u8] = COUNTER_KEY.as_bytes();
pub const INCONSISTENT_KEYSPACE: &str = "Inconsistent keyspace";

const INIT_FAILURE: &str = "Failed to initialise keystore";

/// The prefix that all lane identifiers in the counter keyspace will be prefixed by.
pub const LANE_PREFIX: &str = "lane";

/// The initial value that the lane identifier keyspace will be initialised with if it doesn't
/// already exist.
pub const INITIAL: u64 = 0;
pub const STEP: u64 = 1;

#[derive(Debug)]
pub struct KeyStore<D> {
    delegate: Arc<D>,
    count: Arc<AtomicU64>,
}

impl<D> Clone for KeyStore<D> {
    fn clone(&self) -> Self {
        KeyStore {
            delegate: self.delegate.clone(),
            count: self.count.clone(),
        }
    }
}

impl<D: KeyspaceByteEngine> KeyStore<D> {
    pub fn initialise_with(delegate: Arc<D>) -> KeyStore<D> {
        let count = match delegate.get_keyspace(KeyspaceName::Lane, COUNTER_BYTES) {
            Ok(Some(counter)) => deserialize::<u64>(counter.as_slice()).expect(INIT_FAILURE),
            Ok(None) => INITIAL,
            Err(e) => {
                panic!("{}: `{:?}`", INIT_FAILURE, e)
            }
        };

        KeyStore {
            delegate,
            count: Arc::new(AtomicU64::new(count)),
        }
    }

    pub fn id_for(&self, lane_id: String) -> Result<u64, StoreError> {
        let KeyStore { delegate, count } = self;
        let prefixed = format_key(lane_id);

        match delegate.get_keyspace(KeyspaceName::Lane, prefixed.as_bytes())? {
            Some(bytes) => deserialize_key(bytes),
            None => {
                let id = count.fetch_add(STEP, Ordering::Acquire) + 1;
                delegate.merge_keyspace(KeyspaceName::Lane, COUNTER_BYTES, STEP)?;
                delegate.put_keyspace(
                    KeyspaceName::Lane,
                    prefixed.as_bytes(),
                    serialize(&id)?.as_slice(),
                )?;

                Ok(id)
            }
        }
    }
}

fn deserialize_key<B: AsRef<[u8]>>(bytes: B) -> Result<u64, StoreError> {
    bincode::deserialize::<u64>(bytes.as_ref()).map_err(|e| StoreError::Decoding(e.to_string()))
}

pub fn format_key<I: ToString>(uri: I) -> String {
    format!("{}/{}", LANE_PREFIX, uri.to_string())
}

#[cfg(feature = "persistence")]
pub mod rocks {
    use crate::store::keystore::INITIAL;
    use rocksdb::MergeOperands;
    use store::{deserialize_key, serialize};

    #[cfg(feature = "persistence")]
    const DESERIALIZATION_FAILURE: &str = "Failed to deserialize key";
    #[cfg(feature = "persistence")]
    const SERIALIZATION_FAILURE: &str = "Failed to serialize key";

    #[cfg(feature = "persistence")]
    #[allow(clippy::unnecessary_wraps)]
    pub fn incrementing_merge_operator(
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
}

#[cfg(test)]
mod tests {
    use crate::store::keystore::{
        deserialize_key, format_key, KeyStore, KeyspaceName, COUNTER_BYTES, COUNTER_KEY,
        INCONSISTENT_KEYSPACE,
    };
    use crate::store::mock::MockStore;
    use std::sync::Arc;
    use store::deserialize;
    use store::keyspaces::{Keyspace, KeyspaceByteEngine};

    fn keyspaces() -> Vec<String> {
        vec![
            KeyspaceName::Lane.name().to_string(),
            KeyspaceName::Value.name().to_string(),
            KeyspaceName::Map.name().to_string(),
        ]
    }

    #[test]
    fn lane_id() {
        let delegate = Arc::new(MockStore::with_keyspaces(keyspaces()));
        let store = KeyStore::initialise_with(delegate.clone());

        let lane_uri = "A";

        assert_eq!(store.id_for(lane_uri.to_string()), Ok(1));
        assert_eq!(store.id_for(lane_uri.to_string()), Ok(1));

        let opt = delegate
            .get_keyspace(KeyspaceName::Lane, COUNTER_BYTES)
            .unwrap()
            .expect(INCONSISTENT_KEYSPACE);

        assert_eq!(deserialize_key(opt).unwrap(), 1);
        assert_counters(&delegate, lane_uri, 1, 1);
    }

    #[test]
    fn multiple_lane_ids() {
        let delegate = Arc::new(MockStore::with_keyspaces(keyspaces()));
        let store = KeyStore::initialise_with(delegate.clone());

        let lane_prefix = "lane";
        let mut lane_count = 0;

        for lane_id in 1..=10 {
            lane_count += 1;
            let lane_uri = format!("{}/{}", lane_prefix, lane_id);
            assert_eq!(store.id_for(lane_uri.to_string()), Ok(lane_count));

            assert_counters(&delegate, lane_uri.as_str(), lane_count, lane_count);
        }
    }

    fn assert_counters(
        store: &Arc<MockStore>,
        lane_uri: &str,
        lane_count_at: u64,
        counter_at: u64,
    ) {
        let key = format_key(lane_uri.to_string());
        let lane_id = store
            .get_keyspace(KeyspaceName::Lane, key.as_bytes())
            .unwrap()
            .expect("Missing key");
        assert_eq!(
            deserialize::<u64>(lane_id.as_slice()).unwrap(),
            lane_count_at
        );

        let counter = store
            .get_keyspace(KeyspaceName::Lane, COUNTER_KEY.as_bytes())
            .unwrap()
            .expect("Missing counter");
        assert_eq!(deserialize::<u64>(counter.as_slice()).unwrap(), counter_at);
    }
}
