// Copyright 2015-2023 Swim Inc.
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

use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
};

use bytes::{BufMut, BytesMut};
use futures::{future::join, StreamExt};
use parking_lot::Mutex;
use swimos_agent_protocol::{
    agent::{LaneRequest, LaneRequestDecoder},
    map::{MapMessage, MapMessageDecoder, MapOperation, RawMapOperationDecoder},
};
use swimos_api::store::NodePersistence;
use swimos_api::{
    error::StoreError,
    store::{KeyValue, RangeConsumer},
};
use swimos_utilities::{
    encoding::WithLengthBytesCodec, io::byte_channel::byte_channel, non_zero_usize,
};
use tokio_util::codec::FramedRead;

use crate::agent::store::{AgentPersistence, StorePersistence};

#[derive(Clone)]
struct FakeStore {
    inner: Arc<Mutex<Inner>>,
}

impl FakeStore {
    fn new(value: Option<Vec<u8>>, map: HashMap<Vec<u8>, Vec<u8>>) -> Self {
        FakeStore {
            inner: Arc::new(Mutex::new(Inner { value, map })),
        }
    }
}

struct Inner {
    value: Option<Vec<u8>>,
    map: HashMap<Vec<u8>, Vec<u8>>,
}

struct FakeConsumer {
    entries: VecDeque<(Vec<u8>, Vec<u8>)>,
    current: Option<(Vec<u8>, Vec<u8>)>,
}

impl RangeConsumer for FakeConsumer {
    fn consume_next(&mut self) -> Result<Option<KeyValue<'_>>, StoreError> {
        let FakeConsumer { entries, current } = self;
        if let Some(entry) = entries.pop_front() {
            let (k, v) = current.insert(entry);
            Ok(Some((&*k, &*v)))
        } else {
            Ok(None)
        }
    }
}

const VALUE_NAME: &str = "value";
const MAP_NAME: &str = "map";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Id {
    Value,
    Map,
}

impl NodePersistence for FakeStore {
    type LaneId = Id;

    fn id_for(&self, name: &str) -> Result<Self::LaneId, StoreError> {
        match name {
            VALUE_NAME => Ok(Id::Value),
            MAP_NAME => Ok(Id::Map),
            _ => Err(StoreError::KeyNotFound),
        }
    }

    fn get_value(
        &self,
        id: Self::LaneId,
        buffer: &mut BytesMut,
    ) -> Result<Option<usize>, StoreError> {
        if id == Id::Value {
            let guard = self.inner.lock();
            let maybe_v = &guard.value;
            if let Some(v) = maybe_v {
                buffer.reserve(v.len());
                buffer.put(v.as_ref());
                Ok(Some(v.len()))
            } else {
                Ok(None)
            }
        } else {
            Err(StoreError::DelegateMessage("Wrong key kind.".to_owned()))
        }
    }

    fn put_value(&mut self, id: Self::LaneId, value: &[u8]) -> Result<(), StoreError> {
        if id == Id::Value {
            let mut guard = self.inner.lock();
            let maybe_v = &mut guard.value;
            let v = maybe_v.get_or_insert_with(Default::default);
            v.clear();
            v.extend_from_slice(value);
            Ok(())
        } else {
            Err(StoreError::DelegateMessage("Wrong key kind.".to_owned()))
        }
    }

    fn update_map(&mut self, id: Self::LaneId, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        if id == Id::Map {
            self.inner
                .lock()
                .map
                .insert(key.to_owned(), value.to_owned());
            Ok(())
        } else {
            Err(StoreError::DelegateMessage("Wrong key kind.".to_owned()))
        }
    }

    fn remove_map(&mut self, id: Self::LaneId, key: &[u8]) -> Result<(), StoreError> {
        if id == Id::Map {
            self.inner.lock().map.remove(key);
            Ok(())
        } else {
            Err(StoreError::DelegateMessage("Wrong key kind.".to_owned()))
        }
    }

    fn clear_map(&mut self, id: Self::LaneId) -> Result<(), StoreError> {
        if id == Id::Map {
            self.inner.lock().map.clear();
            Ok(())
        } else {
            Err(StoreError::DelegateMessage("Wrong key kind.".to_owned()))
        }
    }

    fn delete_value(&mut self, id: Self::LaneId) -> Result<(), StoreError> {
        if id == Id::Value {
            let mut guard = self.inner.lock();
            let v = &mut guard.value;
            *v = None;
            Ok(())
        } else {
            Err(StoreError::DelegateMessage("Wrong key kind.".to_owned()))
        }
    }

    type MapCon<'a> = FakeConsumer
    where
        Self: 'a;

    fn read_map(&self, id: Self::LaneId) -> Result<Self::MapCon<'_>, StoreError> {
        if id == Id::Map {
            Ok(FakeConsumer {
                entries: self
                    .inner
                    .lock()
                    .map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                current: None,
            })
        } else {
            Err(StoreError::DelegateMessage("Wrong key kind.".to_owned()))
        }
    }
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

#[tokio::test]
async fn value_initializer() {
    let data = vec![1, 2, 3, 4, 5];
    let store = FakeStore::new(Some(data.clone()), Default::default());

    let persistence = StorePersistence(store.clone());
    let init = persistence
        .init_value_store(Id::Value)
        .expect("Expected initializer.");

    let (mut tx, mut rx) = byte_channel(BUFFER_SIZE);

    let init_task = init.initialize(&mut tx);

    let recv_task = async {
        let mut framed = FramedRead::new(&mut rx, LaneRequestDecoder::new(WithLengthBytesCodec));
        match framed.next().await {
            Some(Ok(LaneRequest::Command(body))) => {
                assert_eq!(body.as_ref(), &data);
            }
            ow => panic!("Unexpected result: {:?}", ow),
        }
        assert!(matches!(
            framed.next().await,
            Some(Ok(LaneRequest::InitComplete))
        ));
    };

    let (result, _) = join(init_task, recv_task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn map_initializer_empty() {
    let store = FakeStore::new(None, Default::default());

    let persistence = StorePersistence(store.clone());
    let init = persistence
        .init_map_store(Id::Map)
        .expect("Expected initializer.");

    let (mut tx, mut rx) = byte_channel(BUFFER_SIZE);

    let init_task = init.initialize(&mut tx);

    let recv_task = async {
        let mut framed = FramedRead::new(
            &mut rx,
            LaneRequestDecoder::new(MapMessageDecoder::new(RawMapOperationDecoder)),
        );
        assert!(matches!(
            framed.next().await,
            Some(Ok(LaneRequest::InitComplete))
        ));
    };

    let (result, _) = join(init_task, recv_task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn map_initializer_with_entries() {
    let mut map = HashMap::new();
    map.insert(vec![1], vec![1, 2, 3]);
    map.insert(vec![2], vec![4, 5, 6]);
    let store = FakeStore::new(None, map.clone());

    let persistence = StorePersistence(store.clone());
    let init = persistence
        .init_map_store(Id::Map)
        .expect("Expected initializer.");

    let (mut tx, mut rx) = byte_channel(BUFFER_SIZE);

    let init_task = init.initialize(&mut tx);

    let recv_task = async {
        let mut framed = FramedRead::new(
            &mut rx,
            LaneRequestDecoder::new(MapMessageDecoder::new(RawMapOperationDecoder)),
        );
        let mut received = HashMap::new();
        loop {
            match framed.next().await {
                Some(Ok(LaneRequest::Command(MapMessage::Update { key, value }))) => {
                    received.insert(key.as_ref().to_owned(), value.as_ref().to_owned());
                }
                Some(Ok(LaneRequest::InitComplete)) => break,
                ow => panic!("Unexpected result: {:?}", ow),
            }
        }
        assert_eq!(received, map);
    };

    let (result, _) = join(init_task, recv_task).await;
    assert!(result.is_ok());
}

#[test]
fn put_value() {
    let data = vec![1, 2, 3, 4, 5];
    let store = FakeStore::new(Some(data), Default::default());

    let mut persistence = StorePersistence(store.clone());

    let replace = &[8, 9, 10];
    assert!(persistence.put_value(Id::Value, replace).is_ok());

    assert_eq!(&store.inner.lock().value, &Some(replace.to_vec()));
}

#[test]
fn insert_map() {
    let store = FakeStore::new(None, Default::default());

    let mut persistence = StorePersistence(store.clone());

    let key = &[6];
    let value = &[1, 4, 6];
    assert!(persistence
        .apply_map::<&[u8]>(Id::Map, &MapOperation::Update { key, value })
        .is_ok());

    let map = &store.inner.lock().map;
    let mut expected = HashMap::new();
    expected.insert(key.to_vec(), value.to_vec());
    assert_eq!(map, &expected);
}

#[test]
fn remove_map() {
    let mut map = HashMap::new();
    map.insert(vec![1], vec![1, 2, 3]);
    map.insert(vec![2], vec![4, 5, 6]);
    let store = FakeStore::new(None, map.clone());

    let mut persistence = StorePersistence(store.clone());

    let key = &[1];
    assert!(persistence
        .apply_map::<&[u8]>(Id::Map, &MapOperation::Remove { key })
        .is_ok());

    let map = &store.inner.lock().map;
    let mut expected = HashMap::new();
    expected.insert(vec![2], vec![4, 5, 6]);
    assert_eq!(map, &expected);
}

#[test]
fn clear_map() {
    let mut map = HashMap::new();
    map.insert(vec![1], vec![1, 2, 3]);
    map.insert(vec![2], vec![4, 5, 6]);
    let store = FakeStore::new(None, map.clone());

    let mut persistence = StorePersistence(store.clone());

    assert!(persistence
        .apply_map::<&[u8]>(Id::Map, &MapOperation::Clear)
        .is_ok());

    let map = &store.inner.lock().map;
    assert!(map.is_empty());
}
