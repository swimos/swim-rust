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

use crate::agent::lane::model::map::{
    summaries_to_events, to_map_store_event, MapLane, MapLaneEvent, MapSubscriber, MapUpdate,
};
use crate::agent::lane::model::DeferredSubscription;
use crate::agent::lane::tests::ExactlyOnce;
use futures::future::ready;
use futures::{FutureExt, Stream, StreamExt};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use stm::stm::Stm;
use stm::transaction::{atomically, TransactionRunner};
use swim_form::Form;
use swim_model::{Attr, Item, Value};
use swim_persistence::agent::lane::error::StoreErrorHandler;
use swim_persistence::agent::lane::map::{MapDataModel, MapLaneStoreIo};
use swim_persistence::agent::lane::StoreIo;
use swim_persistence::agent::{NodeStore, PrefixNodeStore};
use swim_persistence::plane::mock::MockPlaneStore;
use swim_persistence::{StoreEngine, StoreKey};
use swim_store::{serialize, EngineInfo, RangeConsumer, StoreError};
use swim_utilities::algebra::non_zero_usize;

fn buffer_size() -> NonZeroUsize {
    non_zero_usize!(16)
}

fn make_subscribable<K, V>(buffer_size: NonZeroUsize) -> (MapLane<K, V>, MapSubscriber<K, V>)
where
    K: Form + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let (lane, rx) = MapLane::observable(buffer_size);
    (lane, MapSubscriber::new(rx.into_subscriber()))
}

#[test]
fn try_type_update_event_success() {
    let value = Arc::new(4);
    let event = MapLaneEvent::Update(Value::Int32Value(2), value.clone());
    let typed: Result<MapLaneEvent<i32, i32>, _> = event.try_into_typed();
    assert!(matches!(typed, Ok(MapLaneEvent::Update(2, v)) if Arc::ptr_eq(&v, &value)));
}

#[test]
fn try_type_remove_event_success() {
    let event: MapLaneEvent<Value, i32> = MapLaneEvent::Remove(Value::Int32Value(2));
    let typed: Result<MapLaneEvent<i32, i32>, _> = event.try_into_typed();
    assert!(matches!(typed, Ok(MapLaneEvent::Remove(2))));
}

#[test]
fn try_type_update_event_failure() {
    let value = Arc::new(4);
    let event = MapLaneEvent::Update(Value::text("Boom!"), value);
    let typed: Result<MapLaneEvent<i32, i32>, _> = event.try_into_typed();
    assert!(typed.is_err());
}

#[test]
fn try_type_remove_event_failure() {
    let event: MapLaneEvent<Value, i32> = MapLaneEvent::Remove(Value::text("Boom!"));
    let typed: Result<MapLaneEvent<i32, i32>, _> = event.try_into_typed();
    assert!(typed.is_err());
}

async fn update_direct<Str>(lane: &MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    let value = Arc::new(5);
    let result = lane
        .update_direct(1, value.clone())
        .apply(ExactlyOnce)
        .await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Update(1, v)) if Arc::ptr_eq(&v, &value)));
}

#[tokio::test]
async fn update_direct_test() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    update_direct(&lane, &mut events).await;
}

#[tokio::test]
async fn remove_direct_not_contained() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    let result = lane.remove_direct(1).apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn remove_direct_contained() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();

    update_direct(&lane, &mut events).await;
    let result = lane.remove_direct(1).apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Remove(1))));
}

#[tokio::test]
async fn clear_direct_empty() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    let result = lane.clear_direct().apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn clear_direct_nonempty() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    update_direct(&lane, &mut events).await;
    let result = lane.clear_direct().apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Clear)));
}

#[tokio::test]
async fn get_value() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    update_direct(&lane, &mut events).await;

    let result1 = atomically(&lane.get(1), ExactlyOnce).await;

    assert!(matches!(result1, Ok(Some(v)) if *v == 5));

    let result2 = atomically(&lane.get(2), ExactlyOnce).await;

    assert!(matches!(result2, Ok(None)));
}

#[tokio::test]
async fn contains_key() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    update_direct(&lane, &mut events).await;

    let result1 = atomically(&lane.contains(1), ExactlyOnce).await;

    assert!(matches!(result1, Ok(true)));

    let result2 = atomically(&lane.contains(2), ExactlyOnce).await;

    assert!(matches!(result2, Ok(false)));
}

#[tokio::test]
async fn map_len() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();

    let result1 = atomically(&lane.len(), ExactlyOnce).await;

    assert!(matches!(result1, Ok(0)));

    update_direct(&lane, &mut events).await;

    let result2 = atomically(&lane.len(), ExactlyOnce).await;

    assert!(matches!(result2, Ok(1)));
}

#[tokio::test]
async fn map_is_empty() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();

    let result1 = atomically(&lane.is_empty(), ExactlyOnce).await;

    assert!(matches!(result1, Ok(true)));

    update_direct(&lane, &mut events).await;

    let result2 = atomically(&lane.is_empty(), ExactlyOnce).await;

    assert!(matches!(result2, Ok(false)));
}

async fn populate<Str>(lane: &MapLane<i32, i32>, events: &mut Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    let result = lane.update_direct(1, Arc::new(7)).apply(ExactlyOnce).await;
    assert!(result.is_ok());
    let result = lane.update_direct(8, Arc::new(13)).apply(ExactlyOnce).await;
    assert!(result.is_ok());
    let _ = events.next().await;
    while events.next().now_or_never().is_some() {}
}

#[tokio::test]
async fn map_first() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();

    populate(&lane, &mut events).await;

    let result = atomically(&lane.first(), ExactlyOnce).await;

    assert!(matches!(result, Ok(Some(v)) if *v == 7));
}

#[tokio::test]
async fn map_last() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();

    populate(&lane, &mut events).await;

    let result = atomically(&lane.last(), ExactlyOnce).await;

    assert!(matches!(result, Ok(Some(v)) if *v == 13));
}

#[tokio::test]
async fn update_compound() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    let value = Arc::new(5);
    let result = atomically(&lane.update(1, value.clone()), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Update(1, v)) if Arc::ptr_eq(&v, &value)));
}

#[tokio::test]
async fn remove_compound_not_contained() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    let result = atomically(&lane.remove(1), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn remove_compound_contained() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    update_direct(&lane, &mut events).await;
    let result = atomically(&lane.remove(1), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Remove(1))));
}

#[tokio::test]
async fn clear_compound_empty() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();

    let result = atomically(&lane.clear(), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn clear_compound_nonempty() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    update_direct(&lane, &mut events).await;
    let result = atomically(&lane.clear(), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Clear)));
}

#[tokio::test]
async fn double_set() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();

    populate(&lane, &mut events).await;

    let upd = lane.get(1).and_then(|maybe| match maybe {
        Some(i) => lane.update(8, Arc::new(*i + 1)),
        _ => lane.update(8, Arc::new(-1)),
    });

    let stm = lane.update(8, Arc::new(17)).followed_by(upd);

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Update(8, v)) if *v == 8));

    let next_event = events.next().now_or_never();
    assert!(next_event.is_none());
}

#[tokio::test]
async fn transaction_with_clear() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    populate(&lane, &mut events).await;

    let insert = lane.update(42, Arc::new(-4));
    let clear = lane.clear();
    let upd = lane.update(8, Arc::new(123));
    let rem = lane.remove(1);
    let stm = insert.followed_by(clear.followed_by(upd.followed_by(rem)));

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(result.is_ok());

    let received = (&mut events).take(2).collect::<Vec<_>>().await;

    assert!(
        matches!(received.as_slice(), [MapLaneEvent::Clear, MapLaneEvent::Update(8, v)] if **v == 123)
    );

    let another = events.next().now_or_never();
    assert!(another.is_none());
}

#[tokio::test]
async fn snapshot_map() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();

    populate(&lane, &mut events).await;

    let result = atomically(&lane.snapshot(), ExactlyOnce).await;

    let mut expected = HashMap::new();
    expected.insert(1, Arc::new(7));
    expected.insert(8, Arc::new(13));

    assert!(matches!(result, Ok(map) if map == expected));
}

#[tokio::test]
async fn modify_if_defined_direct() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();

    populate(&lane, &mut events).await;

    let result = lane
        .modify_if_defined_direct(1, |n| *n * 2)
        .apply(ExactlyOnce)
        .await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Update(1, v)) if *v == 14));

    let result = lane
        .modify_if_defined_direct(27, |n| *n * 2)
        .apply(ExactlyOnce)
        .await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn modify_direct_some() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    populate(&lane, &mut events).await;

    let result = lane
        .modify_direct(1, |opt| opt.map(|n| *n * 2))
        .apply(ExactlyOnce)
        .await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Update(1, v)) if *v == 14));

    let result = lane.modify_direct(1, |_| None).apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Remove(1))));
}

#[tokio::test]
async fn modify_direct_none() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    populate(&lane, &mut events).await;

    let result = lane
        .modify_direct(27, |_| Some(42))
        .apply(ExactlyOnce)
        .await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Update(27, v)) if *v == 42));

    let result = lane.modify_direct(156, |_| None).apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn checkpoint_map() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());
    let mut events = sub.subscribe().unwrap();
    populate(&lane, &mut events).await;

    let result = atomically(&lane.checkpoint(12), ExactlyOnce).await;

    assert!(result.is_ok());
    let result_map = result.unwrap();
    assert_eq!(result_map.len(), 2);
    assert!(result_map.contains_key(&Value::Int32Value(1)));
    assert!(result_map.contains_key(&Value::Int32Value(8)));

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Checkpoint(12))));
}

#[test]
fn test_derive_map_update() {
    let clear: MapUpdate<i32, i32> = MapUpdate::Clear;
    assert_eq!(clear.as_value(), Value::of_attr("clear"));

    let remove: MapUpdate<i32, i32> = MapUpdate::Remove(100);
    assert_eq!(
        remove.as_value(),
        Value::Record(
            vec![(Attr::of(("remove", Value::from_vec(vec![Item::of(("key", 100i32))]))))],
            Vec::new()
        )
    );

    let update = MapUpdate::Update(100, Arc::new(200));
    assert_eq!(
        update.as_value(),
        Value::Record(
            vec![Attr::of((
                "update",
                Value::from_vec(vec![Item::of(("key", 100i32))])
            ))],
            vec![Item::of(200i32)]
        )
    );
}

#[test]
fn test_map_update_form() {
    let update = MapUpdate::Update(100, Arc::new(200));
    let value = update.into_value();

    let expected_value = Value::Record(
        vec![Attr::of(("update", Value::from_vec(vec![("key", 100i32)])))],
        vec![Item::ValueItem(Value::Int32Value(200))],
    );

    assert_eq!(value, expected_value);

    let converted_update: MapUpdate<i32, i32> = Form::try_from_value(&value).unwrap();
    assert_eq!(MapUpdate::Update(100, Arc::new(200)), converted_update);

    let converted_update: MapUpdate<i32, i32> = Form::try_convert(value).unwrap();
    assert_eq!(MapUpdate::Update(100, Arc::new(200)), converted_update);
}

#[tokio::test]
async fn update_direct_multiple_test() {
    let (lane, _sub) = make_subscribable::<String, i32>(buffer_size());

    let mut runner = TransactionRunner::new(1, || ExactlyOnce);
    for i in 0..50 {
        assert!(lane
            .update_direct("Key".to_owned(), Arc::new(i))
            .apply_with(&mut runner)
            .await
            .is_ok());
    }
}

fn make_store_key(lane_id: u64, key: String) -> StoreKey {
    StoreKey::Map {
        lane_id,
        key: Some(serialize(&key).expect("Failed to serialize store key")),
    }
}

struct It {
    index: usize,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl RangeConsumer for It {
    fn consume_next<'a>(&'a mut self) -> Result<Option<(&'a [u8], &'a [u8])>, StoreError> {
        let It { index, entries } = self;
        let n = *index;
        *index += 1;
        Ok(entries.get(n).map(|(k, v)| (k.as_ref(), v.as_ref())))
    }
}

impl<'a> PrefixNodeStore<'a> for TrackingMapStore {
    type RangeCon = It;

    fn ranged_snapshot_consumer(&'a self, prefix: StoreKey) -> Result<Self::RangeCon, StoreError> {
        let prefix = prefix.serialize_as_bytes();
        let guard = self.values.lock().unwrap();
        let entries = guard
            .deref()
            .iter()
            .filter_map(|(k, v)| {
                if k.starts_with(&prefix) {
                    Some((k.clone(), v.clone()))
                } else {
                    None
                }
            })
            .collect();
        Ok(It { entries, index: 0 })
    }
}

#[derive(Debug, Clone)]
struct TrackingMapStore {
    values: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl NodeStore for TrackingMapStore {
    type Delegate = MockPlaneStore;

    fn engine_info(&self) -> EngineInfo {
        EngineInfo {
            path: "tracking".to_string(),
            kind: "tracking".to_string(),
        }
    }

    fn lane_id_of(&self, _lane: &str) -> Result<u64, StoreError> {
        Ok(0)
    }

    fn load_ranged_snapshot<F, K, V>(
        &self,
        prefix: StoreKey,
        map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let prefix = prefix.serialize_as_bytes();
        let guard = self.values.lock().unwrap();
        let mut entries = guard.deref().clone().into_iter();
        let mapped = entries.try_fold(Vec::new(), |mut entries, (k, v)| {
            if k.starts_with(&prefix) {
                match map_fn(k.as_ref(), v.as_ref()) {
                    Ok((k, v)) => {
                        entries.push((k, v));
                        Ok(entries)
                    }
                    Err(e) => Err(e),
                }
            } else {
                Ok(entries)
            }
        })?;

        if mapped.is_empty() {
            Ok(None)
        } else {
            Ok(Some(mapped))
        }
    }
}

impl StoreEngine for TrackingMapStore {
    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        match key {
            k @ StoreKey::Map { .. } => {
                let mut guard = self.values.lock().unwrap();
                guard.insert(k.serialize_as_bytes(), value.to_vec());
                Ok(())
            }
            StoreKey::Value { .. } => {
                panic!("Expected a map key")
            }
        }
    }

    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        match key {
            k @ StoreKey::Map { .. } => {
                let guard = self.values.lock().unwrap();
                Ok(guard.get(k.serialize_as_bytes().as_slice()).cloned())
            }
            StoreKey::Value { .. } => {
                panic!("Expected a map key")
            }
        }
    }

    fn delete(&self, key: StoreKey) -> Result<(), StoreError> {
        match key {
            k @ StoreKey::Map { .. } => {
                let mut guard = self.values.lock().unwrap();
                guard.remove(k.serialize_as_bytes().as_slice());
                Ok(())
            }
            StoreKey::Value { .. } => {
                panic!("Expected a map key")
            }
        }
    }
}

#[tokio::test]
async fn io_load_some() {
    let mut expected_id_0 = HashMap::new();
    expected_id_0.insert("a".to_string(), Arc::new(1));
    expected_id_0.insert("b".to_string(), Arc::new(2));
    expected_id_0.insert("c".to_string(), Arc::new(3));
    expected_id_0.insert("d".to_string(), Arc::new(4));
    expected_id_0.insert("e".to_string(), Arc::new(5));

    let mut expected_id_1 = HashMap::new();
    expected_id_1.insert("f".to_string(), Arc::new(6));
    expected_id_1.insert("g".to_string(), Arc::new(7));
    expected_id_1.insert("h".to_string(), Arc::new(8));
    expected_id_1.insert("i".to_string(), Arc::new(9));
    expected_id_1.insert("j".to_string(), Arc::new(10));

    let mut initial = expected_id_0
        .clone()
        .into_iter()
        .fold(HashMap::new(), |mut map, (k, v)| {
            let store_key = make_store_key(0, k);
            map.insert(store_key.serialize_as_bytes(), serialize(&v).unwrap());
            map
        });

    let serialized_id_1 = expected_id_1
        .into_iter()
        .fold(HashMap::new(), |mut map, (k, v)| {
            let store_key = make_store_key(1, k);
            map.insert(store_key.serialize_as_bytes(), serialize(&v).unwrap());
            map
        });

    initial.extend(serialized_id_1);

    let values = Arc::new(Mutex::new(initial));
    let store = TrackingMapStore {
        values: values.clone(),
    };

    let model = MapDataModel::new(store, 0);
    let (lane, observer) = MapLane::<String, i32>::store_observable(&model, non_zero_usize!(8));
    let events = summaries_to_events::<String, i32>(observer.clone())
        .filter_map(|e| ready(to_map_store_event(e)));
    let store_io = MapLaneStoreIo::new(events, model);

    let _task_handle = tokio::spawn(store_io.attach(StoreErrorHandler::new(0)));

    let lane_snapshot: HashMap<String, Arc<i32>> =
        atomically(&lane.snapshot(), ExactlyOnce).await.unwrap();
    assert_eq!(lane_snapshot, expected_id_0);
}

#[tokio::test]
async fn io_crud() {
    let mut initial = HashMap::new();
    initial.insert("a".to_string(), Arc::new(1));
    initial.insert("b".to_string(), Arc::new(2));
    initial.insert("c".to_string(), Arc::new(3));
    initial.insert("d".to_string(), Arc::new(4));
    initial.insert("e".to_string(), Arc::new(5));

    let initial = initial
        .clone()
        .into_iter()
        .fold(HashMap::new(), |mut map, (k, v)| {
            let store_key = make_store_key(0, k);
            map.insert(store_key.serialize_as_bytes(), serialize(&v).unwrap());
            map
        });

    let values = Arc::new(Mutex::new(initial));
    let store = TrackingMapStore {
        values: values.clone(),
    };

    let model = MapDataModel::new(store, 0);

    let (lane, observer) = MapLane::<String, i32>::store_observable(&model, non_zero_usize!(8));
    let events = summaries_to_events::<String, i32>(observer.clone())
        .filter_map(|e| ready(to_map_store_event(e)));

    let store_io = MapLaneStoreIo::new(events, model);

    let _task_handle = tokio::spawn(store_io.attach(StoreErrorHandler::new(0)));

    let update_stm = lane.update("b".to_string(), Arc::new(13));
    assert!(atomically(&update_stm, ExactlyOnce).await.is_ok());

    let delete_stm = lane.remove("c".to_string());
    assert!(atomically(&delete_stm, ExactlyOnce).await.is_ok());

    let lane_snapshot: HashMap<String, Arc<i32>> =
        atomically(&lane.snapshot(), ExactlyOnce).await.unwrap();

    let mut expected = HashMap::new();
    expected.insert("a".to_string(), Arc::new(1));
    expected.insert("b".to_string(), Arc::new(13));
    expected.insert("d".to_string(), Arc::new(4));
    expected.insert("e".to_string(), Arc::new(5));

    assert_eq!(lane_snapshot, expected);
}
