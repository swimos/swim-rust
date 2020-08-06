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

use crate::agent::lane::model::map::{make_lane_model, MapLane, MapLaneEvent};
use crate::agent::lane::strategy::{Buffered, Dropping, Queue};
use crate::agent::lane::tests::ExactlyOnce;
use common::model::Value;
use futures::{FutureExt, Stream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use stm::stm::Stm;
use stm::transaction::atomically;

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
    let event = MapLaneEvent::Update(Value::text("Boom!"), value.clone());
    let typed: Result<MapLaneEvent<i32, i32>, _> = event.try_into_typed();
    assert!(typed.is_err());
}

#[test]
fn try_type_remove_event_failure() {
    let event: MapLaneEvent<Value, i32> = MapLaneEvent::Remove(Value::text("Boom!"));
    let typed: Result<MapLaneEvent<i32, i32>, _> = event.try_into_typed();
    assert!(typed.is_err());
}

async fn update_direct<Str>(lane: &MapLane<i32, i32>, events: &mut Str)
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
async fn update_direct_queue() {
    let (lane, mut events) = make_lane_model(Queue::default());
    update_direct(&lane, &mut events).await;
}

#[tokio::test]
async fn update_direct_dropping() {
    let (lane, mut events) = make_lane_model(Dropping);
    update_direct(&lane, &mut events).await;
}

#[tokio::test]
async fn update_direct_buffered() {
    let (lane, mut events) = make_lane_model(Buffered::default());
    update_direct(&lane, &mut events).await;
}

async fn remove_direct_not_contained<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    let result = lane.remove_direct(1).apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn remove_direct_not_contained_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    remove_direct_not_contained(lane, events).await;
}

#[tokio::test]
async fn remove_direct_not_contained_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    remove_direct_not_contained(lane, events).await;
}

#[tokio::test]
async fn remove_direct_not_contained_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    remove_direct_not_contained(lane, events).await;
}

async fn remove_direct_contained<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    update_direct(&lane, &mut events).await;
    let result = lane.remove_direct(1).apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Remove(1))));
}

#[tokio::test]
async fn remove_direct_contained_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    remove_direct_contained(lane, events).await;
}

#[tokio::test]
async fn remove_direct_contained_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    remove_direct_contained(lane, events).await;
}

#[tokio::test]
async fn remove_direct_contained_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    remove_direct_contained(lane, events).await;
}

async fn clear_direct_empty<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    let result = lane.clear_direct().apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn clear_direct_empty_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    clear_direct_empty(lane, events).await;
}

#[tokio::test]
async fn clear_direct_empty_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    clear_direct_empty(lane, events).await;
}

#[tokio::test]
async fn clear_direct_empty_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    clear_direct_empty(lane, events).await;
}

async fn clear_direct_nonempty<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    update_direct(&lane, &mut events).await;
    let result = lane.clear_direct().apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Clear)));
}

#[tokio::test]
async fn clear_direct_nonempty_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    clear_direct_nonempty(lane, events).await;
}

#[tokio::test]
async fn clear_direct_nonempty_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    clear_direct_nonempty(lane, events).await;
}

#[tokio::test]
async fn clear_direct_nonempty_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    clear_direct_nonempty(lane, events).await;
}

#[tokio::test]
async fn get_value() {
    let (lane, mut events) = make_lane_model(Queue::default());
    update_direct(&lane, &mut events).await;

    let result1 = atomically(&lane.get(1), ExactlyOnce).await;

    assert!(matches!(result1, Ok(Some(v)) if *v == 5));

    let result2 = atomically(&lane.get(2), ExactlyOnce).await;

    assert!(matches!(result2, Ok(None)));
}

#[tokio::test]
async fn contains_key() {
    let (lane, mut events) = make_lane_model(Queue::default());
    update_direct(&lane, &mut events).await;

    let result1 = atomically(&lane.contains(1), ExactlyOnce).await;

    assert!(matches!(result1, Ok(true)));

    let result2 = atomically(&lane.contains(2), ExactlyOnce).await;

    assert!(matches!(result2, Ok(false)));
}

#[tokio::test]
async fn map_len() {
    let (lane, mut events) = make_lane_model(Queue::default());

    let result1 = atomically(&lane.len(), ExactlyOnce).await;

    assert!(matches!(result1, Ok(0)));

    update_direct(&lane, &mut events).await;

    let result2 = atomically(&lane.len(), ExactlyOnce).await;

    assert!(matches!(result2, Ok(1)));
}

#[tokio::test]
async fn map_is_empty() {
    let (lane, mut events) = make_lane_model(Queue::default());

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
    let (lane, mut events) = make_lane_model(Queue::default());

    populate(&lane, &mut events).await;

    let result = atomically(&lane.first(), ExactlyOnce).await;

    assert!(matches!(result, Ok(Some(v)) if *v == 7));
}

#[tokio::test]
async fn map_last() {
    let (lane, mut events) = make_lane_model(Queue::default());

    populate(&lane, &mut events).await;

    let result = atomically(&lane.last(), ExactlyOnce).await;

    assert!(matches!(result, Ok(Some(v)) if *v == 13));
}

async fn update_compound<Str>(lane: &MapLane<i32, i32>, events: &mut Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    let value = Arc::new(5);
    let result = atomically(&lane.update(1, value.clone()), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Update(1, v)) if Arc::ptr_eq(&v, &value)));
}

#[tokio::test]
async fn update_compound_queue() {
    let (lane, mut events) = make_lane_model(Queue::default());
    update_compound(&lane, &mut events).await;
}

#[tokio::test]
async fn update_compound_dropping() {
    let (lane, mut events) = make_lane_model(Dropping);
    update_compound(&lane, &mut events).await;
}

#[tokio::test]
async fn update_compound_buffered() {
    let (lane, mut events) = make_lane_model(Buffered::default());
    update_direct(&lane, &mut events).await;
}

async fn remove_compound_not_contained<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    let result = atomically(&lane.remove(1), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn remove_compound_not_contained_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    remove_compound_not_contained(lane, events).await;
}

#[tokio::test]
async fn remove_compound_not_contained_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    remove_compound_not_contained(lane, events).await;
}

#[tokio::test]
async fn remove_compound_not_contained_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    remove_compound_not_contained(lane, events).await;
}

async fn remove_compound_contained<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    update_direct(&lane, &mut events).await;
    let result = atomically(&lane.remove(1), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Remove(1))));
}

#[tokio::test]
async fn remove_compound_contained_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    remove_compound_contained(lane, events).await;
}

#[tokio::test]
async fn remove_compound_contained_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    remove_compound_contained(lane, events).await;
}

#[tokio::test]
async fn remove_compound_contained_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    remove_compound_contained(lane, events).await;
}

async fn clear_compound_empty<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    let result = atomically(&lane.clear(), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().now_or_never();
    assert!(event.is_none());
}

#[tokio::test]
async fn clear_compound_empty_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    clear_compound_empty(lane, events).await;
}

#[tokio::test]
async fn clear_compound_empty_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    clear_compound_empty(lane, events).await;
}

#[tokio::test]
async fn clear_compound_empty_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    clear_compound_empty(lane, events).await;
}

async fn clear_compound_nonempty<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    update_direct(&lane, &mut events).await;
    let result = atomically(&lane.clear(), ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Clear)));
}

#[tokio::test]
async fn clear_compound_nonempty_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    clear_compound_nonempty(lane, events).await;
}

#[tokio::test]
async fn clear_compound_nonempty_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    clear_compound_nonempty(lane, events).await;
}

#[tokio::test]
async fn clear_compound_nonempty_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    clear_compound_nonempty(lane, events).await;
}

async fn double_set<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
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
async fn double_set_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    double_set(lane, events).await;
}

#[tokio::test]
async fn double_set_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    double_set(lane, events).await;
}

#[tokio::test]
async fn double_set_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    double_set(lane, events).await;
}

async fn transaction_with_clear<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
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
async fn transaction_with_clear_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    transaction_with_clear(lane, events).await;
}

#[tokio::test]
async fn transaction_with_clear_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    transaction_with_clear(lane, events).await;
}

#[tokio::test]
async fn transaction_with_clear_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    transaction_with_clear(lane, events).await;
}

#[tokio::test]
async fn snapshot_map() {
    let (lane, mut events) = make_lane_model(Queue::default());

    populate(&lane, &mut events).await;

    let result = atomically(&lane.snapshot(), ExactlyOnce).await;

    let mut expected = HashMap::new();
    expected.insert(1, Arc::new(7));
    expected.insert(8, Arc::new(13));

    assert!(matches!(result, Ok(map) if &map == &expected));
}

async fn modify_if_defined_direct<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
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
async fn modify_if_defined_direct_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    modify_if_defined_direct(lane, events).await;
}

#[tokio::test]
async fn modify_if_defined_direct_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    modify_if_defined_direct(lane, events).await;
}

#[tokio::test]
async fn modify_if_defined_direct_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    modify_if_defined_direct(lane, events).await;
}

async fn modify_direct_some<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
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
async fn modify_direct_some_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    modify_direct_some(lane, events).await;
}

#[tokio::test]
async fn modify_direct_some_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    modify_direct_some(lane, events).await;
}

#[tokio::test]
async fn modify_direct_some_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    modify_direct_some(lane, events).await;
}

async fn modify_direct_none<Str>(lane: MapLane<i32, i32>, mut events: Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
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
async fn modify_direct_none_queue() {
    let (lane, events) = make_lane_model(Queue::default());
    modify_direct_none(lane, events).await;
}

#[tokio::test]
async fn modify_direct_none_dropping() {
    let (lane, events) = make_lane_model(Dropping);
    modify_direct_none(lane, events).await;
}

#[tokio::test]
async fn modify_direct_none_buffered() {
    let (lane, events) = make_lane_model(Buffered::default());
    modify_direct_none(lane, events).await;
}

#[tokio::test]
async fn checkpoint_map() {
    let (lane, mut events) = make_lane_model(Queue::default());

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
