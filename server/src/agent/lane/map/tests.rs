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

use crate::agent::lane::map::summary::MapLaneEvent;
use futures::{Stream, StreamExt, FutureExt};
use crate::agent::lane::map::{MapLane, make_lane};
use crate::agent::lane::tests::ExactlyOnce;
use std::sync::Arc;
use crate::agent::lane::strategy::{Queue, Dropping, Buffered};
use stm::transaction::atomically;

async fn update_direct<Str>(lane: &MapLane<i32, i32>,
                            events: &mut Str)
where
    Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{
    let value = Arc::new(5);
    let result = lane.update_direct(1, value.clone()).apply(ExactlyOnce).await;
    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(MapLaneEvent::Update(1, v)) if Arc::ptr_eq(&v, &value)));
}

#[tokio::test]
async fn update_direct_queue() {
    let (lane, mut events) = make_lane(Queue::default());
    update_direct(&lane, &mut events).await;
}

#[tokio::test]
async fn update_direct_dropping() {
    let (lane, mut events) = make_lane(Dropping);
    update_direct(&lane, &mut events).await;
}

#[tokio::test]
async fn update_direct_buffered() {
    let (lane, mut events) = make_lane(Buffered::default());
    update_direct(&lane, &mut events).await;
}

async fn remove_direct_not_contained<Str>(lane: MapLane<i32, i32>,
                            mut events: Str)
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
    let (lane, events) = make_lane(Queue::default());
    remove_direct_not_contained(lane, events).await;
}

#[tokio::test]
async fn remove_direct_not_contained_dropping() {
    let (lane, events) = make_lane(Dropping);
    remove_direct_not_contained(lane, events).await;
}

#[tokio::test]
async fn remove_direct_not_contained_buffered() {
    let (lane, events) = make_lane(Buffered::default());
    remove_direct_not_contained(lane, events).await;
}

async fn remove_direct_contained<Str>(lane: MapLane<i32, i32>,
                                          mut events: Str)
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
    let (lane, events) = make_lane(Queue::default());
    remove_direct_contained(lane, events).await;
}

#[tokio::test]
async fn remove_direct_contained_dropping() {
    let (lane, events) = make_lane(Dropping);
    remove_direct_contained(lane, events).await;
}

#[tokio::test]
async fn remove_direct_contained_buffered() {
    let (lane, events) = make_lane(Buffered::default());
    remove_direct_contained(lane, events).await;
}

async fn clear_direct_empty<Str>(lane: MapLane<i32, i32>,
                          mut events: Str)
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
    let (lane, events) = make_lane(Queue::default());
    clear_direct_empty(lane, events).await;
}

#[tokio::test]
async fn clear_direct_empty_dropping() {
    let (lane, events) = make_lane(Dropping);
    clear_direct_empty(lane, events).await;
}

#[tokio::test]
async fn clear_direct_empty_buffered() {
    let (lane, events) = make_lane(Buffered::default());
    clear_direct_empty(lane, events).await;
}

async fn clear_direct_nonempty<Str>(lane: MapLane<i32, i32>,
                                 mut events: Str)
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
    let (lane, events) = make_lane(Queue::default());
    clear_direct_nonempty(lane, events).await;
}

#[tokio::test]
async fn clear_direct_nonempty_dropping() {
    let (lane, events) = make_lane(Dropping);
    clear_direct_nonempty(lane, events).await;
}

#[tokio::test]
async fn clear_direct_nonempty_buffered() {
    let (lane, events) = make_lane(Buffered::default());
    clear_direct_nonempty(lane, events).await;
}

#[tokio::test]
async fn get_value() {
    let (lane, mut events) = make_lane(Queue::default());
    update_direct(&lane, &mut events).await;

    let result1 = atomically(&lane.get(1), ExactlyOnce).await;

    assert!(matches!(result1, Ok(Some(v)) if *v == 5));

    let result2 = atomically(&lane.get(2), ExactlyOnce).await;

    assert!(matches!(result2, Ok(None)));
}

#[tokio::test]
async fn contains_key() {
    let (lane, mut events) = make_lane(Queue::default());
    update_direct(&lane, &mut events).await;

    let result1 = atomically(&lane.contains(1), ExactlyOnce).await;

    assert!(matches!(result1, Ok(true)));

    let result2 = atomically(&lane.contains(2), ExactlyOnce).await;

    assert!(matches!(result2, Ok(false)));
}

#[tokio::test]
async fn map_len() {
    let (lane, mut events) = make_lane(Queue::default());

    let result1 = atomically(&lane.len(), ExactlyOnce).await;

    assert!(matches!(result1, Ok(0)));

    update_direct(&lane, &mut events).await;

    let result2 = atomically(&lane.len(), ExactlyOnce).await;

    assert!(matches!(result2, Ok(1)));
}

#[tokio::test]
async fn map_is_empty() {
    let (lane, mut events) = make_lane(Queue::default());

    let result1 = atomically(&lane.is_empty(), ExactlyOnce).await;

    assert!(matches!(result1, Ok(true)));

    update_direct(&lane, &mut events).await;

    let result2 = atomically(&lane.is_empty(), ExactlyOnce).await;

    assert!(matches!(result2, Ok(false)));
}

async fn populate<Str>(lane: &MapLane<i32, i32>,
                            events: &mut Str)
    where
        Str: Stream<Item = MapLaneEvent<i32, i32>> + Unpin,
{

    let result = lane.update_direct(1, Arc::new(7)).apply(ExactlyOnce).await;
    assert!(result.is_ok());
    let result = lane.update_direct(8, Arc::new(13)).apply(ExactlyOnce).await;
    assert!(result.is_ok());
    events.take(2).collect::<Vec<_>>().await;
}

#[tokio::test]
async fn map_first() {
    let (lane, mut events) = make_lane(Queue::default());

    populate(&lane, &mut events).await;

    let result = atomically(&lane.first(), ExactlyOnce).await;

    assert!(matches!(result, Ok(Some(v)) if *v == 7));
}

#[tokio::test]
async fn map_last() {
    let (lane, mut events) = make_lane(Queue::default());

    populate(&lane, &mut events).await;

    let result = atomically(&lane.last(), ExactlyOnce).await;

    assert!(matches!(result, Ok(Some(v)) if *v == 13));
}
