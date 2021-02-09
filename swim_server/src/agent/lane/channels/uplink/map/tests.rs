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

use crate::agent::lane::channels::uplink::map::MapLaneSyncError;
use crate::agent::lane::model::map::{MapLane, MapLaneEvent, MapSubscriber};
use crate::agent::lane::model::DeferredSubscription;
use crate::agent::lane::tests::ExactlyOnce;
use futures::future::{join, ready, Ready};
use futures::sink::drain;
use futures::stream::{repeat, Repeat};
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stm::transaction::RetryManager;
use tokio::sync::mpsc;
use tokio::time::timeout;
use utilities::future::SwimStreamExt;
use utilities::sync::trigger;

fn buffer_size() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}

struct Forever;

impl RetryManager for Forever {
    type ContentionManager = Repeat<()>;
    type RetryFut = Ready<bool>;

    fn contention_manager(&self) -> Self::ContentionManager {
        repeat(())
    }

    fn retry(&mut self) -> Self::RetryFut {
        ready(true)
    }
}

async fn populate_map<Events>(lane: &MapLane<i32, i32>, events: Events)
where
    Events: Stream<Item = MapLaneEvent<i32, i32>>,
{
    assert!(lane
        .update_direct(1, Arc::new(2))
        .apply(ExactlyOnce)
        .await
        .is_ok());
    assert!(lane
        .update_direct(2, Arc::new(4))
        .apply(ExactlyOnce)
        .await
        .is_ok());
    assert!(lane
        .update_direct(3, Arc::new(6))
        .apply(ExactlyOnce)
        .await
        .is_ok());

    let _ = events.take(3).never_error().forward(drain()).await;
}

fn into_map(
    events: Vec<Result<MapLaneEvent<i32, i32>, MapLaneSyncError>>,
) -> Result<HashMap<i32, i32>, MapLaneSyncError> {
    let mut map = HashMap::new();

    for event in events.into_iter() {
        match event? {
            MapLaneEvent::Update(k, v) => {
                map.insert(k, *v);
            }
            MapLaneEvent::Remove(k) => {
                map.remove(&k);
            }
            MapLaneEvent::Clear => {
                map.clear();
            }
            MapLaneEvent::Checkpoint(_) => {}
        }
    }

    Ok(map)
}

#[tokio::test]
async fn sync_map_lane_simple() {
    let (lane, rx) = MapLane::observable(buffer_size());

    let subscriber = MapSubscriber::new(rx.into_subscriber());

    let mut events = subscriber.subscribe().unwrap();

    populate_map(&lane, &mut events).await;

    let sync_stream = super::sync_map_lane(1, &lane, &mut events, ExactlyOnce);

    let sync_events = timeout(Duration::from_secs(10), sync_stream.collect::<Vec<_>>()).await;

    assert!(sync_events.is_ok());

    let sync_map = into_map(sync_events.unwrap());
    let mut expected = HashMap::new();
    expected.insert(1, 2);
    expected.insert(2, 4);
    expected.insert(3, 6);

    assert!(matches!(sync_map, Ok(m) if m == expected));
}

#[tokio::test]
async fn sync_map_lane_replace_value() {
    let (lane, rx) = MapLane::observable(buffer_size());

    let subscriber = MapSubscriber::new(rx.into_subscriber());

    let mut events = subscriber.subscribe().unwrap();

    populate_map(&lane, &mut events).await;

    let (fake_tx, fake_rx) = mpsc::channel(5);

    let mut fake_rx = fake_rx.fuse();

    // Locking a value in the map means that reads cannot complete allowing us to reliably provide
    // a replacement value by injecting a fake event.
    let lock = lane.lock(&2).await;

    // It is necessary for the checkpoint transaction to retry forever due to the locked value.
    let sync_stream = super::sync_map_lane(1, &lane, &mut fake_rx, Forever);

    let (release, tripped) = trigger::trigger();

    assert!(lock.is_some());

    let sync_consumer = async move {
        pin_mut!(sync_stream);
        let mut results = vec![];
        results.push(sync_stream.next().await.unwrap());
        release.trigger();
        while let Some(event) = sync_stream.next().await {
            results.push(event);
        }
        results
    };

    let fake_tx_copy = fake_tx.clone();

    let stream_faker = async move {
        let event = events.next().await.unwrap();
        if matches!(&event, MapLaneEvent::Checkpoint(1)) {
            assert!(fake_tx_copy.send(event).await.is_ok());
            // Wait until the sync process has started otherwise our fake even will be ignored.
            let _ = tripped.await;
            assert!(fake_tx_copy
                .send(MapLaneEvent::Update(2, Arc::new(17)))
                .await
                .is_ok());
        } else {
            panic!("Unexpected event: {:?}", event);
        }
    };

    let (faker_result, sync_results) = join(
        timeout(Duration::from_secs(10), stream_faker),
        timeout(Duration::from_secs(10), sync_consumer),
    )
    .await;

    assert!(faker_result.is_ok());
    assert!(sync_results.is_ok());
    let sync_map = into_map(sync_results.unwrap());

    let mut expected = HashMap::new();
    expected.insert(1, 2);
    expected.insert(2, 17);
    expected.insert(3, 6);
    assert!(matches!(sync_map, Ok(m) if m == expected));
}

#[tokio::test]
async fn sync_map_lane_remove_value() {
    let (lane, rx) = MapLane::observable(buffer_size());

    let subscriber = MapSubscriber::new(rx.into_subscriber());

    let mut events = subscriber.subscribe().unwrap();

    populate_map(&lane, &mut events).await;

    let (fake_tx, fake_rx) = mpsc::channel(5);

    let mut fake_rx = fake_rx.fuse();

    // Locking a value in the map means that reads cannot complete allowing us to reliably provide
    // a replacement value by injecting a fake event.
    let lock = lane.lock(&2).await;

    // It is necessary for the checkpoint transaction to retry forever due to the locked value.
    let sync_stream = super::sync_map_lane(1, &lane, &mut fake_rx, Forever);

    let (release, tripped) = trigger::trigger();

    assert!(lock.is_some());

    let sync_consumer = async move {
        pin_mut!(sync_stream);
        let mut results = vec![];
        results.push(sync_stream.next().await.unwrap());
        release.trigger();
        while let Some(event) = sync_stream.next().await {
            results.push(event);
        }
        results
    };

    let fake_tx_copy = fake_tx.clone();

    let stream_faker = async move {
        let event = events.next().await.unwrap();
        if matches!(&event, MapLaneEvent::Checkpoint(1)) {
            assert!(fake_tx_copy.send(event).await.is_ok());
            // Wait until the sync process has started otherwise our fake even will be ignored.
            let _ = tripped.await;
            assert!(fake_tx_copy.send(MapLaneEvent::Remove(2)).await.is_ok());
        } else {
            panic!("Unexpected event: {:?}", event);
        }
    };

    let (faker_result, sync_results) = join(
        timeout(Duration::from_secs(10), stream_faker),
        timeout(Duration::from_secs(10), sync_consumer),
    )
    .await;

    assert!(faker_result.is_ok());
    assert!(sync_results.is_ok());
    let sync_map = into_map(sync_results.unwrap());

    let mut expected = HashMap::new();
    expected.insert(1, 2);
    expected.insert(3, 6);
    assert!(matches!(sync_map, Ok(m) if m == expected));
}

#[tokio::test]
async fn sync_map_lane_clear() {
    let (lane, rx) = MapLane::observable(buffer_size());

    let subscriber = MapSubscriber::new(rx.into_subscriber());

    let mut events = subscriber.subscribe().unwrap();
    populate_map(&lane, &mut events).await;

    let (fake_tx, fake_rx) = mpsc::channel(5);

    let mut fake_rx = fake_rx.fuse();

    // Locking a value in the map means that reads cannot complete allowing us to reliably provide
    // a replacement value by injecting a fake event.
    let lock = lane.lock(&2).await;

    // It is necessary for the checkpoint transaction to retry forever due to the locked value.
    let sync_stream = super::sync_map_lane(1, &lane, &mut fake_rx, Forever);

    let (release, tripped) = trigger::trigger();

    assert!(lock.is_some());

    let sync_consumer = async move {
        pin_mut!(sync_stream);
        let mut results = vec![];
        results.push(sync_stream.next().await.unwrap());
        release.trigger();
        while let Some(event) = sync_stream.next().await {
            results.push(event);
        }
        results
    };

    let fake_tx_copy = fake_tx.clone();

    let stream_faker = async move {
        let event = events.next().await.unwrap();
        if matches!(&event, MapLaneEvent::Checkpoint(1)) {
            assert!(fake_tx_copy.send(event).await.is_ok());
            // Wait until the sync process has started otherwise our fake even will be ignored.
            let _ = tripped.await;
            assert!(fake_tx_copy.send(MapLaneEvent::Clear).await.is_ok());
        } else {
            panic!("Unexpected event: {:?}", event);
        }
    };

    let (faker_result, sync_results) = join(
        timeout(Duration::from_secs(10), stream_faker),
        timeout(Duration::from_secs(10), sync_consumer),
    )
    .await;

    assert!(faker_result.is_ok());
    assert!(sync_results.is_ok());
    let sync_map = into_map(sync_results.unwrap());

    assert!(matches!(sync_map, Ok(m) if m.is_empty()));
}
