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
use crate::agent::lane::channels::uplink::{
    MapLaneUplink, PeelResult, Uplink, UplinkAction, UplinkError, UplinkMessage,
    UplinkStateMachine, ValueLaneUplink,
};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent, MapSubscriber};
use crate::agent::lane::model::value::ValueLane;
use crate::agent::lane::model::DeferredSubscription;
use crate::agent::lane::tests::ExactlyOnce;
use futures::future::join;
use futures::ready;
use futures::sink::drain;
use futures::{Stream, StreamExt};
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use stm::transaction::TransactionError;
use swim_form::structural::read::ReadError;
use swim_form::Form;
use swim_model::Value;
use swim_common::sink::item;
use swim_utilities::future::SwimStreamExt;
use swim_utilities::time::AtomicInstant;
use swim_utilities::trigger;
use swim_warp::model::map::MapUpdate;
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};
use tokio_stream::wrappers::ReceiverStream;

fn buffer_size() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}

fn make_subscribable<K, V>(buffer_size: NonZeroUsize) -> (MapLane<K, V>, MapSubscriber<K, V>)
where
    K: Form + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let (lane, rx) = MapLane::observable(buffer_size);
    (lane, MapSubscriber::new(rx.into_subscriber()))
}

struct ReportingStream<S> {
    notify: VecDeque<trigger::Sender>,
    values: S,
}

impl<S: Unpin> Stream for ReportingStream<S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let ReportingStream { notify, values } = self.get_mut();

        let v = ready!(values.poll_next_unpin(cx));
        if v.is_some() {
            if let Some(trigger) = notify.pop_front() {
                trigger.trigger();
            }
        }
        Poll::Ready(v)
    }
}

impl<S: Stream> ReportingStream<S> {
    pub fn new(inner: S, notify: Vec<trigger::Sender>) -> Self {
        ReportingStream {
            notify: notify.into_iter().collect(),
            values: inner,
        }
    }
}

#[tokio::test]
async fn uplink_not_linked() {
    let (lane, rx) = ValueLane::observable(0, buffer_size());

    let events = rx.into_stream();

    let (on_event_tx, on_event_rx) = trigger::trigger();

    let events = ReportingStream::new(events, vec![on_event_tx]);

    let (tx_action, rx_action) = mpsc::channel::<UplinkAction>(5);

    let uplink = Uplink::new(
        ValueLaneUplink::new(lane.clone(), None),
        ReceiverStream::new(rx_action).fuse(),
        events.fuse(),
    );

    let (tx_event, rx_event) = mpsc::channel(5);

    let uplinks_idle_since = Arc::new(AtomicInstant::new(Instant::now().into_std()));
    let uplink_task = uplink.run_uplink(item::for_mpsc_sender(tx_event), uplinks_idle_since);

    let send_task = async move {
        lane.store(12).await;
        assert!(on_event_rx.await.is_ok());
        assert!(tx_action.send(UplinkAction::Unlink).await.is_ok());
        ReceiverStream::new(rx_event).collect::<Vec<_>>().await
    };

    let (uplink_result, send_result) = join(
        timeout(Duration::from_secs(10), uplink_task),
        timeout(Duration::from_secs(10), send_task),
    )
    .await;

    assert!(matches!(uplink_result, Ok(Ok(_))));
    assert!(send_result.is_ok());
    assert!(matches!(
        send_result.unwrap().as_slice(),
        [UplinkMessage::Unlinked]
    ));
}

#[tokio::test]
async fn uplink_open_to_linked() {
    let (lane, rx) = ValueLane::observable(0, buffer_size());

    let events = rx.into_stream();

    let (on_event_tx_1, on_event_rx_1) = trigger::trigger();
    let (on_event_tx_2, on_event_rx_2) = trigger::trigger();

    let events = ReportingStream::new(events, vec![on_event_tx_1, on_event_tx_2]);

    let (tx_action, rx_action) = mpsc::channel::<UplinkAction>(5);

    let uplink = Uplink::new(
        ValueLaneUplink::new(lane.clone(), None),
        ReceiverStream::new(rx_action).fuse(),
        events.fuse(),
    );

    let (tx_event, rx_event) = mpsc::channel(5);

    let uplinks_idle_since = Arc::new(AtomicInstant::new(Instant::now().into_std()));
    let uplink_task = uplink.run_uplink(item::for_mpsc_sender(tx_event), uplinks_idle_since);

    let send_task = async move {
        lane.store(12).await;
        assert!(on_event_rx_1.await.is_ok());
        assert!(tx_action.send(UplinkAction::Link).await.is_ok());
        lane.store(25).await;
        assert!(on_event_rx_2.await.is_ok());
        assert!(tx_action.send(UplinkAction::Unlink).await.is_ok());
        ReceiverStream::new(rx_event).collect::<Vec<_>>().await
    };

    let (uplink_result, send_result) = join(
        timeout(Duration::from_secs(10), uplink_task),
        timeout(Duration::from_secs(10), send_task),
    )
    .await;

    assert!(matches!(uplink_result, Ok(Ok(_))));
    assert!(send_result.is_ok());
    assert!(matches!(
        send_result.unwrap().as_slice(),
        [
            UplinkMessage::Linked,
            UplinkMessage::Event(v),
            UplinkMessage::Unlinked
        ] if **v == 25
    ));
}

#[tokio::test]
async fn uplink_open_to_synced() {
    let (lane, rx) = ValueLane::observable(0, buffer_size());

    let events = rx.into_stream();

    let (on_event_tx, on_event_rx) = trigger::trigger();

    let events = ReportingStream::new(events, vec![on_event_tx]);

    let (tx_action, rx_action) = mpsc::channel::<UplinkAction>(5);

    let uplink = Uplink::new(
        ValueLaneUplink::new(lane.clone(), None),
        ReceiverStream::new(rx_action).fuse(),
        events.fuse(),
    );

    let (tx_event, rx_event) = mpsc::channel(5);

    let uplinks_idle_since = Arc::new(AtomicInstant::new(Instant::now().into_std()));
    let uplink_task = uplink.run_uplink(item::for_mpsc_sender(tx_event), uplinks_idle_since);

    let send_task = async move {
        lane.store(12).await;
        assert!(on_event_rx.await.is_ok());
        assert!(tx_action.send(UplinkAction::Sync).await.is_ok());
        assert!(tx_action.send(UplinkAction::Unlink).await.is_ok());
        ReceiverStream::new(rx_event).collect::<Vec<_>>().await
    };

    let (uplink_result, send_result) = join(
        timeout(Duration::from_secs(10), uplink_task),
        timeout(Duration::from_secs(10), send_task),
    )
    .await;

    assert!(matches!(uplink_result, Ok(Ok(_))));
    assert!(send_result.is_ok());
    assert!(matches!(
        send_result.unwrap().as_slice(),
        [
            UplinkMessage::Linked,
            UplinkMessage::Event(v),
            UplinkMessage::Synced,
            UplinkMessage::Unlinked
        ] if **v == 12
    ));
}

#[tokio::test]
async fn value_state_machine_message_for() {
    let lane = ValueLane::new(0);

    let uplink = ValueLaneUplink::new(lane, None);

    let event = Arc::new(4);

    let msg = uplink.message_for(event.clone());

    assert!(matches!(msg, Ok(Some(v)) if Arc::ptr_eq(&v.0, &event)));
}

#[tokio::test]
async fn value_state_machine_sync_from_var() {
    let (lane, rx) = ValueLane::observable(7, buffer_size());

    let events = rx.into_stream();

    let uplink = ValueLaneUplink::new(lane, None);

    let mut events = events.fuse();

    let sync_events = timeout(
        Duration::from_secs(10),
        uplink.sync_lane(&mut events).collect::<Vec<_>>(),
    )
    .await;

    assert!(sync_events.is_ok());

    let event_vec = sync_events.unwrap();

    assert!(
        matches!(event_vec.as_slice(), [PeelResult::Output(Ok(v)), PeelResult::Complete(_)] if **v == 7)
    );
}

#[tokio::test]
async fn value_state_machine_sync_from_events() {
    let lane = ValueLane::new(7);

    let uplink = ValueLaneUplink::new(lane.clone(), None);

    let (tx_fake, rx_fake) = mpsc::channel(5);

    let mut rx_fake = ReceiverStream::new(rx_fake).fuse();

    let _lock = lane.lock().await;

    let sync_task = timeout(
        Duration::from_secs(10),
        uplink.sync_lane(&mut rx_fake).collect::<Vec<_>>(),
    );

    let event = Arc::new(87);

    let send_task = tx_fake.send(event.clone());

    let (sync_result, send_result) = join(sync_task, send_task).await;

    assert!(send_result.is_ok());
    assert!(sync_result.is_ok());

    let event_vec = sync_result.unwrap();

    assert!(
        matches!(event_vec.as_slice(), [PeelResult::Output(Ok(v)), PeelResult::Complete(_)] if Arc::ptr_eq(&v.0, &event))
    );
}

#[tokio::test]
async fn map_state_machine_message_for() {
    let lane = MapLane::new();

    let map_uplink = MapLaneUplink::new(lane, 1, || ExactlyOnce, None);

    let value = Arc::new(4);

    let update = map_uplink.message_for(MapLaneEvent::Update(3, value.clone()));

    assert!(
        matches!(update, Ok(Some(MapUpdate::Update(Value::Int32Value(3), v))) if Arc::ptr_eq(&v, &value))
    );

    let remove = map_uplink.message_for(MapLaneEvent::Remove(2));
    assert!(matches!(
        remove,
        Ok(Some(MapUpdate::Remove(Value::Int32Value(2))))
    ));

    let clear = map_uplink.message_for(MapLaneEvent::Clear);
    assert!(matches!(clear, Ok(Some(MapUpdate::Clear))));

    let checkpoint = map_uplink.message_for(MapLaneEvent::Checkpoint(7));
    assert!(matches!(checkpoint, Ok(None)));
}

fn into_map(
    events: Vec<Result<MapUpdate<Value, i32>, UplinkError>>,
) -> Result<BTreeMap<i32, i32>, UplinkError> {
    let mut map = BTreeMap::new();

    for event in events.into_iter() {
        match event? {
            MapUpdate::Update(k, v) => {
                map.insert(i32::try_convert(k).unwrap(), *v);
            }
            MapUpdate::Remove(k) => {
                map.remove(&i32::try_convert(k).unwrap());
            }
            MapUpdate::Clear => {
                map.clear();
            }
            MapUpdate::Take(n) => {
                let discard = map.keys().skip(n).map(|k| *k).collect::<Vec<_>>();
                for k in discard {
                    map.remove(&k);
                }
            }
            MapUpdate::Drop(n) => {
                let discard = map.keys().take(n).map(|k| *k).collect::<Vec<_>>();
                for k in discard {
                    map.remove(&k);
                }
            }
        }
    }
    Ok(map)
}

#[tokio::test]
async fn map_state_machine_sync() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());

    let events = sub.subscribe().unwrap();
    let mut events = events.fuse();

    assert!(lane
        .update_direct(1, Arc::new(2))
        .apply(ExactlyOnce)
        .await
        .is_ok());
    assert!(lane
        .update_direct(2, Arc::new(5))
        .apply(ExactlyOnce)
        .await
        .is_ok());
    assert!((&mut events)
        .take(2)
        .never_error()
        .forward(drain())
        .await
        .is_ok());

    let map_uplink = MapLaneUplink::new(lane, 1, || ExactlyOnce, None);

    let sync_events = timeout(
        Duration::from_secs(10),
        map_uplink.sync_lane(&mut events).collect::<Vec<_>>(),
    )
    .await;

    assert!(sync_events.is_ok());

    let sync_vec = sync_events.unwrap();

    let sync_values = sync_vec
        .into_iter()
        .filter_map(|peel_result| peel_result.output())
        .collect();

    let results = into_map(sync_values);

    assert!(results.is_ok());

    let mut expected = BTreeMap::new();
    expected.insert(1, 2);
    expected.insert(2, 5);
    assert_eq!(results.unwrap(), expected);
}

#[test]
fn uplink_error_display() {
    assert_eq!(
        format!("{}", UplinkError::ChannelDropped),
        "Uplink send channel was dropped."
    );
    assert_eq!(
        format!("{}", UplinkError::LaneStoppedReporting),
        "The lane stopped reporting its state."
    );
    assert_eq!(format!("{}", UplinkError::FailedTransaction(TransactionError::InvalidRetry)), 
               "The uplink failed to execute a transaction: Retry on transaction with no data dependencies.");
    assert_eq!(
        format!(
            "{}",
            UplinkError::InconsistentForm(ReadError::UnexpectedItem)
        ),
        "A form implementation used by a lane is inconsistent: Unexpected item in record."
    );
    assert_eq!(
        format!("{}", UplinkError::ChannelDropped),
        "Uplink send channel was dropped."
    );
    assert_eq!(
        format!("{}", UplinkError::FailedToStart(2)),
        "Uplink failed to start after 2 attempts."
    );
}

#[test]
fn uplink_error_from_map_sync_error() {
    let err1: UplinkError =
        MapLaneSyncError::FailedTransaction(TransactionError::InvalidRetry).into();
    assert!(matches!(
        err1,
        UplinkError::FailedTransaction(TransactionError::InvalidRetry)
    ));
    let err2: UplinkError = MapLaneSyncError::InconsistentForm(ReadError::UnexpectedItem).into();
    assert!(matches!(
        err2,
        UplinkError::InconsistentForm(ReadError::UnexpectedItem)
    ));
}
