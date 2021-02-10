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

use crate::agent::lane::channels::uplink::{ValueLaneUplink, Uplink, UplinkMessage, ValueLaneEvent, UplinkAction, UplinkError};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use futures::StreamExt;
use swim_common::sink::item;
use tokio::task::JoinHandle;
use crate::agent::lane::model::value::ValueLane;
use utilities::sync::trigger;
use std::time::Duration;
use stm::var::observer::ObserverSubscriber;
use std::num::NonZeroUsize;
use futures::future::join4;
use std::sync::Arc;

struct UplinkComponents {
    action_tx: mpsc::Sender<UplinkAction>,
    event_rx: mpsc::Receiver<UplinkMessage<ValueLaneEvent<i32>>>,
    handle: JoinHandle<Result<(), UplinkError>>
}

async fn make_uplink(lane: &ValueLane<i32>, subscriber: &ObserverSubscriber<i32>) -> UplinkComponents {

    let state_machine = ValueLaneUplink::new(lane.clone(), None);
    let (action_tx, action_rx) = mpsc::channel::<UplinkAction>(8);
    let (event_tx, event_rx) = mpsc::channel::<UplinkMessage<ValueLaneEvent<i32>>>(8);
    let updates = subscriber.subscribe().unwrap().into_stream().fuse();
    let uplink = Uplink::new(state_machine, action_rx.fuse(), updates);

    let uplink_task = uplink.run_uplink(item::for_mpsc_sender(event_tx));
    let handle = tokio::spawn(uplink_task);

    UplinkComponents {
        action_tx,
        event_rx,
        handle,
    }
}

async fn uplink_spawner(lane: ValueLane<i32>,
                        subscriber: ObserverSubscriber<i32>,
                        mut requests: mpsc::Receiver<oneshot::Sender<UplinkComponents>>) {
    while let Some(tx) = requests.recv().await {
        let components = make_uplink(&lane, &subscriber).await;
        tx.send(components).ok().unwrap();
    }
}

impl UplinkComponents {

    async fn run_uplink(self, index: usize, on_synced: trigger::Sender, collect: Arc<parking_lot::Mutex<Vec<String>>>) {
        let UplinkComponents {
            action_tx,
            mut event_rx,
            handle: _handle,
        } = self;

        action_tx.send(UplinkAction::Sync).await.unwrap();

        let mut on_synced = Some(on_synced);

        while let Some(event) = event_rx.recv().await {
            collect.lock().push(format!("Uplink {} output {:?}", index, &event));
            match event {
                UplinkMessage::Event(ValueLaneEvent(ev)) if *ev == -1 => {
                    break;
                },
                UplinkMessage::Synced => {
                    if let Some(on_synced) = on_synced.take() {
                        on_synced.trigger();
                    }
                },
                _ => {}
            }
        }
        let _ = action_tx.send(UplinkAction::Unlink).await;
    }

}

async fn set_task(lane: ValueLane<i32>,
                  limit: i32,
                  delay: Option<Duration>,
                  trigger_level: i32,
                  on_triggered: trigger::Sender,
                  collect: Arc<parking_lot::Mutex<Vec<String>>>) {
    let mut on_triggered = Some(on_triggered);
    for i in 1..limit {
        if let Some(d) = &delay {
            tokio::time::sleep(*d).await;
        }
        collect.lock().push(format!("Setting {}", i));
        lane.store(i).await;
        if i == trigger_level {
            if let Some(on_triggered) = on_triggered.take() {
                on_triggered.trigger();
            }
        }
    }
    lane.store(-1).await;
}

async fn new_uplink(req_tx: &mut mpsc::Sender<oneshot::Sender<UplinkComponents>>) -> UplinkComponents {
    let (tx, rx) = oneshot::channel();
    req_tx.send(tx).await.ok().unwrap();
    rx.await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_two_downlinks() {
    let collect = Arc::new(parking_lot::Mutex::new(vec![]));
    let (lane, observer) = ValueLane::observable(0, NonZeroUsize::new(8).unwrap());
    let sub = observer.subscriber();
    drop(observer);
    let (mut req_tx, req_rx) = mpsc::channel(8);

    let task_timeout = Duration::from_secs(30);

    let spawn_task = tokio::time::timeout(task_timeout, tokio::spawn(uplink_spawner(lane.clone(), sub, req_rx)));

    let (wait_tx, wait_rx) = trigger::trigger();
    let setter = tokio::time::timeout(task_timeout, tokio::spawn(set_task(lane.clone(), 20, Some(Duration::from_secs(1)), 3, wait_tx, collect.clone())));

    let uplink1 = new_uplink(&mut req_tx).await;
    let (sync_tx1, sync_rx1) = trigger::trigger();
    let uplink1_task = tokio::time::timeout(task_timeout, tokio::spawn(uplink1.run_uplink(1, sync_tx1, collect.clone())));
    sync_rx1.await.unwrap();
    wait_rx.await.unwrap();

    let uplink2 = new_uplink(&mut req_tx).await;
    drop(req_tx);
    let (sync_tx2, _sync_rx2) = trigger::trigger();
    let uplink2_task = tokio::time::timeout(task_timeout, tokio::spawn(uplink2.run_uplink(2, sync_tx2, collect.clone())));

    let result = join4(spawn_task, setter, uplink1_task, uplink2_task).await;

    assert!(matches!(result, (Ok(Ok(_)), Ok(Ok(_)), Ok(Ok(_)), Ok(Ok(_)))));

    let lock = collect.lock();

    for line in lock.iter() {
        println!("{}", line);
    }
}