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

use super::*;
use crate::stores::lane::observer::StoreObserver;
use crate::stores::lane::value::mem::ValueDataMemStore;
use crate::stores::node::NodeStore;
use futures::future::join;
use std::num::NonZeroUsize;
use std::time::Duration;
use store::mock::MockNodeStore;
use tokio::sync::mpsc;
use utilities::sync::topic::TryRecvError;
use utilities::sync::trigger;

fn buffer_size() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}

fn make_subscribable<T>(init: T, buffer_size: NonZeroUsize) -> (ValueLane<T>, StoreObserver<T>)
where
    T: Send + Sync + Default + Serialize + DeserializeOwned + 'static,
{
    let (store, rx) = ValueDataMemStore::observable(init, buffer_size);
    let model = ValueDataModel::Mem(store);
    let lane = ValueLane::new(model);

    (lane, rx)
}

#[tokio::test]
async fn value_lane_get() {
    let (lane, receiver) = make_subscribable::<i32>(0, buffer_size());
    let subscriber = receiver.subscriber();
    let mut events = subscriber.subscribe().unwrap();
    let result = lane.get().await;

    assert!(matches!(result, Ok(v) if v == Arc::new(0)));

    let event = events.try_recv();
    assert!(matches!(event, Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn value_lane_set() {
    let (lane, receiver) = make_subscribable::<i32>(0, buffer_size());
    let subscriber = receiver.subscriber();
    let mut events = subscriber.subscribe().unwrap();

    let result = lane.set(1).await;

    assert!(result.is_ok());

    let event = events.recv().await;
    assert!(matches!(event, Some(v) if *v == 1));
}

// todo
// #[tokio::test]
// async fn value_lane_compound_transaction() {
//     let (lane, receiver) = make_subscribable::<i32>(5, buffer_size());
//     let subscriber = receiver.subscriber();
//     let mut events = subscriber.subscribe().unwrap();
//
//     let stm = lane
//         .get()
//         .and_then(|i| lane.set(-1).followed_by(lane.set(*i + 1)));
//
//     let result = atomically(&stm, ExactlyOnce).await;
//
//     assert!(result.is_ok());
//
//     let event = events.recv().await;
//     assert!(matches!(event, Some(v) if v.as_ref() == &6));
//
//     let event2 = events.try_recv();
//     assert!(matches!(event2, Err(TryRecvError::NoValue)));
// }

#[tokio::test(flavor = "multi_thread")]
async fn value_lane_subscribe() {
    let store = MockNodeStore;
    let (model, rx) =
        store.observable_value_lane_store("test", true, NonZeroUsize::new(8).unwrap(), 0);
    let lane = ValueLane::new(model);

    let sub = rx.subscriber();

    let (comm_tx, mut comm_rx) = mpsc::channel::<trigger::Sender>(2);

    let start_task = |mut rx: StoreObserver<i32>| {
        let task = async move { while let Some(_) = rx.recv().await {} };
        tokio::spawn(task);
    };

    start_task(rx);

    let rx_spawner = async move {
        while let Some(trigger) = comm_rx.recv().await {
            start_task(sub.subscribe().unwrap());
            trigger.trigger();
        }
    };

    let gen_task = async move {
        for i in 1..20 {
            if i % 5 == 0 {
                let (task_tx, task_rx) = trigger::trigger();
                comm_tx.send(task_tx).await.unwrap();
                task_rx.await.unwrap();
            }
            lane.store(i).await.unwrap();
        }
    };

    let t1 = tokio::spawn(rx_spawner);
    let t2 = tokio::spawn(gen_task);
    let result = tokio::time::timeout(Duration::from_secs(5), join(t1, t2)).await;
    assert!(matches!(result, Ok((Ok(_), Ok(_)))));
}
