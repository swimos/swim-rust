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
use crate::agent::lane::tests::ExactlyOnce;
use futures::future::join;
use server_store::agent::lane::error::StoreErrorHandler;
use server_store::plane::mock::MockPlaneStore;
use server_store::{StoreEngine, StoreKey};
use std::sync::Mutex;
use std::time::Duration;
use stm::transaction::atomically;
use stm::var::observer::ObserverSubscriber;
use swim_store::{serialize, EngineInfo, StoreError};
use swim_utilities::sync::topic::TryRecvError;
use swim_utilities::trigger;
use tokio::sync::mpsc;

fn buffer_size() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}

fn make_subscribable<T>(init: T, buffer_size: NonZeroUsize) -> (ValueLane<T>, ObserverSubscriber<T>)
where
    T: Send + Sync + 'static,
{
    let (lane, rx) = ValueLane::observable(init, buffer_size);
    (lane, rx.into_subscriber())
}

#[tokio::test]
async fn value_lane_get() {
    let (lane, sub) = make_subscribable::<i32>(0, buffer_size());
    let mut events = sub.subscribe().unwrap();

    let result = atomically(&lane.get(), ExactlyOnce).await;

    assert!(matches!(result, Ok(v) if v == Arc::new(0)));

    let event = events.try_recv();
    assert!(matches!(event, Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn value_lane_set() {
    let (lane, sub) = make_subscribable::<i32>(0, buffer_size());
    let mut events = sub.subscribe().unwrap();

    let result = atomically(&lane.set(1), ExactlyOnce).await;

    assert!(result.is_ok());

    let event = events.recv().await;
    assert!(matches!(event, Some(v) if *v == 1));
}

#[tokio::test]
async fn value_lane_compound_transaction() {
    let (lane, sub) = make_subscribable::<i32>(5, buffer_size());
    let mut events = sub.subscribe().unwrap();

    let stm = lane
        .get()
        .and_then(|i| lane.set(-1).followed_by(lane.set(*i + 1)));

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(result.is_ok());

    let event = events.recv().await;
    assert!(matches!(event, Some(v) if *v == 6));

    let event2 = events.try_recv();
    assert!(matches!(event2, Err(TryRecvError::NoValue)));
}

#[tokio::test(flavor = "multi_thread")]
async fn value_lane_subscribe() {
    let (lane, observer) = ValueLane::observable(0, NonZeroUsize::new(8).unwrap());

    let sub = observer.subscriber();

    let (comm_tx, mut comm_rx) = mpsc::channel::<trigger::Sender>(2);

    let start_task = |mut rx: Observer<i32>| {
        let task = async move { while let Some(_) = rx.recv().await {} };
        tokio::spawn(task);
    };

    start_task(observer);

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
            lane.store(i).await;
        }
    };

    let t1 = tokio::spawn(rx_spawner);
    let t2 = tokio::spawn(gen_task);
    let result = tokio::time::timeout(Duration::from_secs(5), join(t1, t2)).await;
    assert!(matches!(result, Ok((Ok(_), Ok(_)))));
}

#[derive(Clone, Debug)]
struct TrackingValueStore {
    value: Arc<Mutex<Option<Vec<u8>>>>,
}

impl NodeStore for TrackingValueStore {
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
        _prefix: StoreKey,
        _map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        panic!("Unexpected snapshot request")
    }
}

impl StoreEngine for TrackingValueStore {
    fn put(&self, _key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        self.value.lock().unwrap().replace(value.to_vec());
        Ok(())
    }

    fn get(&self, _key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.value.lock().unwrap().clone())
    }

    fn delete(&self, _key: StoreKey) -> Result<(), StoreError> {
        self.value.lock().unwrap().take();
        Ok(())
    }
}

#[tokio::test]
async fn io() {
    let store_initial = "loaded".to_string();
    let store = TrackingValueStore {
        value: Arc::new(Mutex::new(Some(serialize(&store_initial).unwrap()))),
    };

    let model = ValueDataModel::<TrackingValueStore, String>::new(store, 0);
    let (lane, observer) = ValueLane::<String>::store_observable(
        &model,
        NonZeroUsize::new(8).unwrap(),
        Default::default(),
    );
    let observer_stream = observer.into_stream();

    let store_io = ValueLaneStoreIo::new(observer_stream, model);
    let _task_handle = tokio::spawn(store_io.attach(StoreErrorHandler::new(0)));

    let lane_value = lane.load().await;
    assert_eq!(*lane_value, store_initial);
}
