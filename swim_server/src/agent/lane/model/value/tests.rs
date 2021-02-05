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

use super::*;
use crate::agent::lane::tests::ExactlyOnce;
use stm::transaction::atomically;
use stm::var::observer::ObserverSubscriber;
use utilities::sync::topic::TryRecvError;

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
