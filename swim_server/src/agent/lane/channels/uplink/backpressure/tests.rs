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

use crate::agent::lane::channels::uplink::backpressure::{
    KeyedBackpressureConfig, SimpleBackpressureConfig,
};
use crate::agent::lane::channels::uplink::{UplinkError, UplinkMessage, ValueLaneEvent};
use futures::future::{join3, join_all};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use swim_async_runtime::time::timeout::timeout;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::future::item_sink;
use swim_warp::map::MapUpdate;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const TIMEOUT: Duration = Duration::from_secs(10);

fn simple_config() -> SimpleBackpressureConfig {
    SimpleBackpressureConfig {
        buffer_size: non_zero_usize!(2),
        yield_after: non_zero_usize!(256),
    }
}

fn keyed_config() -> KeyedBackpressureConfig {
    KeyedBackpressureConfig {
        buffer_size: non_zero_usize!(2),
        yield_after: non_zero_usize!(256),
        bridge_buffer_size: non_zero_usize!(1),
        cache_size: non_zero_usize!(4),
    }
}

type ValueIn = Result<UplinkMessage<ValueLaneEvent<i32>>, UplinkError>;
type ValueOut = UplinkMessage<ValueLaneEvent<i32>>;

#[tokio::test(flavor = "multi_thread")]
async fn value_uplink_backpressure_release_events() {
    let (in_tx, in_rx) = mpsc::channel::<ValueIn>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<ValueOut>(2);

    let relief_task = super::value_uplink_release_backpressure(
        ReceiverStream::new(in_rx),
        item_sink::for_mpsc_sender(out_tx),
        simple_config(),
    );

    let provide_task = async move {
        for i in 0..100 {
            assert!(in_tx
                .send(Ok(UplinkMessage::Event(ValueLaneEvent(Arc::new(i)))))
                .await
                .is_ok());
        }
    };

    let consume_task = async move {
        let mut current = -1;
        while let Some(msg) = out_rx.recv().await {
            match msg {
                UplinkMessage::Event(ValueLaneEvent(v)) => {
                    let n = *v;
                    assert!(n > current);
                    current = n;
                }
                _ => panic!("Unexpected output."),
            }
        }
        assert_eq!(current, 99);
    };

    let result = timeout(TIMEOUT, join3(relief_task, provide_task, consume_task)).await;
    assert!(matches!(result, Ok((Ok(_), _, _))));
}

#[tokio::test(flavor = "multi_thread")]
async fn value_uplink_backpressure_release_special() {
    let (in_tx, in_rx) = mpsc::channel::<ValueIn>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<ValueOut>(2);

    let relief_task = super::value_uplink_release_backpressure(
        ReceiverStream::new(in_rx),
        item_sink::for_mpsc_sender(out_tx),
        simple_config(),
    );

    let provide_task = async move {
        for i in 0..50 {
            assert!(in_tx
                .send(Ok(UplinkMessage::Event(ValueLaneEvent(Arc::new(i)))))
                .await
                .is_ok());
        }
        assert!(in_tx.send(Ok(UplinkMessage::Synced)).await.is_ok());
        for i in 50..100 {
            assert!(in_tx
                .send(Ok(UplinkMessage::Event(ValueLaneEvent(Arc::new(i)))))
                .await
                .is_ok());
        }
    };

    let consume_task = async move {
        let mut current = -1;
        let mut synced_seen = false;
        while let Some(msg) = out_rx.recv().await {
            match msg {
                UplinkMessage::Event(ValueLaneEvent(v)) => {
                    let n = *v;
                    assert!(n > current);
                    current = n;
                }
                UplinkMessage::Synced => {
                    assert!(!synced_seen);
                    synced_seen = true;
                    assert_eq!(current, 49);
                }
                _ => panic!("Unexpected output."),
            }
        }
        assert_eq!(current, 99);
        assert!(synced_seen)
    };

    let result = timeout(TIMEOUT, join3(relief_task, provide_task, consume_task)).await;
    assert!(matches!(result, Ok((Ok(_), _, _))));
}

#[tokio::test(flavor = "multi_thread")]
async fn value_uplink_backpressure_release_failure() {
    let (in_tx, in_rx) = mpsc::channel::<ValueIn>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<ValueOut>(2);

    let relief_task = super::value_uplink_release_backpressure(
        ReceiverStream::new(in_rx),
        item_sink::for_mpsc_sender(out_tx),
        simple_config(),
    );

    let provide_task = async move {
        for i in 0..5 {
            assert!(in_tx
                .send(Ok(UplinkMessage::Event(ValueLaneEvent(Arc::new(i)))))
                .await
                .is_ok());
        }
        assert!(in_tx
            .send(Err(UplinkError::LaneStoppedReporting))
            .await
            .is_ok());
    };

    let consume_task = async move {
        let mut current = -1;
        while let Some(msg) = out_rx.recv().await {
            match msg {
                UplinkMessage::Event(ValueLaneEvent(v)) => {
                    let n = *v;
                    assert!(n > current);
                    current = n;
                }
                _ => panic!("Unexpected output."),
            }
        }
        assert_eq!(current, 4);
    };

    let result = timeout(TIMEOUT, join3(relief_task, provide_task, consume_task)).await;
    assert!(matches!(
        result,
        Ok((Err(UplinkError::LaneStoppedReporting), _, _))
    ));
}

type MapIn = Result<UplinkMessage<MapUpdate<i32, i32>>, UplinkError>;
type MapOut = UplinkMessage<MapUpdate<i32, i32>>;

#[tokio::test(flavor = "multi_thread")]
async fn map_uplink_backpressure_release_events() {
    let (in_tx, in_rx) = mpsc::channel::<MapIn>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<MapOut>(2);

    let relief_task = super::map_uplink_release_backpressure(
        ReceiverStream::new(in_rx),
        item_sink::for_mpsc_sender(out_tx),
        keyed_config(),
    );

    let provide_tasks = join_all((0..3).into_iter().map(|n| {
        let tx = in_tx.clone();
        async move {
            for i in 0..100 {
                assert!(tx
                    .send(Ok(UplinkMessage::Event(MapUpdate::Update(n, Arc::new(i)))))
                    .await
                    .is_ok());
            }
        }
    }));
    drop(in_tx); //Required or the task will not terminate.

    let consume_task = async move {
        let mut current = HashMap::new();
        while let Some(msg) = out_rx.recv().await {
            match msg {
                UplinkMessage::Event(MapUpdate::Update(k, v)) => {
                    let n = *v;
                    if let Some(current_val) = current.get(&k) {
                        assert!(n > *current_val);
                    }
                    current.insert(k, n);
                }
                _ => panic!("Unexpected output."),
            }
        }
        let expected = vec![(0, 99), (1, 99), (2, 99)]
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(current, expected);
    };

    let result = timeout(TIMEOUT, join3(relief_task, provide_tasks, consume_task)).await;
    assert!(matches!(result, Ok((Ok(_), _, _))));
}

#[tokio::test(flavor = "multi_thread")]
async fn map_uplink_backpressure_release_special() {
    let (in_tx, in_rx) = mpsc::channel::<MapIn>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<MapOut>(2);

    let relief_task = super::map_uplink_release_backpressure(
        ReceiverStream::new(in_rx),
        item_sink::for_mpsc_sender(out_tx),
        keyed_config(),
    );

    let provide_task = async move {
        join_all((0..3).into_iter().map(|n| {
            let tx = in_tx.clone();
            async move {
                for i in 0..((n + 1) * 10) {
                    assert!(tx
                        .send(Ok(UplinkMessage::Event(MapUpdate::Update(n, Arc::new(i)))))
                        .await
                        .is_ok());
                }
            }
        }))
        .await;

        assert!(in_tx.send(Ok(UplinkMessage::Synced)).await.is_ok());

        join_all((0..3).into_iter().map(|n| {
            let tx = in_tx.clone();
            async move {
                for i in ((n + 1) * 10)..100 {
                    assert!(tx
                        .send(Ok(UplinkMessage::Event(MapUpdate::Update(n, Arc::new(i)))))
                        .await
                        .is_ok());
                }
            }
        }))
        .await;
    };

    let consume_task = async move {
        let mut current = HashMap::new();
        while let Some(msg) = out_rx.recv().await {
            match msg {
                UplinkMessage::Event(MapUpdate::Update(k, v)) => {
                    let n = *v;
                    if let Some(current_val) = current.get(&k) {
                        assert!(n > *current_val);
                    }
                    current.insert(k, n);
                }
                UplinkMessage::Synced => {
                    let expected = vec![(0, 9), (1, 19), (2, 29)]
                        .into_iter()
                        .collect::<HashMap<_, _>>();
                    assert_eq!(current, expected);
                }
                _ => panic!("Unexpected output."),
            }
        }
        let expected = vec![(0, 99), (1, 99), (2, 99)]
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(current, expected);
    };

    let result = timeout(TIMEOUT, join3(relief_task, provide_task, consume_task)).await;
    assert!(matches!(result, Ok((Ok(_), _, _))));
}

#[tokio::test(flavor = "multi_thread")]
async fn map_uplink_backpressure_release_failure() {
    let (in_tx, in_rx) = mpsc::channel::<MapIn>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<MapOut>(2);

    let relief_task = super::map_uplink_release_backpressure(
        ReceiverStream::new(in_rx),
        item_sink::for_mpsc_sender(out_tx),
        keyed_config(),
    );

    let provide_task = async move {
        for i in 0..4 {
            assert!(in_tx
                .send(Ok(UplinkMessage::Event(MapUpdate::Update(1, Arc::new(i)))))
                .await
                .is_ok());
        }
        assert!(in_tx
            .send(Err(UplinkError::LaneStoppedReporting))
            .await
            .is_ok());
    };

    let consume_task = async move {
        let mut current = HashMap::new();
        while let Some(msg) = out_rx.recv().await {
            match msg {
                UplinkMessage::Event(MapUpdate::Update(k, v)) => {
                    let n = *v;
                    if let Some(current_val) = current.get(&k) {
                        assert!(n > *current_val);
                    }
                    current.insert(k, n);
                }
                _ => panic!("Unexpected output."),
            }
        }
        let expected = vec![(1, 3)].into_iter().collect::<HashMap<_, _>>();
        assert_eq!(current, expected);
    };

    let result = timeout(TIMEOUT, join3(relief_task, provide_task, consume_task)).await;
    assert!(matches!(
        result,
        Ok((Err(UplinkError::LaneStoppedReporting), _, _))
    ));
}
