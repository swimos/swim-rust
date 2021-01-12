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

use tokio::sync::mpsc;
use swim_common::sink::item;
use crate::agent::lane::channels::uplink::backpressure::SimpleBackpressureConfig;
use futures_util::core_reexport::num::NonZeroUsize;
use crate::agent::lane::channels::uplink::{UplinkMessage, ValueLaneEvent, UplinkError};
use std::sync::Arc;
use futures::future::join3;

fn simple_config() -> SimpleBackpressureConfig {
    SimpleBackpressureConfig {
        buffer_size: NonZeroUsize::new(2).unwrap(),
        yield_after: NonZeroUsize::new(256).unwrap(),
    }
}

type ValueIn = Result<UplinkMessage<ValueLaneEvent<i32>>, UplinkError>;
type ValueOut = UplinkMessage<ValueLaneEvent<i32>>;

#[tokio::test(flavor = "multi_thread")]
async fn value_uplink_backpressure_release_events() {

    let (in_tx, in_rx) = mpsc::channel::<ValueIn>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<ValueOut>(2);

    let relief_task = super::value_uplink_release_backpressure(in_rx, item::for_mpsc_sender(out_tx), simple_config());

    let provide_task = async move {

        for i in 0..100 {
            assert!(in_tx.send(Ok(UplinkMessage::Event(ValueLaneEvent(Arc::new(i))))).await.is_ok());
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
                },
                _ => panic!("Unexpected output.")
            }
        }
        assert_eq!(current, 99);
    };

    assert!(matches!(join3(relief_task, provide_task, consume_task).await, (Ok(_), _, _)));

}

#[tokio::test(flavor = "multi_thread")]
async fn value_uplink_backpressure_release_special() {

    let (in_tx, in_rx) = mpsc::channel::<ValueIn>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<ValueOut>(2);

    let relief_task = super::value_uplink_release_backpressure(in_rx, item::for_mpsc_sender(out_tx), simple_config());

    let provide_task = async move {

        for i in 0..50 {
            assert!(in_tx.send(Ok(UplinkMessage::Event(ValueLaneEvent(Arc::new(i))))).await.is_ok());
        }
        assert!(in_tx.send(Ok(UplinkMessage::Synced)).await.is_ok());
        for i in 50..100 {
            assert!(in_tx.send(Ok(UplinkMessage::Event(ValueLaneEvent(Arc::new(i))))).await.is_ok());
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
                },
                UplinkMessage::Synced => {
                    assert!(!synced_seen);
                    synced_seen = true;
                    assert_eq!(current, 49);
                }
                _ => panic!("Unexpected output.")
            }
        }
        assert_eq!(current, 99);
        assert!(synced_seen)
    };

    assert!(matches!(join3(relief_task, provide_task, consume_task).await, (Ok(_), _, _)));

}