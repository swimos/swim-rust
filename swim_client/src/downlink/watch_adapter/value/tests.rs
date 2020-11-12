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

use super::*;
use std::time::Duration;
use swim_common::sink::item;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(30);

fn yield_after() -> NonZeroUsize {
    NonZeroUsize::new(256).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn single_pass_through() {
    let (tx, mut rx) = mpsc::channel::<i32>(5);

    let mut pump = ValuePump::new(item::for_mpsc_sender(tx).map_err_into(), yield_after()).await;

    let receiver = tokio::task::spawn(async move { rx.recv().await.unwrap() });

    let result = pump.send_item(6).await;

    assert!(result.is_ok());

    let value = timeout(TIMEOUT, receiver).await.unwrap().unwrap();
    assert_eq!(value, 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn send_multiple() {
    let (tx, mut rx) = mpsc::channel::<i32>(5);

    let mut pump = ValuePump::new(item::for_mpsc_sender(tx).map_err_into(), yield_after()).await;

    let receiver = tokio::task::spawn(async move {
        let mut observed: i32 = 0;
        let mut prev: Option<i32> = None;

        let mut in_order = true;

        while let Some(i) = rx.recv().await {
            if let Some(p) = prev {
                if p >= i {
                    in_order = false;
                    break;
                }
            }
            prev = Some(i);
            observed += 1;
            if i == 9 {
                break;
            }
        }
        (in_order, observed, prev)
    });

    for n in 0..10 {
        let result = pump.send_item(n).await;
        assert!(result.is_ok());
    }

    let (in_order, observed, prev) = timeout(TIMEOUT, receiver).await.unwrap().unwrap();

    assert!(in_order);
    assert!(observed <= 10);
    assert_eq!(prev, Some(9));
}

#[tokio::test(flavor = "multi_thread")]
async fn send_multiple_chunks() {
    let (tx, mut rx) = mpsc::channel::<i32>(5);

    let mut pump = ValuePump::new(item::for_mpsc_sender(tx).map_err_into(), yield_after()).await;

    let receiver1 = tokio::task::spawn(async move {
        let mut observed: i32 = 0;
        let mut prev: Option<i32> = None;

        let mut in_order = true;

        while let Some(i) = rx.recv().await {
            if let Some(p) = prev {
                if p >= i {
                    in_order = false;
                    break;
                }
            }
            prev = Some(i);
            observed += 1;
            if i == 4 {
                break;
            }
        }
        (rx, in_order, observed, prev)
    });

    for n in 0..5 {
        let result = pump.send_item(n).await;
        assert!(result.is_ok());
    }

    let (mut rx, in_order1, observed1, prev) = timeout(TIMEOUT, receiver1).await.unwrap().unwrap();

    assert!(in_order1);
    assert_eq!(prev, Some(4));

    let receiver2 = tokio::task::spawn(async move {
        let mut observed: i32 = 0;
        let mut prev: Option<i32> = None;

        let mut in_order = true;

        while let Some(i) = rx.recv().await {
            if let Some(p) = prev {
                if p >= i {
                    in_order = false;
                    break;
                }
            }
            prev = Some(i);
            observed += 1;
            if i == 9 {
                break;
            }
        }
        (in_order, observed, prev)
    });

    for n in 5..10 {
        let result = pump.send_item(n).await;
        assert!(result.is_ok());
    }

    let (in_order2, observed2, prev) = timeout(TIMEOUT, receiver2).await.unwrap().unwrap();

    assert!(in_order2);
    assert!(observed1 + observed2 <= 10);
    assert_eq!(prev, Some(9));
}
