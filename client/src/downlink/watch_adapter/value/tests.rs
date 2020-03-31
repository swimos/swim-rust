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

use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use tokio::sync::mpsc;

use super::*;

#[tokio::test]
async fn single_pass_through() {
    let (tx, mut rx) = mpsc::channel::<i32>(5);

    let mut pump = ValuePump::new(tx.map_err_into()).await;

    let result = pump.send_item(6).await;
    assert_that!(result, ok());

    let value = rx.recv().await.unwrap();

    assert_that!(value, eq(6));
}

#[tokio::test(threaded_scheduler)]
async fn send_multiple() {
    let (tx, mut rx) = mpsc::channel::<i32>(5);

    let mut pump = ValuePump::new(tx.map_err_into()).await;

    for n in 0..10 {
        let result = pump.send_item(n).await;
        assert_that!(result, ok());
    }

    let mut prev: Option<i32> = None;

    while let Some(i) = rx.recv().await {
        if let Some(p) = prev {
            assert_that!(i, greater_than(p));
        }
        prev = Some(i);
        observed += 1;
        if i == 9 {
            break;
        }
    }
    println!("{}", observed);
    assert_that!(observed, leq(10));
    assert_that!(prev, eq(Some(9)));
}

#[tokio::test(threaded_scheduler)]
async fn send_multiple_chunks() {
    let (tx, mut rx) = mpsc::channel::<i32>(5);

    let mut pump = ValuePump::new(tx.map_err_into()).await;

    for n in 0..5 {
        let result = pump.send_item(n).await;
        assert_that!(result, ok());
    }

    let mut observed: i32 = 0;
    let mut prev: Option<i32> = None;

    while let Some(i) = rx.recv().await {
        if let Some(p) = prev {
            assert_that!(i, greater_than(p));
        }
        prev = Some(i);
        observed += 1;
        if i == 4 {
            break;
        }
    }
    assert_that!(prev, eq(Some(4)));

    for n in 5..10 {
        let result = pump.send_item(n).await;
        assert_that!(result, ok());
    }

    while let Some(i) = rx.recv().await {
        if let Some(p) = prev {
            assert_that!(i, greater_than(p));
        }
        prev = Some(i);
        observed += 1;
        if i == 9 {
            break;
        }
    }
    assert_that!(observed, leq(10));
    assert_that!(prev, eq(Some(9)));
}
