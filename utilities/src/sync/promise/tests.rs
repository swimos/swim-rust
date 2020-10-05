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

use crate::sync::promise::PromiseError;
use futures::future::join;

#[test]
fn display_promise_error() {
    let string = PromiseError.to_string();

    assert_eq!(string, "Promise was dropped before it was completed.");
}

#[tokio::test]
async fn await_promise() {

    let (tx, rx) = super::promise();

    let send_task = async move {
        assert!(tx.provide(4).is_ok());
    };

    let receive_task = async move {
        let result = rx.await;
        assert!(matches!(result, Ok(v) if *v == 4));
    };

    join(send_task, receive_task).await;

}

#[tokio::test(threaded_scheduler)]
async fn await_promise_threaded() {

    for _ in 0..10000 {
        let (tx, rx) = super::promise();

        let send_task = async move {
            assert!(tx.provide(4).is_ok());
        };

        let receive_task = async move {
            let result = rx.await;
            assert!(matches!(result, Ok(v) if *v == 4));
        };

        let (r1, r2) = join(tokio::spawn(send_task), tokio::spawn(receive_task)).await;
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }
}

#[tokio::test]
async fn promise_sender_dropped() {

    let (tx, rx) = super::promise::<i32>();

    drop(tx);
    let result = rx.await;
    assert!(result.is_err());

}

#[test]
fn promise_receiver_dropped() {

    let (tx, rx) = super::promise();

    drop(rx);
    let result = tx.provide(6);
    assert_eq!(result, Err(6));

}

