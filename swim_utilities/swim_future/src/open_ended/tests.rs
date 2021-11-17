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

use crate::open_ended::OpenEndedFutures;
use futures::future::ready;
use futures::stream::iter;
use futures::{select_biased, StreamExt};

#[tokio::test]
async fn exhaust_and_replace() {
    let mut other = iter(1..5).fuse();
    let mut open_ended = OpenEndedFutures::new();

    let mut received = vec![];

    assert!(open_ended.try_push(ready(10)).is_ok());
    loop {
        let maybe_value = select_biased! {
            n = open_ended.next() => n,
            n = other.next() => n,
        };
        if let Some(n) = maybe_value {
            match n {
                1 | 2 => {
                    assert!(open_ended.try_push(ready(10 + n)).is_ok());
                }
                3 => {
                    assert!(open_ended.try_push(ready(13)).is_ok());
                    open_ended.stop();
                }
                _ => {}
            }
            received.push(n);
        } else {
            break;
        }
    }

    assert!(open_ended.try_push(ready(14)).is_err());

    assert_eq!(received, vec![10, 1, 11, 2, 12, 3, 13]);
}
