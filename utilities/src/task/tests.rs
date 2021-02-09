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

use crate::future::open_ended::OpenEndedFutures;
use crate::task::{Spawner, TokioSpawner};
use futures::future::ready;
use futures::StreamExt;
use std::collections::HashSet;

#[tokio::test]
async fn open_ended_futures_spawner() {
    let mut spawner = OpenEndedFutures::new();

    let mut expected: HashSet<i32> = HashSet::new();

    assert!(Spawner::is_empty(&spawner));

    for i in 0..5 {
        assert!(spawner.try_add(ready(i)).is_ok());
        expected.insert(i);
    }

    assert!(!Spawner::is_empty(&spawner));

    spawner.stop();

    let results: HashSet<i32> = spawner.collect().await;

    assert_eq!(results, expected);
}

#[tokio::test]
async fn tokio_task_spawner() {
    let mut spawner = TokioSpawner::new();

    let mut expected: HashSet<i32> = HashSet::new();

    assert!(Spawner::is_empty(&spawner));

    for i in 0..5 {
        assert!(spawner.try_add(ready(i)).is_ok());
        expected.insert(i);
    }

    assert!(!Spawner::is_empty(&spawner));

    spawner.stop();

    let results: HashSet<i32> = spawner.collect().await;

    assert_eq!(results, expected);
}

#[tokio::test]
#[should_panic]
async fn tokio_task_spawner_panic() {
    let mut spawner = TokioSpawner::new();

    assert!(spawner
        .try_add(async {
            panic!("Boom!");
        })
        .is_ok());

    spawner.next().await;
}
