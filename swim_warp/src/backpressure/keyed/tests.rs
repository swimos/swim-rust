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

use crate::backpressure::keyed::{release_pressure, Keyed};
use futures::{FutureExt, StreamExt};
use std::num::NonZeroUsize;
use swim_common::sink::item::for_mpsc_sender;
use swim_runtime::time::timeout::timeout;
use tokio::sync::mpsc;
use tokio::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(30);

fn buffer_size() -> NonZeroUsize {
    NonZeroUsize::new(5).unwrap()
}

fn max_active_keys() -> NonZeroUsize {
    NonZeroUsize::new(5).unwrap()
}

fn yield_after() -> NonZeroUsize {
    NonZeroUsize::new(256).unwrap()
}

#[derive(Clone, Debug, PartialEq)]
struct WarpUplinkProfile {
    key: String,
    event_count: i32,
    command_count: i32,
}

impl Keyed for WarpUplinkProfile {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.key.clone()
    }
}

fn make_event(p: i32) -> WarpUplinkProfile {
    WarpUplinkProfile {
        key: "/lane/1".to_string(),
        event_count: p,
        command_count: p,
    }
}

#[tokio::test]
async fn simple_release() {
    let max = 20i32;

    let (tx_in, rx_in) = mpsc::channel(max as usize * 2);
    let (tx_out, mut rx_out) = mpsc::channel(8);
    let release_task = release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);
    let modifications = (0..=max).into_iter().map(make_event);

    for m in modifications.into_iter() {
        let result = tx_in.send(m).await;
        assert!(result.is_ok());
    }

    let receiver = tokio::task::spawn(async move {
        let min = max - buffer_size().get() as i32;

        for i in min..max {
            assert_eq!(rx_out.next().await, Some(make_event(i)));
        }

        assert_eq!(rx_out.next().now_or_never(), None);
    });

    let _output = timeout(TIMEOUT, receiver).await.unwrap();

    drop(tx_in);

    assert!(matches!(release_result.await, Ok(Ok(_))));
}
