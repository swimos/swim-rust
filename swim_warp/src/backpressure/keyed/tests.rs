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

use crate::backpressure::keyed::{release_pressure, Keyed};
use futures::FutureExt;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::ops::Range;
use swim_utilities::future::item_sink::for_mpsc_sender;
use swim_runtime::time::timeout::timeout;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

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

fn make_event(lane: String, p: i32) -> WarpUplinkProfile {
    WarpUplinkProfile {
        key: lane,
        event_count: p,
        command_count: p,
    }
}

#[tokio::test]
async fn simple_release() {
    let max = 20i32;
    let lane = "/lane".to_string();

    let (tx_in, rx_in) = mpsc::channel(max as usize * 2);
    let (tx_out, mut rx_out) = mpsc::channel(8);
    let release_task = release_pressure(
        ReceiverStream::new(rx_in),
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);
    let profiles = (0..=max).into_iter().map(|p| make_event(lane.clone(), p));

    for m in profiles.into_iter() {
        let result = tx_in.send(m).await;
        assert!(result.is_ok());
    }

    let receiver = tokio::task::spawn(async move {
        let min = max - buffer_size().get() as i32 + 1;

        for i in min..=max {
            assert_eq!(rx_out.recv().await, Some(make_event(lane.clone(), i)));
        }

        assert_eq!(rx_out.recv().now_or_never(), None);
    });

    let _output = timeout(TIMEOUT, receiver).await.unwrap();

    drop(tx_in);

    assert!(matches!(release_result.await, Ok(Ok(_))));
}

fn data(range: Range<i32>) -> Vec<WarpUplinkProfile> {
    range
        .clone()
        .into_iter()
        .map(|count| {
            range.clone().into_iter().map(move |lane_id| {
                let lane = format_lane(lane_id);
                make_event(lane, count)
            })
        })
        .flatten()
        .collect::<Vec<WarpUplinkProfile>>()
}

fn format_lane<D: Display>(id: D) -> String {
    format!("/lane/{}", id)
}

// Test using multiple keys, fill up every buffer and assert that every value is received.
#[tokio::test(flavor = "multi_thread")]
async fn multiple_keys() {
    let (tx_in, rx_in) = mpsc::channel(8);
    let max = 20i32;
    let range = 0..max;

    let mut expected: HashMap<String, Vec<WarpUplinkProfile>> = HashMap::new();

    let (tx_out, mut rx_out) = mpsc::channel(8);
    let release_task = release_pressure(
        ReceiverStream::new(rx_in),
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        NonZeroUsize::new(20).unwrap(),
        NonZeroUsize::new(20).unwrap(),
    );

    let release_result = tokio::task::spawn(release_task);
    let profiles = data(range);
    profiles.iter().for_each(|profile| {
        expected
            .entry(profile.key())
            .or_default()
            .push(profile.clone());
    });

    let mut seen: HashMap<String, Vec<WarpUplinkProfile>> = HashMap::new();

    let receiver = tokio::task::spawn(async move {
        while let Some(profile) = rx_out.recv().await {
            let profile: WarpUplinkProfile = profile;
            seen.entry(profile.key()).or_default().push(profile.clone());
        }

        assert_eq!(expected, seen);
    });

    for m in profiles.into_iter() {
        let result = tx_in.send(m).await;
        assert!(result.is_ok());
    }

    drop(tx_in);

    timeout(TIMEOUT, receiver).await.unwrap().unwrap();

    assert!(matches!(release_result.await, Ok(Ok(_))));
}

#[tokio::test]
async fn overflow() {
    let max = 20i32;
    let range = 0..max;
    let count = max * max;

    let (tx_in, rx_in) = mpsc::channel(count as usize);
    let (tx_out, mut rx_out) = mpsc::channel(8);
    let release_task = release_pressure(
        ReceiverStream::new(rx_in),
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        NonZeroUsize::new(20).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    );

    let profiles = data(range);

    let release_result = tokio::task::spawn(release_task);

    // fill up the buffers for each key
    for m in profiles.into_iter() {
        let result = tx_in.send(m).await;
        assert!(result.is_ok());
    }

    drop(tx_in);

    // assert that we only receive the maximum value for
    // each lane id - which will be the most recent value
    let receiver = tokio::task::spawn(async move {
        let mut seen = BTreeMap::new();

        while let Some(value) = rx_out.recv().await {
            seen.insert(value.key(), value);
        }

        (0..max).into_iter().for_each(|i| {
            let lane = format_lane(i);
            assert!(seen.contains_key(&lane));
        });

        seen.into_iter()
            .for_each(|(_k, v): (String, WarpUplinkProfile)| {
                assert_eq!(v.event_count, max - 1);
                assert_eq!(v.command_count, max - 1);
            })
    });

    timeout(TIMEOUT, receiver).await.unwrap().unwrap();

    assert!(matches!(release_result.await, Ok(Ok(_))));
}
