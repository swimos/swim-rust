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

use crate::meta::metric::uplink::{
    uplink_observer, TaggedWarpUplinkProfile, TrySendError, UplinkProfileSender,
};
use crate::meta::metric::WarpUplinkProfile;
use futures::future::join;
use futures::FutureExt;
use std::sync::atomic::Ordering;
use std::time::Duration;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[tokio::test]
async fn uplink_sender_ok() {
    let path = RelativePath::new("/node", "/lane");
    let (tx, mut rx) = mpsc::channel(1);
    let sender = UplinkProfileSender::new(path.clone(), tx);
    let profile = WarpUplinkProfile::default();

    assert!(sender.try_send(profile.clone()).is_ok());
    let expected = TaggedWarpUplinkProfile { path, profile };

    assert_eq!(rx.recv().now_or_never().unwrap().unwrap(), expected);
}

#[tokio::test]
async fn uplink_sender_err() {
    let path = RelativePath::new("/node", "/lane");
    let (tx, _) = mpsc::channel(1);
    let sender = UplinkProfileSender::new(path.clone(), tx);

    assert_eq!(
        sender.try_send(WarpUplinkProfile::default()),
        Err(TrySendError)
    );
}

#[tokio::test]
async fn test_receive() {
    let path = RelativePath::new("/node", "/lane");
    let (tx, mut rx) = mpsc::channel(1);
    let sender = UplinkProfileSender::new(path.clone(), tx);

    let (event_observer, action_observer) = uplink_observer(Duration::from_nanos(1), sender);

    event_observer
        .inner
        .command_count
        .fetch_add(2, Ordering::Acquire);
    action_observer
        .inner
        .event_count
        .fetch_add(2, Ordering::Acquire);

    event_observer.inner.flush();

    let tagged = rx.recv().await.unwrap();

    assert_eq!(tagged.profile.event_count, 2);
    assert_eq!(tagged.profile.command_count, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_receive_threaded() {
    let path = RelativePath::new("/node", "/lane");
    let (tx, mut rx) = mpsc::channel(2048);
    let sender = UplinkProfileSender::new(path.clone(), tx);

    let sample_rate = Duration::from_secs(1);
    let (event_observer, action_observer) = uplink_observer(sample_rate, sender);

    let left_event_observer = event_observer.clone();
    let left_action_observer = action_observer.clone();

    let right_event_observer = event_observer.clone();
    let right_action_observer = action_observer.clone();

    let task_left = async move {
        for i in 0..100 {
            left_event_observer.on_event();
            left_action_observer.on_command();
            left_action_observer.did_open();
            left_action_observer.did_close();

            if i % 10 == 0 {
                sleep(Duration::from_millis(100)).await;
            }
        }
    };

    let task_right = async move {
        for i in 0..100 {
            right_action_observer.on_command();
            right_event_observer.on_event();
            right_action_observer.did_close();
            right_action_observer.did_open();

            if i % 10 == 0 {
                sleep(Duration::from_millis(100)).await;
            }
        }
    };

    let _r = join(task_left, task_right).await;

    sleep(sample_rate).await;

    event_observer.inner.flush();

    drop(event_observer);
    drop(action_observer);

    let mut profiles = Vec::new();

    while let Some(profile) = rx.recv().await {
        profiles.push(profile);
    }

    let tagged = profiles.pop().expect("Missing profile");
    let WarpUplinkProfile {
        event_delta,
        event_count,
        command_delta,
        command_count,
        open_delta,
        open_count,
        close_delta,
        close_count,
        ..
    } = tagged.profile;

    assert_eq!(event_delta as u64 + event_count, 200);
    assert_eq!(command_delta as u64 + command_count, 200);
    assert_eq!(open_delta + open_count, 200);
    assert_eq!(close_delta + close_count, 200);
}
