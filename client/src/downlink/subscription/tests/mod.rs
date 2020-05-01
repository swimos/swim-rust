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

use super::*;
use crate::configuration::downlink::{
    ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};
use crate::downlink::any::TopicKind;
use common::warp::path::AbsolutePath;
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use tokio::time::Duration;

mod harness;

// Uses the same configuration for everything.
fn default_config() -> ConfigHierarchy {
    let client_params = ClientParams::new(2, Default::default()).unwrap();
    let timeout = Duration::from_secs(60000);
    let default_params = DownlinkParams::new_queue(
        BackpressureMode::Propagate,
        5,
        timeout,
        5,
        OnInvalidMessage::Terminate,
    )
    .unwrap();
    ConfigHierarchy::new(client_params, default_params)
}

// Configuration overridden for a specific host.
fn per_host_config() -> ConfigHierarchy {
    let timeout = Duration::from_secs(60000);
    let special_params = DownlinkParams::new_dropping(
        BackpressureMode::Propagate,
        timeout,
        5,
        OnInvalidMessage::Terminate,
    )
    .unwrap();
    let mut conf = default_config();
    conf.for_host("ws://127.0.0.2/", special_params);
    conf
}

// Configuration overridden for a specific lane.
fn per_lane_config() -> ConfigHierarchy {
    let timeout = Duration::from_secs(60000);
    let special_params = DownlinkParams::new_buffered(
        BackpressureMode::Propagate,
        5,
        timeout,
        5,
        OnInvalidMessage::Terminate,
    )
    .unwrap();
    let mut conf = per_host_config();
    conf.for_lane(
        &AbsolutePath::new("ws://127.0.0.2/", "my_agent", "my_lane").unwrap(),
        special_params,
    );
    conf
}

async fn dl_manager(conf: ConfigHierarchy) -> Downlinks {
    let router = harness::StubRouter::new();
    Downlinks::new(Arc::new(conf), router).await
}

#[tokio::test]
async fn subscribe_value_lane_default_config() {
    let path = AbsolutePath::new("ws://127.0.0.1/", "node", "lane").unwrap();
    let mut downlinks = dl_manager(default_config()).await;
    let result = downlinks.subscribe_value(Value::Extant, path).await;
    assert_that!(&result, ok());
    let (dl, _rec) = result.unwrap();

    assert_that!(dl.kind(), eq(TopicKind::Queue));
}

#[tokio::test]
async fn subscribe_value_lane_per_host_config() {
    let path = AbsolutePath::new("ws://127.0.0.2/", "node", "lane").unwrap();
    let mut downlinks = dl_manager(per_host_config()).await;
    let result = downlinks.subscribe_value(Value::Extant, path).await;
    assert_that!(&result, ok());
    let (dl, _rec) = result.unwrap();

    assert_that!(dl.kind(), eq(TopicKind::Dropping));
}

#[tokio::test]
async fn subscribe_value_lane_per_lane_config() {
    let path = AbsolutePath::new("ws://127.0.0.2/", "my_agent", "my_lane").unwrap();
    let mut downlinks = dl_manager(per_lane_config()).await;
    let result = downlinks.subscribe_value(Value::Extant, path).await;
    assert_that!(&result, ok());
    let (dl, _rec) = result.unwrap();

    assert_that!(dl.kind(), eq(TopicKind::Buffered));
}

#[tokio::test]
async fn subscribe_map_lane_default_config() {
    let path = AbsolutePath::new("ws://127.0.0.1/", "node", "lane").unwrap();
    let mut downlinks = dl_manager(default_config()).await;
    let result = downlinks.subscribe_map(path).await;
    assert_that!(&result, ok());
    let (dl, _rec) = result.unwrap();

    assert_that!(dl.kind(), eq(TopicKind::Queue));
}

#[tokio::test]
async fn subscribe_map_lane_per_host_config() {
    let path = AbsolutePath::new("ws://127.0.0.2/", "node", "lane").unwrap();
    let mut downlinks = dl_manager(per_host_config()).await;
    let result = downlinks.subscribe_map(path).await;
    assert_that!(&result, ok());
    let (dl, _rec) = result.unwrap();

    assert_that!(dl.kind(), eq(TopicKind::Dropping));
}

#[tokio::test]
async fn subscribe_map_lane_per_lane_config() {
    let path = AbsolutePath::new("ws://127.0.0.2/", "my_agent", "my_lane").unwrap();
    let mut downlinks = dl_manager(per_lane_config()).await;
    let result = downlinks.subscribe_map(path).await;
    assert_that!(&result, ok());
    let (dl, _rec) = result.unwrap();

    assert_that!(dl.kind(), eq(TopicKind::Buffered));
}

#[tokio::test]
async fn request_map_dl_for_running_value_dl() {
    let path = AbsolutePath::new("ws://127.0.0.1/", "node", "lane").unwrap();
    let mut downlinks = dl_manager(default_config()).await;
    let result = downlinks.subscribe_value(Value::Extant, path.clone()).await;
    assert_that!(&result, ok());
    let _dl = result.unwrap();

    let next_result = downlinks.subscribe_map(path).await;
    assert_that!(&next_result, err());
    let err = next_result.err().unwrap();
    assert_that!(
        err,
        eq(SubscriptionError::bad_kind(
            DownlinkKind::Map,
            DownlinkKind::Value
        ))
    );
}

#[tokio::test]
async fn request_value_dl_for_running_map_dl() {
    let path = AbsolutePath::new("ws://127.0.0.1/", "node", "lane").unwrap();
    let mut downlinks = dl_manager(default_config()).await;
    let result = downlinks.subscribe_map(path.clone()).await;
    assert_that!(&result, ok());
    let _dl = result.unwrap();

    let next_result = downlinks.subscribe_value(Value::Extant, path).await;
    assert_that!(&next_result, err());
    let err = next_result.err().unwrap();
    assert_that!(
        err,
        eq(SubscriptionError::bad_kind(
            DownlinkKind::Value,
            DownlinkKind::Map
        ))
    );
}

#[tokio::test]
async fn subscribe_value_twice() {
    let path = AbsolutePath::new("ws://127.0.0.1/", "node", "lane").unwrap();
    let mut downlinks = dl_manager(default_config()).await;
    let result1 = downlinks.subscribe_value(Value::Extant, path.clone()).await;
    assert_that!(&result1, ok());
    let (dl1, _rec1) = result1.unwrap();

    let result2 = downlinks.subscribe_value(Value::Extant, path).await;
    assert_that!(&result2, ok());
    let (dl2, _rec2) = result2.unwrap();

    assert!(dl1.same_downlink(&dl2));
}

#[tokio::test]
async fn subscribe_map_twice() {
    let path = AbsolutePath::new("ws://127.0.0.1/", "node", "lane").unwrap();
    let mut downlinks = dl_manager(default_config()).await;
    let result1 = downlinks.subscribe_map(path.clone()).await;
    assert_that!(&result1, ok());
    let (dl1, _rec1) = result1.unwrap();

    let result2 = downlinks.subscribe_map(path).await;
    assert_that!(&result2, ok());
    let (dl2, _rec2) = result2.unwrap();

    assert!(dl1.same_downlink(&dl2));
}
