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

use super::*;
use crate::configuration::downlink::{ConfigHierarchy, DownlinkParams, OnInvalidMessage};
use futures::join;
use swim_common::routing::remote::RawRoute;
use swim_common::warp::path::AbsolutePath;
use tokio::time::Duration;
use url::Url;

// Configuration overridden for a specific host.
fn per_host_config() -> ConfigHierarchy<AbsolutePath> {
    let timeout = Duration::from_secs(60000);
    let special_params = DownlinkParams::new(
        BackpressureMode::Propagate,
        timeout,
        5,
        OnInvalidMessage::Terminate,
        256,
    )
    .unwrap();

    let mut conf = ConfigHierarchy::default();
    conf.for_host(Url::parse("ws://127.0.0.2").unwrap(), special_params);
    conf
}

// Configuration overridden for a specific lane.
fn per_lane_config() -> ConfigHierarchy<AbsolutePath> {
    let timeout = Duration::from_secs(60000);
    let special_params = DownlinkParams::new(
        BackpressureMode::Propagate,
        timeout,
        5,
        OnInvalidMessage::Terminate,
        256,
    )
    .unwrap();
    let mut conf = per_host_config();
    conf.for_lane(
        &AbsolutePath::new(
            url::Url::parse("ws://127.0.0.2/").unwrap(),
            "my_agent",
            "my_lane",
        ),
        special_params,
    );
    conf
}

async fn dl_manager(conf: ConfigHierarchy<AbsolutePath>) -> Downlinks<AbsolutePath> {
    let (client_conn_request_tx, mut client_conn_request_rx) = mpsc::channel(32);
    //Todo dm
    let (_close_tx, close_rx) = promise::promise();
    let (downlinks, handle) = Downlinks::new(client_conn_request_tx, Arc::new(conf), close_rx);

    tokio::spawn(async move {
        let DownlinksHandle {
            downlinks_task,
            request_receiver,
            task_manager,
        } = handle;

        join!(
            downlinks_task.run(ReceiverStream::new(request_receiver)),
            task_manager.run()
        )
        .0
        .unwrap();
    });

    tokio::spawn(async move {
        while let Some(client_request) = client_conn_request_rx.recv().await {
            match client_request {
                ClientRequest::Connect { request, .. } => {
                    let (outgoing_tx, _outgoing_rx) = mpsc::channel(8);
                    let (_on_drop_tx, on_drop_rx) = promise::promise();
                    request
                        .send(Ok(RawRoute::new(outgoing_tx, on_drop_rx)))
                        .unwrap();
                }
                ClientRequest::Subscribe { request, .. } => {
                    let (outgoing_tx, _outgoing_rx) = mpsc::channel(8);
                    let (_incoming_tx, incoming_rx) = mpsc::channel(8);
                    let (_on_drop_tx, on_drop_rx) = promise::promise();
                    request
                        .send(Ok((RawRoute::new(outgoing_tx, on_drop_rx), incoming_rx)))
                        .unwrap();
                }
            }
        }
    });

    downlinks
}

#[tokio::test]
async fn subscribe_value_lane_default_config() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.1/").unwrap(), "node", "lane");
    let downlinks = dl_manager(Default::default()).await;
    let result = downlinks.subscribe_value_untyped(Value::Extant, path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_value_lane_per_host_config() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.2/").unwrap(), "node", "lane");
    let downlinks = dl_manager(per_host_config()).await;
    let result = downlinks.subscribe_value_untyped(Value::Extant, path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_value_lane_per_lane_config() {
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.2/").unwrap(),
        "my_agent",
        "my_lane",
    );
    let downlinks = dl_manager(per_lane_config()).await;
    let result = downlinks.subscribe_value_untyped(Value::Extant, path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_map_lane_default_config() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.1/").unwrap(), "node", "lane");
    let downlinks = dl_manager(Default::default()).await;
    let result = downlinks.subscribe_map_untyped(path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_map_lane_per_host_config() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.2/").unwrap(), "node", "lane");
    let downlinks = dl_manager(per_host_config()).await;
    let result = downlinks.subscribe_map_untyped(path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_map_lane_per_lane_config() {
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.2/").unwrap(),
        "my_agent",
        "my_lane",
    );
    let downlinks = dl_manager(per_lane_config()).await;
    let result = downlinks.subscribe_map_untyped(path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn request_map_dl_for_running_value_dl() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.1/").unwrap(), "node", "lane");
    let downlinks = dl_manager(Default::default()).await;
    let result = downlinks
        .subscribe_value_untyped(Value::Extant, path.clone())
        .await;
    assert!(result.is_ok());
    let _dl = result.unwrap();

    let next_result = downlinks.subscribe_map_untyped(path).await;
    assert!(next_result.is_err());
    let err = next_result.err().unwrap();
    assert_eq!(
        err,
        SubscriptionError::bad_kind(DownlinkKind::Map, DownlinkKind::Value)
    );
}

#[tokio::test]
async fn request_value_dl_for_running_map_dl() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.1/").unwrap(), "node", "lane");
    let downlinks = dl_manager(Default::default()).await;
    let result = downlinks.subscribe_map_untyped(path.clone()).await;
    assert!(result.is_ok());
    let _dl = result.unwrap();

    let next_result = downlinks.subscribe_value_untyped(Value::Extant, path).await;
    assert!(next_result.is_err());
    let err = next_result.err().unwrap();
    assert_eq!(
        err,
        SubscriptionError::bad_kind(DownlinkKind::Value, DownlinkKind::Map)
    );
}

#[tokio::test]
async fn subscribe_value_twice() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.1/").unwrap(), "node", "lane");
    let downlinks = dl_manager(Default::default()).await;
    let result1 = downlinks
        .subscribe_value_untyped(Value::Extant, path.clone())
        .await;
    assert!(result1.is_ok());
    let (dl1, _rec1) = result1.unwrap();

    let result2 = downlinks.subscribe_value_untyped(Value::Extant, path).await;
    assert!(result2.is_ok());
    let (dl2, _rec2) = result2.unwrap();

    assert!(Arc::ptr_eq(&dl1, &dl2));
}

#[tokio::test]
async fn subscribe_map_twice() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.1/").unwrap(), "node", "lane");
    let downlinks = dl_manager(Default::default()).await;
    let result1 = downlinks.subscribe_map_untyped(path.clone()).await;
    assert!(result1.is_ok());
    let (dl1, _rec1) = result1.unwrap();

    let result2 = downlinks.subscribe_map_untyped(path).await;
    assert!(result2.is_ok());
    let (dl2, _rec2) = result2.unwrap();

    assert!(Arc::ptr_eq(&dl1, &dl2));
}

#[tokio::test]
async fn subscribe_value_lane_typed() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.2/").unwrap(), "node", "lane");
    let downlinks = dl_manager(Default::default()).await;
    let result = downlinks.subscribe_value::<i32>(0, path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_map_lane_typed() {
    let path = AbsolutePath::new(url::Url::parse("ws://127.0.0.2/").unwrap(), "node", "lane");
    let downlinks = dl_manager(Default::default()).await;
    let result = downlinks.subscribe_map::<String, i32>(path).await;
    assert!(result.is_ok());
}
