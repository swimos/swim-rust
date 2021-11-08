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
use crate::configuration::{ClientDownlinksConfig, DownlinkConnectionsConfig};
use crate::configuration::{DownlinkConfig, OnInvalidMessage};
use crate::router::tests::{FakeConnections, MockRemoteRouterTask};
use crate::router::{ClientRouterFactory, TopLevelClientRouterFactory};
use futures::join;
use swim_common::routing::CloseSender;
use swim_common::warp::path::AbsolutePath;
use swim_utilities::algebra::non_zero_usize;
use tokio::time::Duration;
use url::Url;

// Configuration overridden for a specific host.
fn per_host_config() -> ClientDownlinksConfig {
    let timeout = Duration::from_secs(60000);
    let special_params = DownlinkConfig::new(
        BackpressureMode::Propagate,
        timeout,
        non_zero_usize!(5),
        OnInvalidMessage::Terminate,
        non_zero_usize!(256),
    );

    let mut conf = ClientDownlinksConfig::default();
    conf.for_host(Url::parse("ws://127.0.0.2").unwrap(), special_params);
    conf
}

// Configuration overridden for a specific lane.
fn per_lane_config() -> ClientDownlinksConfig {
    let timeout = Duration::from_secs(60000);
    let special_params = DownlinkConfig::new(
        BackpressureMode::Propagate,
        timeout,
        non_zero_usize!(5),
        OnInvalidMessage::Terminate,
        non_zero_usize!(256),
    );
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

async fn dl_manager(
    conf: ClientDownlinksConfig,
    conns: FakeConnections,
) -> (Downlinks<AbsolutePath>, CloseSender) {
    let (client_tx, client_rx) = mpsc::channel(32);
    let (conn_request_tx, _conn_request_rx) = mpsc::channel(32);
    let (close_tx, close_rx) = promise::promise();

    let remote_tx = MockRemoteRouterTask::new(conns);

    let delegate_fac = TopLevelClientRouterFactory::new(client_tx.clone(), remote_tx.clone());
    let client_router_fac = ClientRouterFactory::new(conn_request_tx, delegate_fac);

    let (connection_pool, pool_task) = SwimConnPool::new(
        DownlinkConnectionsConfig::default(),
        (client_tx, client_rx),
        client_router_fac,
        close_rx.clone(),
    );

    let (downlinks, downlinks_task) = Downlinks::new(
        non_zero_usize!(8),
        connection_pool,
        Arc::new(conf),
        close_rx,
    );

    tokio::spawn(async move {
        join!(downlinks_task.run(), pool_task.run()).0.unwrap();
    });

    (downlinks, close_tx)
}

#[tokio::test]
async fn subscribe_value_lane_default_config() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(Default::default(), conns).await;
    let result = downlinks.subscribe_value_untyped(Value::Extant, path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_value_lane_per_host_config() {
    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(Default::default(), conns).await;
    let result = downlinks.subscribe_value_untyped(Value::Extant, path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_value_lane_per_lane_config() {
    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let path = AbsolutePath::new(url.clone(), "my_agent", "my_lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(per_lane_config(), conns).await;
    let result = downlinks.subscribe_value_untyped(Value::Extant, path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_map_lane_default_config() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(Default::default(), conns).await;
    let result = downlinks.subscribe_map_untyped(path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_map_lane_per_host_config() {
    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(per_host_config(), conns).await;
    let result = downlinks.subscribe_map_untyped(path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_map_lane_per_lane_config() {
    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let path = AbsolutePath::new(url.clone(), "my_agent", "my_lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(per_lane_config(), conns).await;
    let result = downlinks.subscribe_map_untyped(path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn request_map_dl_for_running_value_dl() {
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(Default::default(), conns).await;
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
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(Default::default(), conns).await;
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
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(Default::default(), conns).await;
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
    let url = url::Url::parse("ws://127.0.0.1/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(Default::default(), conns).await;
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
    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(Default::default(), conns).await;
    let result = downlinks.subscribe_value::<i32>(0, path).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn subscribe_map_lane_typed() {
    let url = url::Url::parse("ws://127.0.0.2/").unwrap();
    let path = AbsolutePath::new(url.clone(), "node", "lane");
    let mut conns = FakeConnections::new();
    let _conn = conns.add_connection(url);
    let (downlinks, _close_tx) = dl_manager(Default::default(), conns).await;
    let result = downlinks.subscribe_map::<String, i32>(path).await;
    assert!(result.is_ok());
}
