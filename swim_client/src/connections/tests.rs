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

use std::time::Duration;
use tokio::sync::mpsc;

use super::*;
use crate::router::tests::{FakeConnections, MockRemoteRouterTask};

use swim_model::path::AbsolutePath;
use swim_runtime::routing::CloseSender;
use swim_utilities::future::retryable::Quantity;
use swim_warp::envelope::Envelope;

async fn create_connection_pool(
    fake_connections: FakeConnections,
) -> (SwimConnPool<AbsolutePath>, CloseSender) {
    let (client_tx, client_rx) = mpsc::channel(32);
    let (close_tx, close_rx) = promise::promise();
    let remote_tx = MockRemoteRouterTask::build(fake_connections);
    let router = Router::client(client_tx.clone(), remote_tx);

    let (connection_pool, pool_task) = SwimConnPool::new(
        DownlinkConnectionsConfig::default(),
        (client_tx, client_rx),
        router,
        close_rx,
    );

    tokio::task::spawn(pool_task.run());
    (connection_pool, close_tx)
}

#[tokio::test]
async fn test_connection_pool_send_single_message_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let envelope = Envelope::command()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("Hello")
        .done();

    let mut fake_conns = FakeConnections::new();
    let (_remote_tx, mut remote_rx) = fake_conns.add_connection(host_url);
    let (mut connection_pool, _close_tx) = create_connection_pool(fake_conns).await;

    let (mut connection_sender, _connection_receiver) = connection_pool
        .request_connection(path, ConnectionType::Outgoing)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender.send_item(envelope.clone()).await.unwrap();

    // Then
    assert_eq!(
        remote_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(0), envelope)
    );
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let first_envelope = Envelope::command()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("First_Text")
        .done();
    let second_envelope = Envelope::command()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("Second_Text")
        .done();

    let mut fake_conns = FakeConnections::new();
    let (_remote_tx, mut remote_rx) = fake_conns.add_connection(host_url);
    let (mut connection_pool, _close_tx) = create_connection_pool(fake_conns).await;

    let (mut connection_sender, _connection_receiver) = connection_pool
        .request_connection(path.clone(), ConnectionType::Outgoing)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender
        .send_item(first_envelope.clone())
        .await
        .unwrap();
    connection_sender
        .send_item(second_envelope.clone())
        .await
        .unwrap();

    // Then
    assert_eq!(
        remote_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(0), first_envelope)
    );
    assert_eq!(
        remote_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(0), second_envelope)
    );
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_multiple_connections() {
    // Given
    let first_host_url = url::Url::parse("ws://127.0.0.1:9001").unwrap();
    let second_host_url = url::Url::parse("ws://127.0.0.2:9001/").unwrap();
    let third_host_url = url::Url::parse("ws://127.0.0.3:9001/").unwrap();
    let first_path = AbsolutePath::new(first_host_url.clone(), "/foo", "/bar");
    let second_path = AbsolutePath::new(second_host_url.clone(), "/foo", "/bar");
    let third_path = AbsolutePath::new(third_host_url.clone(), "/foo", "/bar");

    let first_envelope = Envelope::command()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("First_Text")
        .done();
    let second_envelope = Envelope::command()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("Second_Text")
        .done();
    let third_envelope = Envelope::command()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("Third_Text")
        .done();

    let mut fake_conns = FakeConnections::new();
    let (_first_remote_tx, mut first_remote_rx) = fake_conns.add_connection(first_host_url);
    let (_second_remote_tx, mut second_remote_rx) = fake_conns.add_connection(second_host_url);
    let (_third_remote_tx, mut third_remote_rx) = fake_conns.add_connection(third_host_url);
    let (mut connection_pool, _close_tx) = create_connection_pool(fake_conns).await;

    let (mut first_connection_sender, _first_connection_receiver) = connection_pool
        .request_connection(first_path, ConnectionType::Outgoing)
        .await
        .unwrap()
        .unwrap();

    let (mut second_connection_sender, _second_connection_receiver) = connection_pool
        .request_connection(second_path, ConnectionType::Outgoing)
        .await
        .unwrap()
        .unwrap();

    let (mut third_connection_sender, _third_connection_receiver) = connection_pool
        .request_connection(third_path, ConnectionType::Outgoing)
        .await
        .unwrap()
        .unwrap();

    // When
    first_connection_sender
        .send_item(first_envelope.clone())
        .await
        .unwrap();
    second_connection_sender
        .send_item(second_envelope.clone())
        .await
        .unwrap();
    third_connection_sender
        .send_item(third_envelope.clone())
        .await
        .unwrap();

    // Then
    assert_eq!(
        first_remote_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(0), first_envelope)
    );
    assert_eq!(
        second_remote_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(1), second_envelope)
    );
    assert_eq!(
        third_remote_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(2), third_envelope)
    );
}

#[tokio::test]
async fn test_connection_pool_receive_single_message_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let envelope = Envelope::event()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("Hello")
        .done();

    let mut fake_conns = FakeConnections::new();
    let (remote_tx, _remote_rx) = fake_conns.add_connection(host_url);
    let (mut connection_pool, _close_tx) = create_connection_pool(fake_conns).await;

    // When
    let (_connection_sender, connection_receiver) = connection_pool
        .request_connection(path, ConnectionType::Full)
        .await
        .unwrap()
        .unwrap();

    remote_tx
        .send(TaggedEnvelope(RoutingAddr::remote(0), envelope.clone()))
        .await
        .unwrap();

    // Then
    let pool_message = connection_receiver.unwrap().recv().await.unwrap();
    assert_eq!(
        pool_message,
        RouterEvent::Message(envelope.into_response().unwrap())
    );
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let first_envelope = Envelope::event()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("first_message")
        .done();
    let second_envelope = Envelope::event()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("second_message")
        .done();
    let third_envelope = Envelope::event()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("third_message")
        .done();

    let mut fake_conns = FakeConnections::new();
    let (remote_tx, _remote_rx) = fake_conns.add_connection(host_url);
    let (mut connection_pool, _close_tx) = create_connection_pool(fake_conns).await;

    // When
    let (_connection_sender, connection_receiver) = connection_pool
        .request_connection(path, ConnectionType::Full)
        .await
        .unwrap()
        .unwrap();

    let mut connection_receiver = connection_receiver.unwrap();

    remote_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(0),
            first_envelope.clone(),
        ))
        .await
        .unwrap();
    remote_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(0),
            second_envelope.clone(),
        ))
        .await
        .unwrap();
    remote_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(0),
            third_envelope.clone(),
        ))
        .await
        .unwrap();

    // Then
    let first_pool_message = connection_receiver.recv().await.unwrap();
    let second_pool_message = connection_receiver.recv().await.unwrap();
    let third_pool_message = connection_receiver.recv().await.unwrap();

    assert_eq!(
        first_pool_message,
        RouterEvent::Message(first_envelope.into_response().unwrap())
    );
    assert_eq!(
        second_pool_message,
        RouterEvent::Message(second_envelope.into_response().unwrap())
    );
    assert_eq!(
        third_pool_message,
        RouterEvent::Message(third_envelope.into_response().unwrap())
    );
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_multiple_connections() {
    // Given
    let first_host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let second_host_url = url::Url::parse("ws://127.0.0.2:9001/").unwrap();
    let third_host_url = url::Url::parse("ws://127.0.0.3:9001//").unwrap();
    let first_path = AbsolutePath::new(first_host_url.clone(), "/foo", "/bar");
    let second_path = AbsolutePath::new(second_host_url.clone(), "/foo", "/bar");
    let third_path = AbsolutePath::new(third_host_url.clone(), "/foo", "/bar");

    let first_envelope = Envelope::event()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("first_message")
        .done();
    let second_envelope = Envelope::event()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("second_message")
        .done();
    let third_envelope = Envelope::event()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("third_message")
        .done();

    let mut fake_conns = FakeConnections::new();
    let (first_reader_tx, _) = fake_conns.add_connection(first_host_url);
    let (second_reader_tx, _) = fake_conns.add_connection(second_host_url);
    let (third_reader_tx, _) = fake_conns.add_connection(third_host_url);
    let (mut connection_pool, _close_tx) = create_connection_pool(fake_conns).await;

    // When
    let (_first_sender, mut first_receiver) = connection_pool
        .request_connection(first_path, ConnectionType::Full)
        .await
        .unwrap()
        .unwrap();

    let (_second_sender, mut second_receiver) = connection_pool
        .request_connection(second_path, ConnectionType::Full)
        .await
        .unwrap()
        .unwrap();

    let (_third_sender, mut third_receiver) = connection_pool
        .request_connection(third_path, ConnectionType::Full)
        .await
        .unwrap()
        .unwrap();

    first_reader_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(0),
            first_envelope.clone(),
        ))
        .await
        .unwrap();
    second_reader_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(1),
            second_envelope.clone(),
        ))
        .await
        .unwrap();
    third_reader_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(2),
            third_envelope.clone(),
        ))
        .await
        .unwrap();

    // Then
    let first_pool_message = first_receiver.take().unwrap().recv().await.unwrap();
    let second_pool_message = second_receiver.take().unwrap().recv().await.unwrap();
    let third_pool_message = third_receiver.take().unwrap().recv().await.unwrap();

    assert_eq!(
        first_pool_message,
        RouterEvent::Message(first_envelope.into_response().unwrap())
    );
    assert_eq!(
        second_pool_message,
        RouterEvent::Message(second_envelope.into_response().unwrap())
    );
    assert_eq!(
        third_pool_message,
        RouterEvent::Message(third_envelope.into_response().unwrap())
    );
}

#[tokio::test]
async fn test_connection_pool_send_and_receive_messages() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let incoming_envelope = Envelope::event()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("recv_baz")
        .done();

    let outgoing_envelope = Envelope::command()
        .node_uri("/foo")
        .lane_uri("/bar")
        .body("send_bar")
        .done();

    let mut fake_conns = FakeConnections::new();
    let (remote_tx, mut remote_rx) = fake_conns.add_connection(host_url);
    let (mut connection_pool, _close_tx) = create_connection_pool(fake_conns).await;

    let (mut connection_sender, connection_receiver) = connection_pool
        .request_connection(path, ConnectionType::Full)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender
        .send_item(outgoing_envelope.clone())
        .await
        .unwrap();

    remote_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(0),
            incoming_envelope.clone(),
        ))
        .await
        .unwrap();

    // Then
    let pool_message = connection_receiver.unwrap().recv().await.unwrap();

    assert_eq!(
        pool_message,
        RouterEvent::Message(incoming_envelope.into_response().unwrap())
    );
    assert_eq!(
        remote_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(0), outgoing_envelope)
    );
}

#[tokio::test]
async fn test_connection_pool_connection_error() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let fake_conns = FakeConnections::new();
    let (mut connection_pool, _close_tx) = create_connection_pool(fake_conns).await;

    // When
    let connection = connection_pool
        .request_connection(path, ConnectionType::Full)
        .await
        .unwrap();

    // Then
    assert!(connection.is_err());
}

#[tokio::test]
async fn test_connection_pool_close() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();

    let mut fake_conns = FakeConnections::new();
    let (remote_tx, mut remote_rx) = fake_conns.add_connection(host_url);
    let (_connection_pool, close_tx) = create_connection_pool(fake_conns).await;

    let (response_tx, mut response_rx) = mpsc::channel(8);
    // When
    assert!(close_tx.provide(response_tx).is_ok());

    // Then
    assert!(response_rx.recv().await.is_none());
    assert!(remote_rx.recv().await.is_none());
    assert!(remote_tx.is_closed());
}

#[tokio::test]
async fn test_retry_open_connection_cancel() {
    // Given
    let target = RegistrationTarget::Local(String::from("/foo"));
    let retry_strategy = RetryStrategy::interval(Duration::from_secs(10), Quantity::Infinite);
    let (request_tx, _request_rx) = mpsc::channel(8);

    let router = Router::<RelativePath>::client(request_tx, mpsc::channel(1).0);
    let mut tagged = router.tagged(RoutingAddr::client(1));

    let (close_tx, close_rx) = promise::promise();
    let (response_tx, _response_rx) = mpsc::channel(8);
    // When
    close_tx.provide(response_tx).unwrap();
    let result = open_connection(target, retry_strategy, &mut tagged, close_rx).await;
    // Then
    assert!(result.is_err());
}
