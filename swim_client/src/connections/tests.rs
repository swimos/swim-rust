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

use tokio::sync::mpsc;

use super::*;
use swim_common::routing::remote::RawRoute;
use swim_common::warp::path::AbsolutePath;
use url::Url;
use utilities::sync::promise::promise;

struct FakeConnections {
    outgoing_channels: HashMap<Url, mpsc::Sender<TaggedEnvelope>>,
    incoming_channels: HashMap<Url, mpsc::Receiver<Envelope>>,
}

impl FakeConnections {
    fn new() -> Self {
        FakeConnections {
            outgoing_channels: HashMap::new(),
            incoming_channels: HashMap::new(),
        }
    }

    fn add_connection(
        &mut self,
        url: Url,
    ) -> (mpsc::Sender<Envelope>, mpsc::Receiver<TaggedEnvelope>) {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(8);
        let (incoming_tx, incoming_rx) = mpsc::channel(8);

        let _ = self.outgoing_channels.insert(url.clone(), outgoing_tx);
        let _ = self.incoming_channels.insert(url, incoming_rx);

        (incoming_tx, outgoing_rx)
    }
}

async fn create_mock_conn_request_loop(
    mut fake_conns: FakeConnections,
) -> mpsc::Sender<DownlinkRoutingRequest<AbsolutePath>> {
    let (client_conn_request_tx, mut client_conn_request_rx) =
        mpsc::channel::<DownlinkRoutingRequest<AbsolutePath>>(8);

    tokio::spawn(async move {
        while let Some(client_request) = client_conn_request_rx.recv().await {
            match client_request {
                DownlinkRoutingRequest::Connect { .. } => {
                    unimplemented!();
                }
                DownlinkRoutingRequest::Subscribe { request, target } => {
                    let maybe_outgoing_tx = fake_conns.outgoing_channels.get(&target.host);
                    let maybe_incoming_rx = fake_conns.incoming_channels.remove(&target.host);

                    match (maybe_outgoing_tx, maybe_incoming_rx) {
                        (Some(outgoing_tx), Some(incoming_rx)) => {
                            let (_on_drop_tx, on_drop_rx) = promise();

                            request
                                .send(Ok((
                                    RawRoute::new(outgoing_tx.clone(), on_drop_rx),
                                    incoming_rx,
                                )))
                                .unwrap();
                        }
                        _ => request
                            .send(Err(ConnectionError::Closed(CloseError::closed())))
                            .unwrap(),
                    }
                }
            }
        }
    });
    client_conn_request_tx
}

async fn create_connection_pool(fake_conns: FakeConnections) -> SwimConnPool<AbsolutePath> {
    let client_conn_request_tx = create_mock_conn_request_loop(fake_conns).await;
    let (pool, task) = SwimConnPool::new(ConnectionPoolParams::default(), client_conn_request_tx);
    tokio::task::spawn(task.run());
    pool
}

#[tokio::test]
async fn test_connection_pool_send_single_message_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let envelope = Envelope::make_command("/foo", "/bar", Some("Hello".into()));

    let mut fake_conns = FakeConnections::new();
    let (_, mut writer_rx) = fake_conns.add_connection(host_url);
    let mut connection_pool = create_connection_pool(fake_conns).await;

    let (mut connection_sender, _connection_receiver) = connection_pool
        .request_connection(path, false)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender
        .send_message(envelope.clone())
        .await
        .unwrap();

    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), envelope)
    );
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let first_envelope = Envelope::make_command("/foo", "/bar", Some("First_Text".into()));
    let second_envelope = Envelope::make_command("/foo", "/bar", Some("Second_Text".into()));

    let mut fake_conns = FakeConnections::new();
    let (_, mut writer_rx) = fake_conns.add_connection(host_url);
    let mut connection_pool = create_connection_pool(fake_conns).await;

    let (mut connection_sender, _connection_receiver) = connection_pool
        .request_connection(path.clone(), false)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender
        .send_message(first_envelope.clone())
        .await
        .unwrap();
    connection_sender
        .send_message(second_envelope.clone())
        .await
        .unwrap();

    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), first_envelope)
    );
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), second_envelope)
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

    let first_envelope = Envelope::make_command("/foo", "/bar", Some("First_Text".into()));
    let second_envelope = Envelope::make_command("/foo", "/bar", Some("Second_Text".into()));
    let third_envelope = Envelope::make_command("/foo", "/bar", Some("Third_Text".into()));

    let mut fake_conns = FakeConnections::new();
    let (_, mut first_writer_rx) = fake_conns.add_connection(first_host_url);
    let (_, mut second_writer_rx) = fake_conns.add_connection(second_host_url);
    let (_, mut third_writer_rx) = fake_conns.add_connection(third_host_url);
    let mut connection_pool = create_connection_pool(fake_conns).await;

    let (mut first_connection_sender, _first_connection_receiver) = connection_pool
        .request_connection(first_path, false)
        .await
        .unwrap()
        .unwrap();

    let (mut second_connection_sender, _second_connection_receiver) = connection_pool
        .request_connection(second_path, false)
        .await
        .unwrap()
        .unwrap();

    let (mut third_connection_sender, _third_connection_receiver) = connection_pool
        .request_connection(third_path, false)
        .await
        .unwrap()
        .unwrap();

    // When
    first_connection_sender
        .send_message(first_envelope.clone())
        .await
        .unwrap();
    second_connection_sender
        .send_message(second_envelope.clone())
        .await
        .unwrap();
    third_connection_sender
        .send_message(third_envelope.clone())
        .await
        .unwrap();

    // Then
    assert_eq!(
        first_writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), first_envelope)
    );
    assert_eq!(
        second_writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), second_envelope)
    );
    assert_eq!(
        third_writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), third_envelope)
    );
}

#[tokio::test]
async fn test_connection_pool_receive_single_message_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let envelope = Envelope::make_command("/foo", "/bar", Some("Hello".into()));

    let mut fake_conns = FakeConnections::new();
    let (reader_tx, _) = fake_conns.add_connection(host_url);
    let mut connection_pool = create_connection_pool(fake_conns).await;

    // When
    let (_connection_sender, connection_receiver) = connection_pool
        .request_connection(path, false)
        .await
        .unwrap()
        .unwrap();

    reader_tx.send(envelope.clone()).await.unwrap();

    // Then
    let pool_message = connection_receiver.unwrap().recv().await.unwrap();
    assert_eq!(pool_message, envelope);
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let first_envelope = Envelope::make_command("/foo", "/bar", Some("first_message".into()));
    let second_envelope = Envelope::make_command("/foo", "/bar", Some("second_message".into()));
    let third_envelope = Envelope::make_command("/foo", "/bar", Some("third_message".into()));

    let mut fake_conns = FakeConnections::new();
    let (reader_tx, _) = fake_conns.add_connection(host_url);
    let mut connection_pool = create_connection_pool(fake_conns).await;

    // When
    let (_connection_sender, connection_receiver) = connection_pool
        .request_connection(path, false)
        .await
        .unwrap()
        .unwrap();

    let mut connection_receiver = connection_receiver.unwrap();

    reader_tx.send(first_envelope.clone()).await.unwrap();
    reader_tx.send(second_envelope.clone()).await.unwrap();
    reader_tx.send(third_envelope.clone()).await.unwrap();

    // Then
    let first_pool_message = connection_receiver.recv().await.unwrap();
    let second_pool_message = connection_receiver.recv().await.unwrap();
    let third_pool_message = connection_receiver.recv().await.unwrap();

    assert_eq!(first_pool_message, first_envelope);
    assert_eq!(second_pool_message, second_envelope);
    assert_eq!(third_pool_message, third_envelope);
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

    let first_envelope = Envelope::make_command("/foo", "/bar", Some("first_message".into()));
    let second_envelope = Envelope::make_command("/foo", "/bar", Some("second_message".into()));
    let third_envelope = Envelope::make_command("/foo", "/bar", Some("third_message".into()));

    let mut fake_conns = FakeConnections::new();
    let (first_reader_tx, _) = fake_conns.add_connection(first_host_url);
    let (second_reader_tx, _) = fake_conns.add_connection(second_host_url);
    let (third_reader_tx, _) = fake_conns.add_connection(third_host_url);
    let mut connection_pool = create_connection_pool(fake_conns).await;

    // When
    let (_first_sender, mut first_receiver) = connection_pool
        .request_connection(first_path, false)
        .await
        .unwrap()
        .unwrap();

    let (_second_sender, mut second_receiver) = connection_pool
        .request_connection(second_path, false)
        .await
        .unwrap()
        .unwrap();

    let (_third_sender, mut third_receiver) = connection_pool
        .request_connection(third_path, false)
        .await
        .unwrap()
        .unwrap();

    first_reader_tx.send(first_envelope.clone()).await.unwrap();
    second_reader_tx
        .send(second_envelope.clone())
        .await
        .unwrap();
    third_reader_tx.send(third_envelope.clone()).await.unwrap();

    // Then
    let first_pool_message = first_receiver.take().unwrap().recv().await.unwrap();
    let second_pool_message = second_receiver.take().unwrap().recv().await.unwrap();
    let third_pool_message = third_receiver.take().unwrap().recv().await.unwrap();

    assert_eq!(first_pool_message, first_envelope);
    assert_eq!(second_pool_message, second_envelope);
    assert_eq!(third_pool_message, third_envelope);
}

#[tokio::test]
async fn test_connection_pool_send_and_receive_messages() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let incoming_envelope = Envelope::make_command("/foo", "/bar", Some("recv_baz".into()));
    let outgoing_envelope = Envelope::make_command("/foo", "/bar", Some("send_bar".into()));

    let mut fake_conns = FakeConnections::new();
    let (reader_tx, mut writer_rx) = fake_conns.add_connection(host_url);
    let mut connection_pool = create_connection_pool(fake_conns).await;

    let (mut connection_sender, connection_receiver) = connection_pool
        .request_connection(path, false)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender
        .send_message(outgoing_envelope.clone())
        .await
        .unwrap();

    reader_tx.send(incoming_envelope.clone()).await.unwrap();

    // Then
    let pool_message = connection_receiver.unwrap().recv().await.unwrap();

    assert_eq!(pool_message, incoming_envelope);
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), outgoing_envelope)
    );
}

#[tokio::test]
async fn test_connection_pool_connection_error() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");

    let fake_conns = FakeConnections::new();
    let mut connection_pool = create_connection_pool(fake_conns).await;

    // When
    let connection = connection_pool
        .request_connection(path, false)
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
    let (_, mut writer_rx) = fake_conns.add_connection(host_url);
    let connection_pool = create_connection_pool(fake_conns).await;

    // When
    assert!(connection_pool.close().await.is_ok());

    // Then
    assert!(writer_rx.recv().await.is_none());
}

#[tokio::test]
async fn test_connection_send_single_message() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");
    let buffer_size = 5;

    let envelope = Envelope::make_command("/foo", "/bar", Some("Hello".into()));

    let mut fake_conns = FakeConnections::new();
    let (_, mut writer_rx) = fake_conns.add_connection(host_url);
    let client_conn_request_tx = create_mock_conn_request_loop(fake_conns).await;

    let connection = ClientConnection::new(path, buffer_size, &client_conn_request_tx)
        .await
        .unwrap();

    // When
    connection.tx.send(envelope.clone()).await.unwrap();

    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), envelope)
    );
}

#[tokio::test]
async fn test_connection_send_multiple_messages() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");
    let buffer_size = 5;

    let first_envelope = Envelope::make_command("/foo", "/bar", Some("First message".into()));
    let second_envelope = Envelope::make_command("/foo", "/bar", Some("Second message".into()));
    let third_envelope = Envelope::make_command("/foo", "/bar", Some("Third message".into()));

    let mut fake_conns = FakeConnections::new();
    let (_, mut writer_rx) = fake_conns.add_connection(host_url);
    let client_conn_request_tx = create_mock_conn_request_loop(fake_conns).await;

    let mut connection = ClientConnection::new(path, buffer_size, &client_conn_request_tx)
        .await
        .unwrap();

    let connection_sender = &mut connection.tx;
    // When
    connection_sender
        .send(first_envelope.clone())
        .await
        .unwrap();
    connection_sender
        .send(second_envelope.clone())
        .await
        .unwrap();
    connection_sender
        .send(third_envelope.clone())
        .await
        .unwrap();

    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), first_envelope)
    );
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), second_envelope)
    );
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), third_envelope)
    );
}

#[tokio::test]
async fn test_connection_send_and_receive_messages() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");
    let buffer_size = 5;

    let envelope_sent = Envelope::make_command("/foo", "/bar", Some("message_sent".into()));
    let envelope_received = Envelope::make_command("/foo", "/bar", Some("message_received".into()));

    let mut fake_conns = FakeConnections::new();
    let (reader_tx, mut writer_rx) = fake_conns.add_connection(host_url);
    let client_conn_request_tx = create_mock_conn_request_loop(fake_conns).await;

    let mut connection = ClientConnection::new(path, buffer_size, &client_conn_request_tx)
        .await
        .unwrap();

    // When
    connection.tx.send(envelope_sent.clone()).await.unwrap();
    reader_tx.send(envelope_received.clone()).await.unwrap();

    // Then
    let pool_message = connection.rx.take().unwrap().recv().await.unwrap();
    assert_eq!(pool_message, envelope_received);

    assert_eq!(
        writer_rx.recv().await.unwrap(),
        TaggedEnvelope(RoutingAddr::client(), envelope_sent)
    );
}

#[tokio::test]
async fn test_connection_receive_message_error() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");
    let buffer_size = 5;

    let mut fake_conns = FakeConnections::new();
    let (reader_tx, _writer_rx) = fake_conns.add_connection(host_url);
    let client_conn_request_tx = create_mock_conn_request_loop(fake_conns).await;

    let connection = ClientConnection::new(path, buffer_size, &client_conn_request_tx)
        .await
        .unwrap();

    // When
    drop(reader_tx);
    let result = connection._receive_handle.await.unwrap();

    // Then
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap(),
        ConnectionError::Closed(CloseError::unexpected())
    );
}

#[tokio::test]
async fn test_new_connection_send_message_error() {
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let path = AbsolutePath::new(host_url.clone(), "/foo", "/bar");
    let buffer_size = 5;

    let mut fake_conns = FakeConnections::new();
    let (_reader_tx, _writer_rx) = fake_conns.add_connection(host_url);
    let client_conn_request_tx = create_mock_conn_request_loop(fake_conns).await;

    let connection = ClientConnection::new(path, buffer_size, &client_conn_request_tx)
        .await
        .unwrap();

    let ClientConnection {
        tx, _send_handle, ..
    } = connection;

    drop(tx);

    let result = _send_handle.await.unwrap();
    // Then
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap(),
        ConnectionError::Closed(CloseError::unexpected())
    );
}
