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
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use swim_common::routing_server::remote::test_fixture::{
    ErrorMode, FakeConnections, FakeSocket, FakeWebsockets,
};

#[tokio::test]
async fn test_connection_pool_send_single_message_single_connection() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let text = "Hello";

    let mut sockets = HashMap::new();

    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);

    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec![],
            true,
            Some(writer_tx),
            ErrorMode::None,
        )),
    );

    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let ws_factory = FakeWebsockets;

    let mut connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    let (mut connection_sender, _connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender.send_message(text.into()).await.unwrap();

    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("Hello".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_single_connection() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let first_text = "First_Text";
    let second_text = "Second_Text";

    let mut sockets = HashMap::new();

    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);

    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec![],
            true,
            Some(writer_tx),
            ErrorMode::None,
        )),
    );

    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let ws_factory = FakeWebsockets;

    let mut connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    let (mut first_connection_sender, _first_connection_receiver) = connection_pool
        .request_connection(host_url.clone(), false)
        .await
        .unwrap()
        .unwrap();

    let (mut second_connection_sender, _second_connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    // When
    first_connection_sender
        .send_message(first_text.into())
        .await
        .unwrap();
    second_connection_sender
        .send_message(second_text.into())
        .await
        .unwrap();

    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("First_Text".to_string())
    );
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("Second_Text".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_multiple_connections() {
    // Given
    let buffer_size = 5;
    let first_host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let second_host_url = url::Url::parse("ws://127.0.0.2:9001/").unwrap();
    let third_host_url = url::Url::parse("ws://127.0.0.3:9001/").unwrap();
    let first_text = "First_Text";
    let second_text = "Second_Text";
    let third_text = "Third_Text";

    let mut sockets = HashMap::new();

    let (first_writer_tx, mut first_writer_rx) = mpsc::channel(buffer_size);
    let (second_writer_tx, mut second_writer_rx) = mpsc::channel(buffer_size);
    let (third_writer_tx, mut third_writer_rx) = mpsc::channel(buffer_size);

    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec![],
            true,
            Some(first_writer_tx),
            ErrorMode::None,
        )),
    );

    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 9001),
        Ok(FakeSocket::new(
            vec![],
            true,
            Some(second_writer_tx),
            ErrorMode::None,
        )),
    );

    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 9001),
        Ok(FakeSocket::new(
            vec![],
            true,
            Some(third_writer_tx),
            ErrorMode::None,
        )),
    );

    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let ws_factory = FakeWebsockets;

    let mut connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    let (mut first_connection_sender, _first_connection_receiver) = connection_pool
        .request_connection(first_host_url, false)
        .await
        .unwrap()
        .unwrap();

    let (mut second_connection_sender, _second_connection_receiver) = connection_pool
        .request_connection(second_host_url, false)
        .await
        .unwrap()
        .unwrap();

    let (mut third_connection_sender, _third_connection_receiver) = connection_pool
        .request_connection(third_host_url, false)
        .await
        .unwrap()
        .unwrap();

    // When
    first_connection_sender
        .send_message(first_text.into())
        .await
        .unwrap();
    second_connection_sender
        .send_message(second_text.into())
        .await
        .unwrap();
    third_connection_sender
        .send_message(third_text.into())
        .await
        .unwrap();

    // Then
    assert_eq!(
        first_writer_rx.recv().await.unwrap(),
        WsMessage::Text("First_Text".to_string())
    );
    assert_eq!(
        second_writer_rx.recv().await.unwrap(),
        WsMessage::Text("Second_Text".to_string())
    );
    assert_eq!(
        third_writer_rx.recv().await.unwrap(),
        WsMessage::Text("Third_Text".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_receive_single_message_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();

    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec!["new_message".into()],
            true,
            None,
            ErrorMode::None,
        )),
    );

    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let ws_factory = FakeWebsockets;

    let mut connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    // When
    let (_connection_sender, connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    // Then
    let pool_message = connection_receiver.unwrap().recv().await.unwrap();
    assert_eq!(pool_message, WsMessage::Text("new_message".to_string()));
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_single_connection() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();

    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec![
                "first_message".into(),
                "second_message".into(),
                "third_message".into(),
            ],
            true,
            None,
            ErrorMode::None,
        )),
    );

    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let ws_factory = FakeWebsockets;

    let mut connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    // When
    let (_connection_sender, connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    let mut connection_receiver = connection_receiver.unwrap();

    // Then
    let first_pool_message = connection_receiver.recv().await.unwrap();
    let second_pool_message = connection_receiver.recv().await.unwrap();
    let third_pool_message = connection_receiver.recv().await.unwrap();

    assert_eq!(
        first_pool_message,
        WsMessage::Text("first_message".to_string())
    );
    assert_eq!(
        second_pool_message,
        WsMessage::Text("second_message".to_string())
    );
    assert_eq!(
        third_pool_message,
        WsMessage::Text("third_message".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_multiple_connections() {
    // Given
    let first_host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let second_host_url = url::Url::parse("ws://127.0.0.2:9001/").unwrap();
    let third_host_url = url::Url::parse("ws://127.0.0.3:9001//").unwrap();

    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec!["first_message".into()],
            true,
            None,
            ErrorMode::None,
        )),
    );

    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 9001),
        Ok(FakeSocket::new(
            vec!["second_message".into()],
            true,
            None,
            ErrorMode::None,
        )),
    );

    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 9001),
        Ok(FakeSocket::new(
            vec!["third_message".into()],
            true,
            None,
            ErrorMode::None,
        )),
    );

    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let ws_factory = FakeWebsockets;

    let mut connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    // When
    let (_first_sender, mut first_receiver) = connection_pool
        .request_connection(first_host_url, false)
        .await
        .unwrap()
        .unwrap();

    let (_second_sender, mut second_receiver) = connection_pool
        .request_connection(second_host_url, false)
        .await
        .unwrap()
        .unwrap();

    let (_third_sender, mut third_receiver) = connection_pool
        .request_connection(third_host_url, false)
        .await
        .unwrap()
        .unwrap();

    // Then
    let first_pool_message = first_receiver.take().unwrap().recv().await.unwrap();
    let second_pool_message = second_receiver.take().unwrap().recv().await.unwrap();
    let third_pool_message = third_receiver.take().unwrap().recv().await.unwrap();

    assert_eq!(
        first_pool_message,
        WsMessage::Text("first_message".to_string())
    );
    assert_eq!(
        second_pool_message,
        WsMessage::Text("second_message".to_string())
    );
    assert_eq!(
        third_pool_message,
        WsMessage::Text("third_message".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_send_and_receive_messages() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();

    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec!["recv_baz".into()],
            true,
            Some(writer_tx),
            ErrorMode::None,
        )),
    );

    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let ws_factory = FakeWebsockets;

    let mut connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    let (mut connection_sender, connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    // When
    connection_sender
        .send_message("send_bar".into())
        .await
        .unwrap();
    // Then
    let pool_message = connection_receiver.unwrap().recv().await.unwrap();

    assert_eq!(pool_message, WsMessage::Text("recv_baz".to_string()));

    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("send_bar".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_connection_error() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();

    let sockets = HashMap::new();
    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let ws_factory = FakeWebsockets;

    let mut connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    // When
    let connection = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap();

    // Then
    assert!(connection.is_err());
}

#[tokio::test]
async fn test_connection_pool_connection_error_send_message() {
    // Given
    let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let (writer_tx, mut writer_rx) = mpsc::channel(5);

    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec![],
            true,
            Some(writer_tx),
            ErrorMode::None,
        )),
    );

    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 1);
    let ws_factory = FakeWebsockets;

    let mut connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    // When
    let first_connection = connection_pool
        .request_connection(host_url.clone(), false)
        .await
        .unwrap();

    let (mut second_connection_sender, _second_connection_receiver) = connection_pool
        .request_connection(host_url, false)
        .await
        .unwrap()
        .unwrap();

    second_connection_sender
        .send_message("Test_message".into())
        .await
        .unwrap();

    // Then
    assert!(first_connection.is_err());
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("Test_message".to_string())
    );
}

#[tokio::test]
async fn test_connection_pool_close() {
    // Given
    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec![],
            true,
            Some(writer_tx),
            ErrorMode::None,
        )),
    );

    let conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let ws_factory = FakeWebsockets;

    let connection_pool =
        SwimConnPool::new(ConnectionPoolParams::default(), conn_factory, ws_factory);

    // When
    assert!(connection_pool.close().await.is_ok());

    // Then
    assert!(writer_rx.recv().await.is_none());
}

#[tokio::test]
async fn test_connection_send_single_message() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let buffer_size = 5;

    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec![],
            true,
            Some(writer_tx),
            ErrorMode::None,
        )),
    );

    let mut conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let mut ws_factory = FakeWebsockets;

    let connection = ClientConnection::new(host, buffer_size, &mut conn_factory, &mut ws_factory)
        .await
        .unwrap();

    // When
    connection.tx.send("foo".into()).await.unwrap();
    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("foo".to_string())
    );
}

#[tokio::test]
async fn test_connection_send_multiple_messages() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let buffer_size = 5;

    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec![],
            true,
            Some(writer_tx),
            ErrorMode::None,
        )),
    );

    let mut conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let mut ws_factory = FakeWebsockets;

    let mut connection =
        ClientConnection::new(host, buffer_size, &mut conn_factory, &mut ws_factory)
            .await
            .unwrap();

    let connection_sender = &mut connection.tx;
    // When
    connection_sender.send("foo".into()).await.unwrap();
    connection_sender.send("bar".into()).await.unwrap();
    connection_sender.send("baz".into()).await.unwrap();
    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("foo".to_string())
    );
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("bar".to_string())
    );
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("baz".to_string())
    );
}

#[tokio::test]
async fn test_connection_send_and_receive_messages() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let buffer_size = 5;

    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(
            vec!["message_received".into()],
            true,
            Some(writer_tx),
            ErrorMode::None,
        )),
    );

    let mut conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let mut ws_factory = FakeWebsockets;

    let mut connection =
        ClientConnection::new(host, buffer_size, &mut conn_factory, &mut ws_factory)
            .await
            .unwrap();

    // When
    connection.tx.send("message_sent".into()).await.unwrap();
    // Then
    let pool_message = connection.rx.take().unwrap().recv().await.unwrap();
    assert_eq!(
        pool_message,
        WsMessage::Text("message_received".to_string())
    );

    assert_eq!(
        writer_rx.recv().await.unwrap(),
        WsMessage::Text("message_sent".to_string())
    );
}

#[tokio::test]
async fn test_connection_receive_message_error() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let buffer_size = 5;

    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(vec![], false, None, ErrorMode::Receive)),
    );

    let mut conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let mut ws_factory = FakeWebsockets;

    let connection = ClientConnection::new(host, buffer_size, &mut conn_factory, &mut ws_factory)
        .await
        .unwrap();
    // When
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
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let buffer_size = 5;

    let mut sockets = HashMap::new();
    sockets.insert(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001),
        Ok(FakeSocket::new(vec![], false, None, ErrorMode::Send)),
    );

    let mut conn_factory = FakeConnections::new(sockets, HashMap::new(), None, 0);
    let mut ws_factory = FakeWebsockets;

    let connection = ClientConnection::new(host, buffer_size, &mut conn_factory, &mut ws_factory)
        .await
        .unwrap();
    // When
    let result = connection._send_handle.await.unwrap();
    // Then
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap(),
        ConnectionError::Closed(CloseError::unexpected())
    );
}
