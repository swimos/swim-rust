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

use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::connections::factory::WebsocketFactory;
use crate::connections::{
    Connection, ConnectionError, ConnectionFactory, ConnectionPool, ConnectionPoolMessage,
    SwimConnection,
};
use async_trait::async_trait;
use futures::future::{ready, Ready};
use futures::task::{Context, Poll};
use futures::Sink;
use futures_util::stream::Stream;
use tokio::macros::support::Pin;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;

#[tokio::test]
async fn test_connection_pool_send_single_message_single_connection() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1").unwrap();
    let text = "Hello";
    let (router_tx, _router_rx) = mpsc::channel(5);

    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };

    let factory = TestConnectionFactory {
        write_stream,
        read_stream,
    };

    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    let rx = connection_pool.request_connection(host_url).unwrap();
    let mut connection_sender = rx.await.unwrap().unwrap();
    // When
    connection_sender.send_message(text).await.unwrap();

    // Then
    assert_eq!(writer_rx.recv().await.unwrap().to_text().unwrap(), "Hello");
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_single_connection() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1").unwrap();
    let first_text = "First_Text";
    let second_text = "Second_Text";
    let (router_tx, _router_rx) = mpsc::channel(5);

    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };

    let factory = TestConnectionFactory {
        write_stream,
        read_stream,
    };

    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    let rx = connection_pool
        .request_connection(host_url.clone())
        .unwrap();
    let mut first_connection_sender = rx.await.unwrap().unwrap();

    let rx = connection_pool.request_connection(host_url).unwrap();
    let mut second_connection_sender = rx.await.unwrap().unwrap();
    // When
    first_connection_sender
        .send_message(first_text)
        .await
        .unwrap();
    second_connection_sender
        .send_message(second_text)
        .await
        .unwrap();

    // Then
    assert_eq!(
        writer_rx.recv().await.unwrap().to_text().unwrap(),
        "First_Text"
    );
    assert_eq!(
        writer_rx.recv().await.unwrap().to_text().unwrap(),
        "Second_Text"
    );
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_multiple_connections() {
    // Given
    let buffer_size = 5;
    let first_host_url = url::Url::parse("ws://127.0.0.1").unwrap();
    let second_host_url = url::Url::parse("ws://127.0.0.2").unwrap();
    let third_host_url = url::Url::parse("ws://127.0.0.3").unwrap();
    let first_text = "First_Text";
    let second_text = "Second_Text";
    let third_text = "Third_Text";
    let (router_tx, _router_rx) = mpsc::channel(5);

    let mut read_streams = Vec::<TestReadStream>::new();
    let mut write_streams = Vec::<TestWriteStream>::new();

    let (first_writer_tx, mut first_writer_rx) = mpsc::channel(5);

    let first_write_stream = TestWriteStream {
        tx: first_writer_tx,
        error: false,
    };
    let first_read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };

    let (second_writer_tx, mut second_writer_rx) = mpsc::channel(5);

    let second_write_stream = TestWriteStream {
        tx: second_writer_tx,
        error: false,
    };
    let second_read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };

    let (third_writer_tx, mut third_writer_rx) = mpsc::channel(5);

    let third_write_stream = TestWriteStream {
        tx: third_writer_tx,
        error: false,
    };
    let third_read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };

    read_streams.push(first_read_stream);
    write_streams.push(first_write_stream);
    read_streams.push(second_read_stream);
    write_streams.push(second_write_stream);
    read_streams.push(third_read_stream);
    write_streams.push(third_write_stream);

    let factory = TestMultipleConnectionFactory {
        write_streams,
        read_streams,
    };

    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    let rx = connection_pool.request_connection(first_host_url).unwrap();
    let mut first_connection_sender = rx.await.unwrap().unwrap();

    let rx = connection_pool.request_connection(second_host_url).unwrap();
    let mut second_connection_sender = rx.await.unwrap().unwrap();

    let rx = connection_pool.request_connection(third_host_url).unwrap();
    let mut third_connection_sender = rx.await.unwrap().unwrap();

    // When
    first_connection_sender
        .send_message(first_text)
        .await
        .unwrap();
    second_connection_sender
        .send_message(second_text)
        .await
        .unwrap();
    third_connection_sender
        .send_message(third_text)
        .await
        .unwrap();

    // Then
    assert_eq!(
        first_writer_rx.recv().await.unwrap().to_text().unwrap(),
        "First_Text"
    );
    assert_eq!(
        second_writer_rx.recv().await.unwrap().to_text().unwrap(),
        "Second_Text"
    );
    assert_eq!(
        third_writer_rx.recv().await.unwrap().to_text().unwrap(),
        "Third_Text"
    );
}

#[tokio::test]
async fn test_connection_pool_receive_single_message_single_connection() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1").unwrap();
    let mut items = Vec::new();
    items.push(Message::text("new_message"));
    let (writer_tx, _writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items,
        error: false,
    };
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let factory = TestConnectionFactory {
        write_stream,
        read_stream,
    };
    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    // When
    let _rx = connection_pool.request_connection(host_url).unwrap();

    // Then
    let pool_message = router_rx.recv().await.unwrap().unwrap();
    assert_eq!(&pool_message.message, "new_message");
    assert_eq!(&pool_message.host, "ws://127.0.0.1/");
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_single_connection() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1").unwrap();
    let mut items = Vec::new();
    items.push(Message::text("first_message"));
    items.push(Message::text("second_message"));
    items.push(Message::text("third_message"));
    let (writer_tx, _writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items,
        error: false,
    };
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let factory = TestConnectionFactory {
        write_stream,
        read_stream,
    };
    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    // When
    let _rx = connection_pool.request_connection(host_url).unwrap();

    // Then
    let first_pool_message = router_rx.recv().await.unwrap().unwrap();
    let second_pool_message = router_rx.recv().await.unwrap().unwrap();
    let third_pool_message = router_rx.recv().await.unwrap().unwrap();

    assert_eq!(&first_pool_message.message, "first_message");
    assert_eq!(&first_pool_message.host, "ws://127.0.0.1/");
    assert_eq!(&second_pool_message.message, "second_message");
    assert_eq!(&second_pool_message.host, "ws://127.0.0.1/");
    assert_eq!(&third_pool_message.message, "third_message");
    assert_eq!(&third_pool_message.host, "ws://127.0.0.1/");
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_multiple_connections() {
    // Given
    let buffer_size = 5;

    let mut first_items = Vec::new();
    let mut second_items = Vec::new();
    let mut third_items = Vec::new();

    first_items.push(Message::text("first_message"));
    second_items.push(Message::text("second_message"));
    third_items.push(Message::text("third_message"));

    let first_host_url = url::Url::parse("ws://127.0.0.1").unwrap();
    let second_host_url = url::Url::parse("ws://127.0.0.2").unwrap();
    let third_host_url = url::Url::parse("ws://127.0.0.3").unwrap();

    let (router_tx, mut router_rx) = mpsc::channel(5);

    let mut read_streams = Vec::<TestReadStream>::new();
    let mut write_streams = Vec::<TestWriteStream>::new();

    let (first_writer_tx, _first_writer_rx) = mpsc::channel(5);

    let first_write_stream = TestWriteStream {
        tx: first_writer_tx,
        error: false,
    };
    let first_read_stream = TestReadStream {
        items: first_items,
        error: false,
    };

    let (second_writer_tx, _second_writer_rx) = mpsc::channel(5);

    let second_write_stream = TestWriteStream {
        tx: second_writer_tx,
        error: false,
    };
    let second_read_stream = TestReadStream {
        items: second_items,
        error: false,
    };

    let (third_writer_tx, _third_writer_rx) = mpsc::channel(5);

    let third_write_stream = TestWriteStream {
        tx: third_writer_tx,
        error: false,
    };
    let third_read_stream = TestReadStream {
        items: third_items,
        error: false,
    };

    read_streams.push(first_read_stream);
    write_streams.push(first_write_stream);
    read_streams.push(second_read_stream);
    write_streams.push(second_write_stream);
    read_streams.push(third_read_stream);
    write_streams.push(third_write_stream);

    let factory = TestMultipleConnectionFactory {
        write_streams,
        read_streams,
    };

    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    // When

    let _rx = connection_pool.request_connection(first_host_url).unwrap();
    let _rx = connection_pool.request_connection(second_host_url).unwrap();
    let _rx = connection_pool.request_connection(third_host_url).unwrap();

    // Then
    let first_pool_message = router_rx.recv().await.unwrap().unwrap();
    let second_pool_message = router_rx.recv().await.unwrap().unwrap();
    let third_pool_message = router_rx.recv().await.unwrap().unwrap();

    assert_eq!(&first_pool_message.message, "first_message");
    assert_eq!(&first_pool_message.host, "ws://127.0.0.1/");
    assert_eq!(&second_pool_message.message, "second_message");
    assert_eq!(&second_pool_message.host, "ws://127.0.0.2/");
    assert_eq!(&third_pool_message.message, "third_message");
    assert_eq!(&third_pool_message.host, "ws://127.0.0.3/");
}

#[tokio::test]
async fn test_connection_pool_send_and_receive_messages() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1").unwrap();
    let mut items = Vec::new();
    items.push(Message::text("recv_baz"));
    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items,
        error: false,
    };
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let factory = TestConnectionFactory {
        write_stream,
        read_stream,
    };
    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    let rx = connection_pool.request_connection(host_url).unwrap();
    let mut connection_sender = rx.await.unwrap().unwrap();
    // When
    connection_sender.send_message("send_bar").await.unwrap();
    // Then
    let pool_message = router_rx.recv().await.unwrap().unwrap();

    assert_eq!(&pool_message.message, "recv_baz");

    assert_eq!(&pool_message.host, "ws://127.0.0.1/");

    assert_eq!(
        writer_rx.recv().await.unwrap().to_text().unwrap(),
        "send_bar"
    );
}

#[tokio::test]
async fn test_connection_pool_connection_error() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1").unwrap();
    let (router_tx, _router_rx) = mpsc::channel(5);
    let (writer_tx, _writer_rx) = mpsc::channel(5);

    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };

    let factory = TestErrorConnectionFactory {
        error_connection_count: 1,
        write_stream,
        read_stream,
    };
    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    // When
    let rx = connection_pool
        .request_connection(host_url.clone())
        .unwrap();
    let connection_sender = rx.await.unwrap();

    // Then
    assert!(connection_sender.is_err());
}

#[tokio::test]
async fn test_connection_pool_connection_error_send_message() {
    // Given
    let buffer_size = 5;
    let host_url = url::Url::parse("ws://127.0.0.1").unwrap();
    let text = "Test_message";
    let (router_tx, _router_rx) = mpsc::channel(5);
    let (writer_tx, mut writer_rx) = mpsc::channel(5);

    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };

    let factory = TestErrorConnectionFactory {
        error_connection_count: 1,
        write_stream,
        read_stream,
    };
    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    // When
    let rx = connection_pool
        .request_connection(host_url.clone())
        .unwrap();
    let first_connection_sender = rx.await.unwrap();

    let rx = connection_pool.request_connection(host_url).unwrap();
    let mut second_connection_sender = rx.await.unwrap().unwrap();
    second_connection_sender.send_message(text).await.unwrap();

    // Then
    assert!(first_connection_sender.is_err());
    assert_eq!(
        writer_rx.recv().await.unwrap().into_text().unwrap(),
        "Test_message"
    );
}

#[tokio::test]
async fn test_connection_pool_close() {
    // Given
    let buffer_size = 5;
    let (router_tx, mut router_rx) = mpsc::channel(5);

    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };

    let factory = TestConnectionFactory {
        write_stream,
        read_stream,
    };

    let connection_pool = ConnectionPool::new(buffer_size, router_tx, factory);

    // When
    connection_pool.close().await;

    // Then
    assert!(router_rx.recv().await.is_none());
    assert!(writer_rx.recv().await.is_none());
}

#[tokio::test]
async fn test_connection_receive_single_messages() {
    // Given
    let buffer_size = 5;
    let host = String::from("ws://127.0.0.1");
    let mut items = Vec::new();
    items.push(Message::text("foo"));
    let read_stream = TestReadStream {
        items,
        error: false,
    };
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    // When
    let _result = SwimConnection::receive_messages(read_stream, pool_tx, host).await;
    // Then
    let pool_message = pool_rx.recv().await.unwrap().unwrap();
    assert_eq!(&pool_message.message, "foo");
    assert_eq!(&pool_message.host, "ws://127.0.0.1");
}

#[tokio::test]
async fn test_connection_receive_multiple_messages() {
    // Given
    let buffer_size = 5;
    let host = String::from("ws://127.0.0.1");
    let mut items = Vec::new();
    items.push(Message::text("foo"));
    items.push(Message::text("bar"));
    items.push(Message::text("baz"));
    let read_stream = TestReadStream {
        items,
        error: false,
    };
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    // When
    let _result = SwimConnection::receive_messages(read_stream, pool_tx, host).await;
    // Then

    let first_message = pool_rx.recv().await.unwrap().unwrap();
    let second_message = pool_rx.recv().await.unwrap().unwrap();
    let third_message = pool_rx.recv().await.unwrap().unwrap();

    assert_eq!(&first_message.message, "foo");
    assert_eq!(&first_message.host, "ws://127.0.0.1");

    assert_eq!(&second_message.message, "bar");
    assert_eq!(&first_message.host, "ws://127.0.0.1");

    assert_eq!(&third_message.message, "baz");
    assert_eq!(&first_message.host, "ws://127.0.0.1");
}

#[tokio::test]
async fn test_connection_send_single_message() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999").unwrap();
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };
    let mut connection = SwimConnection::new(
        host,
        buffer_size,
        _pool_tx,
        TestWebsocketFactory {
            write_stream,
            read_stream,
        },
    )
    .await
    .unwrap();
    let mut connection_sender = connection.get_sender();
    // When
    connection_sender.send_message("foo").await.unwrap();
    // Then
    assert_eq!(writer_rx.recv().await.unwrap().to_text().unwrap(), "foo");
}

#[tokio::test]
async fn test_connection_send_multiple_messages() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999").unwrap();
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };
    let mut connection = SwimConnection::new(
        host,
        buffer_size,
        _pool_tx,
        TestWebsocketFactory {
            write_stream,
            read_stream,
        },
    )
    .await
    .unwrap();
    let mut connection_sender = connection.get_sender();
    // When
    connection_sender.send_message("foo").await.unwrap();
    connection_sender.send_message("bar").await.unwrap();
    connection_sender.send_message("baz").await.unwrap();
    // Then
    assert_eq!(writer_rx.recv().await.unwrap().to_text().unwrap(), "foo");
    assert_eq!(writer_rx.recv().await.unwrap().to_text().unwrap(), "bar");
    assert_eq!(writer_rx.recv().await.unwrap().to_text().unwrap(), "baz");
}

#[tokio::test]
async fn test_connection_send_and_receive_messages() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999").unwrap();
    let buffer_size = 5;
    let mut items = Vec::new();
    items.push(Message::text("message_received"));
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items,
        error: false,
    };
    let mut connection = SwimConnection::new(
        host,
        buffer_size,
        pool_tx,
        TestWebsocketFactory {
            write_stream,
            read_stream,
        },
    )
    .await
    .unwrap();
    let mut connection_sender = connection.get_sender();
    // When
    connection_sender
        .send_message("message_sent")
        .await
        .unwrap();
    // Then
    let pool_message = pool_rx.recv().await.unwrap().unwrap();
    assert_eq!(&pool_message.message, "message_received");
    assert_eq!(&pool_message.host, "ws://127.0.0.1:9999/");

    assert_eq!(
        writer_rx.recv().await.unwrap().to_text().unwrap(),
        "message_sent"
    );
}

#[tokio::test]
async fn test_connection_receive_message_error() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999").unwrap();
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);

    let (writer_tx, _writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream {
        items: Vec::new(),
        error: true,
    };
    let connection = SwimConnection::new(
        host,
        buffer_size,
        _pool_tx,
        TestWebsocketFactory {
            write_stream,
            read_stream,
        },
    )
    .await
    .unwrap();
    // When
    let result = connection._receive_handler.await.unwrap();
    // Then
    assert!(result.is_err());
    assert_eq!(result.err(), Some(ConnectionError::ReceiveMessageError));
}

#[tokio::test]
async fn test_connection_connect_error() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999").unwrap();
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    // When
    let factory = TungsteniteWsFactory::new(buffer_size).await;
    let result = SwimConnection::new(host, buffer_size, _pool_tx, factory).await;
    // Then
    assert!(result.is_err());
    assert_eq!(result.err(), Some(ConnectionError::ConnectError));
}

#[tokio::test]
async fn test_new_connection_send_message_error() {
    // Given
    let host = url::Url::parse("ws://127.0.0.1:9999").unwrap();
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);

    let (writer_tx, _writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: true,
    };
    let read_stream = TestReadStream {
        items: Vec::new(),
        error: false,
    };
    let connection = SwimConnection::new(
        host,
        buffer_size,
        _pool_tx,
        TestWebsocketFactory {
            write_stream,
            read_stream,
        },
    )
    .await
    .unwrap();
    // When
    let result = connection._send_handler.await.unwrap();
    // Then
    assert!(result.is_err());
    assert_eq!(result.err(), Some(ConnectionError::SendMessageError));
}

struct TestConnectionFactory {
    write_stream: TestWriteStream,
    read_stream: TestReadStream,
}

#[async_trait]
impl ConnectionFactory for TestConnectionFactory {
    type ConnectionType = TestConnection;

    async fn create_connection(
        &mut self,
        host: url::Url,
        buffer_size: usize,
        pool_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError> {
        TestConnection::new(
            host,
            buffer_size,
            pool_tx,
            self.write_stream.clone(),
            self.read_stream.clone(),
        )
    }
}

struct TestMultipleConnectionFactory {
    write_streams: Vec<TestWriteStream>,
    read_streams: Vec<TestReadStream>,
}

#[async_trait]
impl ConnectionFactory for TestMultipleConnectionFactory {
    type ConnectionType = TestConnection;

    async fn create_connection(
        &mut self,
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError> {
        TestConnection::new(
            host_url,
            buffer_size,
            router_tx,
            self.write_streams.drain(0..1).next().unwrap(),
            self.read_streams.drain(0..1).next().unwrap(),
        )
    }
}

struct TestErrorConnectionFactory {
    error_connection_count: u32,
    write_stream: TestWriteStream,
    read_stream: TestReadStream,
}

#[async_trait]
impl ConnectionFactory for TestErrorConnectionFactory {
    type ConnectionType = TestConnection;

    async fn create_connection(
        &mut self,
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError> {
        if self.error_connection_count > 0 {
            self.error_connection_count = self.error_connection_count - 1;
            Err(ConnectionError::ConnectError)
        } else {
            TestConnection::new(
                host_url,
                buffer_size,
                router_tx,
                self.write_stream.clone(),
                self.read_stream.clone(),
            )
        }
    }
}

struct TestConnection {
    tx: mpsc::Sender<Message>,
    _send_handler: JoinHandle<Result<(), ConnectionError>>,
    _receive_handler: JoinHandle<Result<(), ConnectionError>>,
}

impl TestConnection {
    fn new(
        host_url: url::Url,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        write_stream: TestWriteStream,
        read_stream: TestReadStream,
    ) -> Result<TestConnection, ConnectionError> {
        let (tx, rx) = mpsc::channel(buffer_size);

        let receive = TestConnection::receive_messages(
            read_stream,
            router_tx,
            host_url.to_string().to_owned(),
        );
        let send = TestConnection::send_messages(write_stream, rx);

        let send_handler = tokio::spawn(send);
        let receive_handler = tokio::spawn(receive);

        Ok(TestConnection {
            tx,
            _send_handler: send_handler,
            _receive_handler: receive_handler,
        })
    }
}

#[async_trait]
impl Connection for TestConnection {
    fn get_tx(&mut self) -> &mut mpsc::Sender<Message> {
        &mut self.tx
    }
}

struct TestWebsocketFactory {
    write_stream: TestWriteStream,
    read_stream: TestReadStream,
}

impl WebsocketFactory for TestWebsocketFactory {
    type WsStream = TestReadStream;
    type WsSink = TestWriteStream;
    type ConnectFut = Ready<Result<(Self::WsSink, Self::WsStream), ConnectionError>>;

    fn connect(&mut self, _url: Url) -> Self::ConnectFut {
        ready(Ok((self.write_stream.clone(), self.read_stream.clone())))
    }
}

#[derive(Clone)]
struct TestReadStream {
    items: Vec<Message>,
    error: bool,
}

impl Stream for TestReadStream {
    type Item = Result<Message, ConnectionError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.error {
            Poll::Ready(Some(Err(ConnectionError::ReceiveMessageError)))
        } else {
            if self.items.is_empty() {
                Poll::Ready(None)
            } else {
                let message = self.items.drain(0..1).next();
                Poll::Ready(Some(message.ok_or(ConnectionError::SendMessageError)))
            }
        }
    }
}

#[derive(Clone)]
struct TestWriteStream {
    tx: mpsc::Sender<Message>,
    error: bool,
}

impl Sink<Message> for TestWriteStream {
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.error {
            Poll::Ready(Err(()))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        if self.error {
            Err(())
        } else {
            self.tx.try_send(item).unwrap();
            Ok(())
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.error {
            Poll::Ready(Err(()))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.error {
            Poll::Ready(Err(()))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
