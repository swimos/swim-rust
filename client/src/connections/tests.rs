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

use crate::connections::{
    Connection, ConnectionError, ConnectionPool, ConnectionPoolMessage, ConnectionProducer,
    SwimConnection, SwimWebsocketProducer, WebsocketProducer,
};
use async_trait::async_trait;
use futures::task::{Context, Poll};
use futures::Sink;
use futures_util::stream::Stream;
use tokio::macros::support::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

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
    let read_stream = TestReadStream { items: Vec::new() };

    let producer = TestConnectionProducer {
        write_stream,
        read_stream,
    };

    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, producer);

    let (tx, rx) = oneshot::channel();
    connection_pool.request_connection(host_url, tx).unwrap();
    let mut connection_handler = rx.await.unwrap();

    // When
    connection_handler.send_message(text).await.unwrap();

    // Then
    assert_eq!("Hello", writer_rx.recv().await.unwrap().to_text().unwrap());
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_single_connection() {
    // Given
    let buffer_size = 5;
    let host = url::Url::parse("ws://127.0.0.1").unwrap();
    let first_text = "First_Text";
    let second_text = "Second_Text";
    let (router_tx, _router_rx) = mpsc::channel(5);

    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream { items: Vec::new() };

    let producer = TestConnectionProducer {
        write_stream,
        read_stream,
    };

    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, producer);

    let (tx, rx) = oneshot::channel();
    connection_pool
        .request_connection(host.clone(), tx)
        .unwrap();
    let mut first_connection_handler = rx.await.unwrap();

    let (tx, rx) = oneshot::channel();
    connection_pool.request_connection(host, tx).unwrap();
    let mut second_connection_handler = rx.await.unwrap();
    // When
    first_connection_handler
        .send_message(first_text)
        .await
        .unwrap();
    second_connection_handler
        .send_message(second_text)
        .await
        .unwrap();

    // Then
    assert_eq!(
        "First_Text",
        writer_rx.recv().await.unwrap().to_text().unwrap()
    );
    assert_eq!(
        "Second_Text",
        writer_rx.recv().await.unwrap().to_text().unwrap()
    );
}

#[tokio::test]
async fn test_connection_pool_send_multiple_messages_multiple_connections() {
    // Given
    let buffer_size = 5;
    let first_host = url::Url::parse("ws://127.0.0.1").unwrap();
    let second_host = url::Url::parse("ws://127.0.0.2").unwrap();
    let third_host = url::Url::parse("ws://127.0.0.3").unwrap();
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
    let first_read_stream = TestReadStream { items: Vec::new() };

    let (second_writer_tx, mut second_writer_rx) = mpsc::channel(5);

    let second_write_stream = TestWriteStream {
        tx: second_writer_tx,
        error: false,
    };
    let second_read_stream = TestReadStream { items: Vec::new() };

    let (third_writer_tx, mut third_writer_rx) = mpsc::channel(5);

    let third_write_stream = TestWriteStream {
        tx: third_writer_tx,
        error: false,
    };
    let third_read_stream = TestReadStream { items: Vec::new() };

    read_streams.push(first_read_stream);
    write_streams.push(first_write_stream);
    read_streams.push(second_read_stream);
    write_streams.push(second_write_stream);
    read_streams.push(third_read_stream);
    write_streams.push(third_write_stream);

    let producer = TestMultipleConnectionProducer {
        write_streams,
        read_streams,
    };

    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, producer);

    let (tx, rx) = oneshot::channel();
    connection_pool.request_connection(first_host, tx).unwrap();
    let mut first_connection_handler = rx.await.unwrap();

    let (tx, rx) = oneshot::channel();
    connection_pool.request_connection(second_host, tx).unwrap();
    let mut second_connection_handler = rx.await.unwrap();

    let (tx, rx) = oneshot::channel();
    connection_pool.request_connection(third_host, tx).unwrap();
    let mut third_connection_handler = rx.await.unwrap();

    // When
    first_connection_handler
        .send_message(first_text)
        .await
        .unwrap();
    second_connection_handler
        .send_message(second_text)
        .await
        .unwrap();
    third_connection_handler
        .send_message(third_text)
        .await
        .unwrap();

    // Then
    assert_eq!(
        "First_Text",
        first_writer_rx.recv().await.unwrap().to_text().unwrap()
    );
    assert_eq!(
        "Second_Text",
        second_writer_rx.recv().await.unwrap().to_text().unwrap()
    );
    assert_eq!(
        "Third_Text",
        third_writer_rx.recv().await.unwrap().to_text().unwrap()
    );
}

#[tokio::test]
async fn test_connection_pool_receive_single_message_single_connection() {
    // Given
    let buffer_size = 5;
    let host = url::Url::parse("ws://127.0.0.1").unwrap();
    let mut items = Vec::new();
    items.push(Message::text("new_message"));
    let (writer_tx, _writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream { items };
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let producer = TestConnectionProducer {
        write_stream,
        read_stream,
    };
    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, producer);

    let (tx, _rx) = oneshot::channel();
    // When
    connection_pool.request_connection(host, tx).unwrap();

    // Then
    let pool_message = router_rx.recv().await.unwrap().unwrap();
    assert_eq!("new_message", &pool_message.message);
    assert_eq!("ws://127.0.0.1/", &pool_message.host);
}

#[tokio::test]
async fn test_connection_pool_receive_multiple_messages_single_connection() {
    // Given
    let buffer_size = 5;
    let host = url::Url::parse("ws://127.0.0.1").unwrap();
    let mut items = Vec::new();
    items.push(Message::text("first_message"));
    items.push(Message::text("second_message"));
    items.push(Message::text("third_message"));
    let (writer_tx, _writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream { items };
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let producer = TestConnectionProducer {
        write_stream,
        read_stream,
    };
    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, producer);

    let (tx, _rx) = oneshot::channel();
    // When
    connection_pool.request_connection(host, tx).unwrap();

    // Then
    let first_pool_message = router_rx.recv().await.unwrap().unwrap();
    let second_pool_message = router_rx.recv().await.unwrap().unwrap();
    let third_pool_message = router_rx.recv().await.unwrap().unwrap();

    assert_eq!("first_message", &first_pool_message.message);
    assert_eq!("ws://127.0.0.1/", &first_pool_message.host);
    assert_eq!("second_message", &second_pool_message.message);
    assert_eq!("ws://127.0.0.1/", &second_pool_message.host);
    assert_eq!("third_message", &third_pool_message.message);
    assert_eq!("ws://127.0.0.1/", &third_pool_message.host);
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

    let first_host = url::Url::parse("ws://127.0.0.1").unwrap();
    let second_host = url::Url::parse("ws://127.0.0.2").unwrap();
    let third_host = url::Url::parse("ws://127.0.0.3").unwrap();

    let (router_tx, mut router_rx) = mpsc::channel(5);

    let mut read_streams = Vec::<TestReadStream>::new();
    let mut write_streams = Vec::<TestWriteStream>::new();

    let (first_writer_tx, _first_writer_rx) = mpsc::channel(5);

    let first_write_stream = TestWriteStream {
        tx: first_writer_tx,
        error: false,
    };
    let first_read_stream = TestReadStream { items: first_items };

    let (second_writer_tx, _second_writer_rx) = mpsc::channel(5);

    let second_write_stream = TestWriteStream {
        tx: second_writer_tx,
        error: false,
    };
    let second_read_stream = TestReadStream {
        items: second_items,
    };

    let (third_writer_tx, _third_writer_rx) = mpsc::channel(5);

    let third_write_stream = TestWriteStream {
        tx: third_writer_tx,
        error: false,
    };
    let third_read_stream = TestReadStream { items: third_items };

    read_streams.push(first_read_stream);
    write_streams.push(first_write_stream);
    read_streams.push(second_read_stream);
    write_streams.push(second_write_stream);
    read_streams.push(third_read_stream);
    write_streams.push(third_write_stream);

    let producer = TestMultipleConnectionProducer {
        write_streams,
        read_streams,
    };

    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, producer);

    // When
    let (tx, _rx) = oneshot::channel();
    connection_pool.request_connection(first_host, tx).unwrap();
    let (tx, _rx) = oneshot::channel();
    connection_pool.request_connection(second_host, tx).unwrap();
    let (tx, _rx) = oneshot::channel();
    connection_pool.request_connection(third_host, tx).unwrap();

    // Then
    let first_pool_message = router_rx.recv().await.unwrap().unwrap();
    let second_pool_message = router_rx.recv().await.unwrap().unwrap();
    let third_pool_message = router_rx.recv().await.unwrap().unwrap();

    assert_eq!("first_message", &first_pool_message.message);
    assert_eq!("ws://127.0.0.1/", &first_pool_message.host);
    assert_eq!("second_message", &second_pool_message.message);
    assert_eq!("ws://127.0.0.2/", &second_pool_message.host);
    assert_eq!("third_message", &third_pool_message.message);
    assert_eq!("ws://127.0.0.3/", &third_pool_message.host);
}

#[tokio::test]
async fn test_connection_pool_send_and_receive_messages() {
    // Given
    let buffer_size = 5;
    let host = url::Url::parse("ws://127.0.0.1").unwrap();
    let mut items = Vec::new();
    items.push(Message::text("recv_baz"));
    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream { items };
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let producer = TestConnectionProducer {
        write_stream,
        read_stream,
    };
    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, producer);

    let (tx, rx) = oneshot::channel();
    connection_pool.request_connection(host, tx).unwrap();
    let mut connection_handler = rx.await.unwrap();
    // When
    connection_handler.send_message("send_bar").await.unwrap();
    // Then
    let pool_message = router_rx.recv().await.unwrap().unwrap();

    assert_eq!("recv_baz", &pool_message.message);

    assert_eq!("ws://127.0.0.1/", &pool_message.host);

    assert_eq!(
        "send_bar",
        writer_rx.recv().await.unwrap().to_text().unwrap()
    );
}

#[tokio::test]
async fn test_connection_receive_single_messages() {
    // Given
    let buffer_size = 5;
    let host = String::from("ws://127.0.0.1");
    let mut items = Vec::new();
    items.push(Message::text("foo"));
    let read_stream = TestReadStream { items };
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    // When
    SwimConnection::receive_messages(pool_tx, read_stream, host)
        .await
        .unwrap();
    // Then
    let pool_message = pool_rx.recv().await.unwrap().unwrap();
    assert_eq!("foo", &pool_message.message);
    assert_eq!("ws://127.0.0.1", &pool_message.host);
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
    let read_stream = TestReadStream { items };
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    // When
    SwimConnection::receive_messages(pool_tx, read_stream, host)
        .await
        .unwrap();
    // Then

    let first_message = pool_rx.recv().await.unwrap().unwrap();
    let second_message = pool_rx.recv().await.unwrap().unwrap();
    let third_message = pool_rx.recv().await.unwrap().unwrap();

    assert_eq!("foo", &first_message.message);
    assert_eq!("ws://127.0.0.1", &first_message.host);

    assert_eq!("bar", &second_message.message);
    assert_eq!("ws://127.0.0.1", &first_message.host);

    assert_eq!("baz", &third_message.message);
    assert_eq!("ws://127.0.0.1", &first_message.host);
}

#[tokio::test]
async fn test_connection_send_single_message() {
    // Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream { items: Vec::new() };
    let mut connection = SwimConnection::new(
        host,
        buffer_size,
        _pool_tx,
        TestWebsocketProducer {
            write_stream,
            read_stream,
        },
    )
    .await
    .unwrap();
    let mut connection_handler = connection.get_handler();
    // When
    connection_handler.send_message("foo").await.unwrap();
    // Then
    assert_eq!("foo", writer_rx.recv().await.unwrap().to_text().unwrap());
}

#[tokio::test]
async fn test_connection_send_multiple_messages() {
    // Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream { items: Vec::new() };
    let mut connection = SwimConnection::new(
        host,
        buffer_size,
        _pool_tx,
        TestWebsocketProducer {
            write_stream,
            read_stream,
        },
    )
    .await
    .unwrap();
    let mut connection_handler = connection.get_handler();
    // When
    connection_handler.send_message("foo").await.unwrap();
    connection_handler.send_message("bar").await.unwrap();
    connection_handler.send_message("baz").await.unwrap();
    // Then
    assert_eq!("foo", writer_rx.recv().await.unwrap().to_text().unwrap());
    assert_eq!("bar", writer_rx.recv().await.unwrap().to_text().unwrap());
    assert_eq!("baz", writer_rx.recv().await.unwrap().to_text().unwrap());
}

#[tokio::test]
async fn test_connection_send_and_receive_messages() {
    // Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let mut items = Vec::new();
    items.push(Message::text("message_received"));
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: false,
    };
    let read_stream = TestReadStream { items };
    let mut connection = SwimConnection::new(
        host,
        buffer_size,
        pool_tx,
        TestWebsocketProducer {
            write_stream,
            read_stream,
        },
    )
    .await
    .unwrap();
    let mut connection_handler = connection.get_handler();
    // When
    connection_handler
        .send_message("message_sent")
        .await
        .unwrap();
    // Then
    let pool_message = pool_rx.recv().await.unwrap().unwrap();
    assert_eq!("message_received", &pool_message.message);
    assert_eq!("ws://127.0.0.1:9999", &pool_message.host);

    assert_eq!(
        "message_sent",
        writer_rx.recv().await.unwrap().to_text().unwrap()
    );
}

#[tokio::test]
async fn test_connection_parse_error() {
    // Given
    let host = "foo";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    // When
    let result = SwimConnection::new(host, buffer_size, _pool_tx, SwimWebsocketProducer {}).await;
    // Then
    assert!(result.is_err());
    assert_eq!(Some(ConnectionError::ParseError), result.err());
}

#[tokio::test]
async fn test_connection_connect_error() {
    // Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    // When
    let result = SwimConnection::new(host, buffer_size, _pool_tx, SwimWebsocketProducer {}).await;
    // Then
    assert!(result.is_err());
    assert_eq!(Some(ConnectionError::ConnectError), result.err());
}

#[tokio::test]
async fn test_new_connection_send_message_error() {
    // Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);

    let (writer_tx, _writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream {
        tx: writer_tx,
        error: true,
    };
    let read_stream = TestReadStream { items: Vec::new() };
    let connection = SwimConnection::new(
        host,
        buffer_size,
        _pool_tx,
        TestWebsocketProducer {
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
    assert_eq!(Some(ConnectionError::SendMessageError), result.err());
}

struct TestConnectionProducer {
    write_stream: TestWriteStream,
    read_stream: TestReadStream,
}

#[async_trait]
impl ConnectionProducer for TestConnectionProducer {
    type ConnectionType = TestConnection;

    async fn create_connection(
        &mut self,
        host: &str,
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

struct TestMultipleConnectionProducer {
    write_streams: Vec<TestWriteStream>,
    read_streams: Vec<TestReadStream>,
}

#[async_trait]
impl ConnectionProducer for TestMultipleConnectionProducer {
    type ConnectionType = TestConnection;

    async fn create_connection(
        &mut self,
        host: &str,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
    ) -> Result<Self::ConnectionType, ConnectionError> {
        TestConnection::new(
            host,
            buffer_size,
            router_tx,
            self.write_streams.drain(0..1).next().unwrap(),
            self.read_streams.drain(0..1).next().unwrap(),
        )
    }
}

struct TestConnection {
    tx: mpsc::Sender<Message>,
    _send_handler: JoinHandle<Result<(), ConnectionError>>,
    _receive_handler: JoinHandle<Result<(), ConnectionError>>,
}

impl TestConnection {
    fn new(
        host: &str,
        buffer_size: usize,
        router_tx: mpsc::Sender<Result<ConnectionPoolMessage, ConnectionError>>,
        write_stream: TestWriteStream,
        read_stream: TestReadStream,
    ) -> Result<TestConnection, ConnectionError> {
        let (tx, rx) = mpsc::channel(buffer_size);

        let receive = TestConnection::receive_messages(router_tx, read_stream, host.to_owned());
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

struct TestWebsocketProducer {
    write_stream: TestWriteStream,
    read_stream: TestReadStream,
}

#[async_trait]
impl WebsocketProducer for TestWebsocketProducer {
    type WebsocketType = TestReadWriteStream;

    async fn connect(self, _url: url::Url) -> Result<Self::WebsocketType, ConnectionError> {
        Ok(TestReadWriteStream {
            write_stream: self.write_stream.clone(),
            read_stream: self.read_stream.clone(),
        })
    }
}

struct TestReadWriteStream {
    read_stream: TestReadStream,
    write_stream: TestWriteStream,
}

impl Stream for TestReadWriteStream {
    type Item = Result<Message, ConnectionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.read_stream).poll_next(cx)
    }
}

impl Sink<Message> for TestReadWriteStream {
    type Error = ();

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.write_stream).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.write_stream).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.write_stream).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.write_stream).poll_close(cx)
    }
}

#[derive(Clone)]
struct TestReadStream {
    items: Vec<Message>,
}

impl Stream for TestReadStream {
    type Item = Result<Message, ConnectionError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.items.is_empty() {
            Poll::Ready(None)
        } else {
            let message = self.items.drain(0..1).next();
            Poll::Ready(Some(message.ok_or(ConnectionError::SendMessageError)))
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
