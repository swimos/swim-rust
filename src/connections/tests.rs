use std::{thread, time};

use futures::task::{Context, Poll};
use futures::Sink;
use futures_util::stream::Stream;
use tokio::macros::support::Pin;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::connections::{
    Client, Connection, ConnectionError, ConnectionPool, ConnectionPoolMessage,
};

#[test]
fn test_new_connection_pool() {
    // Given
    let buffer_size = 5;
    // When
    let (connection_pool, mut _handle) = ConnectionPool::new(buffer_size);
    // Then
    assert_eq!(0, connection_pool.connections.len())
}

#[test]
fn test_connection_pool_send_message() {
    // Given
    let buffer_size = 5;
    let host = "127.0.0.1";
    let text = "Hello";
    let (mut connection_pool, mut handle) = ConnectionPool::new(buffer_size);
    // When
    handle.send_message("127.0.0.1", "Hello").unwrap();
    let request: ConnectionPoolMessage = connection_pool.rx.try_recv().unwrap();
    // Then
    assert_eq!(host, request.host);
    assert_eq!(text, request.message);
}

#[test]
fn test_new_connection() {
    // Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    // When
    let (connection, _handle) = Connection::new(host, buffer_size, _pool_tx).unwrap();
    // Then
    assert!(connection.url.host().is_some());
    assert_eq!(9999, connection.url.port().unwrap());
    assert_eq!("ws://127.0.0.1:9999/", connection.url.as_str());
}

#[tokio::test]
async fn test_connection_receive_single_messages() {
    // Given
    let buffer_size = 5;
    let mut items = Vec::new();
    items.push(Message::text("foo"));
    let read_stream = TestReadStream { items };
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    // When
    Connection::receive_messages(pool_tx, read_stream).await;
    // Then
    assert_eq!("foo", pool_rx.try_recv().unwrap().to_text().unwrap());
}

#[tokio::test]
async fn test_connection_receive_multiple_messages() {
    // Given
    let buffer_size = 5;
    let mut items = Vec::new();
    items.push(Message::text("foo"));
    items.push(Message::text("bar"));
    items.push(Message::text("baz"));
    let read_stream = TestReadStream { items };
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    // When
    Connection::receive_messages(pool_tx, read_stream).await;
    // Then
    assert_eq!("foo", pool_rx.try_recv().unwrap().to_text().unwrap());
    assert_eq!("bar", pool_rx.try_recv().unwrap().to_text().unwrap());
    assert_eq!("baz", pool_rx.try_recv().unwrap().to_text().unwrap());
}

#[tokio::test]
async fn test_connection_send_multiple_message() {}

#[test]
fn test_connection_parse_error() {
    // Given
    let host = "foo";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    // When
    let result = Connection::new(host, buffer_size, _pool_tx);
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
    let (connection, _handle) = Connection::new(host, buffer_size, _pool_tx).unwrap();
    // When
    let result = connection.open().await;
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
    let (connection, _handle) = Connection::new(host, buffer_size, _pool_tx).unwrap();
    let write_stream = TestErrorWriter {};
    // When
    let result = connection.send_message(write_stream).await;
    // Then
    assert!(result.is_err());
    assert_eq!(Some(ConnectionError::SendMessageError), result.err());
}

// Todo only for debugging. (Make sure to enable stdout)
#[test]
fn test_with_remote() {
    let client = Client::new().unwrap();
    let (connection_pool, mut handler) = ConnectionPool::new(5);
    connection_pool.open(&client);
    handler
        .send_message(
            "ws://127.0.0.1:9001",
            "@sync(node:\"/unit/foo\", lane:\"info\")",
        )
        .unwrap();
    thread::sleep(time::Duration::from_secs(2));
}

struct TestReadStream {
    items: Vec<Message>,
}

impl Stream for TestReadStream {
    type Item = Result<Message, tungstenite::error::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.items.is_empty() {
            Poll::Ready(None)
        } else {
            let message = self.items.drain(0..1).next();
            Poll::Ready(Some(
                message.ok_or(tungstenite::error::Error::ConnectionClosed),
            ))
        }
    }
}

struct TestErrorWriter {}

impl<T> Sink<T> for TestErrorWriter {
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(()))
    }

    fn start_send(self: Pin<&mut Self>, _item: T) -> Result<(), Self::Error> {
        Err(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(()))
    }
}
