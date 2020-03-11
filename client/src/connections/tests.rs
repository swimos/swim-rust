use std::{thread, time};

use crate::connections::{
    Connection, ConnectionError, ConnectionHandler, ConnectionPool, ConnectionPoolMessage,
    ConnectionProducer, SwimConnection, SwimConnectionProducer,
};
use async_trait::async_trait;
use futures::task::{Context, Poll};
use futures::{Sink, StreamExt};
use futures_util::stream::Stream;
use futures_util::TryStreamExt;
use tokio::macros::support::Pin;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

#[tokio::test]
async fn test_new_connection_pool() {
    // Given
    let buffer_size = 5;
    let (router_tx, _router_rx) = mpsc::channel(5);
    // When
    let _connection_pool = ConnectionPool::new(buffer_size, router_tx, SwimConnectionProducer {});
    // Then
    // assert_eq!(0, connection_pool.connections.len());
    // assert_eq!(buffer_size, connection_pool.buffer_size);
}

//
#[tokio::test(core_threads = 2)]
async fn test_connection_pool_send_and_receive_message() {
    // Given
    let buffer_size = 5;
    let host = "ws://127.0.0.1";
    let mut items = Vec::new();
    items.push(Message::text("recv_baz"));
    let (writer_tx, mut writer_rx) = mpsc::channel(5);
    let write_stream = TestWriteStream { tx: writer_tx };
    let read_stream = TestReadStream { items };
    let (router_tx, mut router_rx) = mpsc::channel(5);
    // When
    let producer = TestConnectionProducer {
        write_stream,
        read_stream,
    };
    let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, producer);

    // Then
    connection_pool.send_message(host, "send_bar").unwrap();

    assert_eq!(
        "recv_baz",
        router_rx.recv().await.unwrap().to_text().unwrap()
    );
    assert_eq!(
        "send_bar",
        writer_rx.recv().await.unwrap().to_text().unwrap()
    );
}

// #[tokio::test]
// async fn test_connection_pool_send_message() {
//     Given
// let buffer_size = 5;
// let host = "ws://127.0.0.1";
// let text = "Hello";
// let (router_tx, mut router_rx) = mpsc::channel(5);
//
// let (writer_tx, mut writer_rx) = mpsc::channel(5);
// let write_stream = TestWriteStream { tx: writer_tx };
// let read_stream = TestReadStream { items: Vec::new() };
//
// let producer = TestConnectionProducer {
//     write_stream,
//     read_stream,
// };
//
// let mut connection_pool =
//     ConnectionPool::new(buffer_size, router_tx, producer);

// When
// connection_pool.send_message("ws://127.0.0.1", "Hello").unwrap();
// let request = router_rx.recv().await;
// Then
// println!("{:?}", request);
// assert_eq!(host, request.host);
// assert_eq!(text, request.message);
// }

#[test]
fn test_new_connection() {
    // Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    // When
    let (connection, _handle) = SwimConnection::new(host, buffer_size, _pool_tx).unwrap();
    // Then
    assert!(connection.url.host().is_some());
    assert_eq!(9999, connection.url.port().unwrap());
    assert_eq!("ws://127.0.0.1:9999/", connection.url.as_str());
}

#[tokio::test]
async fn test_connection_start() {
    //Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    let mut items = Vec::new();
    items.push(Message::text("recv_foo"));
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let ws_stream = TestReadWriteStream {
        write_stream: TestWriteStream { tx: writer_tx },
        read_stream: TestReadStream { items },
    };
    let (connection, mut handle) = SwimConnection::new(host, buffer_size, pool_tx).unwrap();
    handle.send_message("send_foo").await.unwrap();
    //When
    let (send_handle, receive_handle) = connection.start(ws_stream);
    let _result = send_handle.await.unwrap();
    let _result = receive_handle.await.unwrap().unwrap();
    //Then
    assert_eq!("send_foo", writer_rx.try_recv().unwrap().to_text().unwrap());
    assert_eq!("recv_foo", pool_rx.try_recv().unwrap().to_text().unwrap());
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
    SwimConnection::receive_messages(pool_tx, read_stream)
        .await
        .unwrap();
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
    SwimConnection::receive_messages(pool_tx, read_stream)
        .await
        .unwrap();
    // Then
    assert_eq!("foo", pool_rx.try_recv().unwrap().to_text().unwrap());
    assert_eq!("bar", pool_rx.try_recv().unwrap().to_text().unwrap());
    assert_eq!("baz", pool_rx.try_recv().unwrap().to_text().unwrap());
}

#[tokio::test]
async fn test_connection_send_single_message() {
    // Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream { tx: writer_tx };
    let (connection, mut handle) = SwimConnection::new(host, buffer_size, _pool_tx).unwrap();
    // When
    handle.send_message("foo").await.unwrap();
    let _result = connection.send_message(write_stream).await;
    // Then
    assert_eq!("foo", writer_rx.try_recv().unwrap().to_text().unwrap());
}

#[tokio::test]
async fn test_connection_send_multiple_messages() {
    // Given
    let host = "ws://127.0.0.1:9999";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    let (writer_tx, mut writer_rx) = mpsc::channel(buffer_size);
    let write_stream = TestWriteStream { tx: writer_tx };
    let (connection, mut handle) = SwimConnection::new(host, buffer_size, _pool_tx).unwrap();
    // When
    handle.send_message("foo").await.unwrap();
    handle.send_message("bar").await.unwrap();
    handle.send_message("baz").await.unwrap();
    let _result = connection.send_message(write_stream).await;
    // Then
    assert_eq!("foo", writer_rx.try_recv().unwrap().to_text().unwrap());
    assert_eq!("bar", writer_rx.try_recv().unwrap().to_text().unwrap());
    assert_eq!("baz", writer_rx.try_recv().unwrap().to_text().unwrap());
}

#[test]
fn test_connection_parse_error() {
    // Given
    let host = "foo";
    let buffer_size = 5;
    let (_pool_tx, _pool_rx) = mpsc::channel(buffer_size);
    // When
    let result = SwimConnection::new(host, buffer_size, _pool_tx);
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
    let (connection, _handle) = SwimConnection::new(host, buffer_size, _pool_tx).unwrap();
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
    let (connection, _handle) = SwimConnection::new(host, buffer_size, _pool_tx).unwrap();
    let write_stream = TestErrorWriter {};
    // When
    let result = connection.send_message(write_stream).await;
    // Then
    assert!(result.is_err());
    assert_eq!(Some(ConnectionError::SendMessageError), result.err());
}

// Todo only for debugging. (Make sure to enable stdout)
#[tokio::test(core_threads = 2)]
async fn test_with_remote() {
    let (router_tx, _router_rx) = mpsc::channel(5);

    let mut connection_pool =
        ConnectionPool::new(5, router_tx, SwimConnectionProducer {});

    connection_pool
        .send_message(
            "ws://127.0.0.1:9001",
            "@sync(node:\"/unit/foo\", lane:\"info\")",
        )
        .unwrap();

    thread::sleep(time::Duration::from_secs(2));
}

struct TestConnectionProducer {
    write_stream: TestWriteStream,
    read_stream: TestReadStream,
}

impl ConnectionProducer for TestConnectionProducer {
    type T = TestConnection;

    fn create_connection(
        &self,
        host: &str,
        buffer_size: usize,
        pool_tx: mpsc::Sender<Message>,
    ) -> Result<(Self::T, ConnectionHandler), ConnectionError> {
        TestConnection::new(
            host,
            buffer_size,
            pool_tx,
            self.write_stream.clone(),
            self.read_stream.clone(),
        )
    }
}

struct TestConnection {
    _url: url::Url,
    rx: mpsc::Receiver<Message>,
    pool_tx: mpsc::Sender<Message>,
    write_stream: TestWriteStream,
    read_stream: TestReadStream,
}

impl TestConnection {
    fn new(
        host: &str,
        buffer_size: usize,
        pool_tx: mpsc::Sender<Message>,
        write_stream: TestWriteStream,
        read_stream: TestReadStream,
    ) -> Result<(TestConnection, ConnectionHandler), ConnectionError> {
        let url = url::Url::parse(&host)?;
        let (tx, rx) = mpsc::channel(buffer_size);

        Ok((
            TestConnection {
                _url: url,
                rx,
                pool_tx,
                write_stream,
                read_stream,
            },
            ConnectionHandler { tx },
        ))
    }
}

#[async_trait]
impl Connection for TestConnection {
    async fn open(
        self,
    ) -> Result<
        (
            JoinHandle<Result<(), ConnectionError>>,
            JoinHandle<Result<(), ConnectionError>>,
        ),
        ConnectionError,
    > {
        let ws_stream = TestReadWriteStream {
            write_stream: self.write_stream.clone(),
            read_stream: self.read_stream.clone(),
        };

        let ws_stream = ws_stream.map_err(|_| ConnectionError::SendMessageError);
        Ok(self.start(ws_stream))
    }

    fn start<S>(
        self,
        ws_stream: S,
    ) -> (
        JoinHandle<Result<(), ConnectionError>>,
        JoinHandle<Result<(), ConnectionError>>,
    )
        where
            S: Stream<Item=Result<Message, ConnectionError>> + Sink<Message> + Send + 'static,
    {
        let (write_stream, read_stream) = ws_stream.split();

        let receive = TestConnection::receive_messages(self.pool_tx.clone(), read_stream);
        let send = self.send_message(write_stream);

        let send_handle = tokio::spawn(send);
        let receive_handle = tokio::spawn(receive);

        (send_handle, receive_handle)
    }

    async fn send_message<S>(self, write_stream: S) -> Result<(), ConnectionError>
        where
            S: Sink<Message> + Send + 'static,
    {
        self.rx
            .map(Ok)
            .forward(write_stream)
            .await
            .map_err(|_| ConnectionError::SendMessageError)?;
        Ok(())
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
}

impl Sink<Message> for TestWriteStream {
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.tx.try_send(item).unwrap();
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
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
