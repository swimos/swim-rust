use crate::connections::{
    receive_messages, Connection, ConnectionError, ConnectionPool, ConnectionProducer,
    SwimConnection, SwimWebsocketProducer, WebsocketProducer,
};
use async_trait::async_trait;
use futures::task::{Context, Poll};
use futures::Sink;
use futures_util::stream::Stream;
use tokio::macros::support::Pin;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

#[tokio::test]
async fn test_connection_pool_send_and_receive_messages() {
    // Given
    let buffer_size = 5;
    let host = "ws://127.0.0.1";
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

    // When
    connection_pool.send_message(host, "send_bar").unwrap();

    // Then
    assert_eq!(
        "recv_baz",
        router_rx.recv().await.unwrap().unwrap().to_text().unwrap()
    );
    assert_eq!(
        "send_bar",
        writer_rx.recv().await.unwrap().to_text().unwrap()
    );
}

// #[tokio::test]
// async fn test_connection_pool_send_message() {
//     // Given
//     let buffer_size = 5;
//     let host = "ws://127.0.0.1";
//     let text = "Hello";
//     let (router_tx, mut router_rx) = mpsc::channel(5);
//
//     let (writer_tx, mut writer_rx) = mpsc::channel(5);
//     let write_stream = TestWriteStream {
//         tx: writer_tx,
//         error: false,
//     };
//     let read_stream = TestReadStream { items: Vec::new() };
//
//     let producer = TestConnectionProducer {
//         write_stream,
//         read_stream,
//     };
//
//     let mut connection_pool = ConnectionPool::new(buffer_size, router_tx, producer);
//
//     // When
//     connection_pool
//         .send_message("ws://127.0.0.1", "Hello")
//         .unwrap();
//     let request = router_rx.recv().await;
//     // Then
//     println!("{:?}", request);
//     assert_eq!(host, request.host);
//     assert_eq!(text, request.message);
// }

#[tokio::test]
async fn test_connection_receive_single_messages() {
    // Given
    let buffer_size = 5;
    let mut items = Vec::new();
    items.push(Message::text("foo"));
    let read_stream = TestReadStream { items };
    let (pool_tx, mut pool_rx) = mpsc::channel(buffer_size);
    // When
    receive_messages(pool_tx, read_stream).await.unwrap();
    // Then
    assert_eq!(
        "foo",
        pool_rx.try_recv().unwrap().unwrap().to_text().unwrap()
    );
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
    receive_messages(pool_tx, read_stream).await.unwrap();
    // Then
    assert_eq!(
        "foo",
        pool_rx.try_recv().unwrap().unwrap().to_text().unwrap()
    );
    assert_eq!(
        "bar",
        pool_rx.try_recv().unwrap().unwrap().to_text().unwrap()
    );
    assert_eq!(
        "baz",
        pool_rx.try_recv().unwrap().unwrap().to_text().unwrap()
    );
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
    // When
    connection.send_message("foo").await.unwrap();
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
    // When
    connection.send_message("foo").await.unwrap();
    connection.send_message("bar").await.unwrap();
    connection.send_message("baz").await.unwrap();
    // Then
    assert_eq!("foo", writer_rx.recv().await.unwrap().to_text().unwrap());
    assert_eq!("bar", writer_rx.recv().await.unwrap().to_text().unwrap());
    assert_eq!("baz", writer_rx.recv().await.unwrap().to_text().unwrap());
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
    let result = connection.send_handler.await.unwrap();
    // Then
    assert!(result.is_err());
    assert_eq!(Some(ConnectionError::SendMessageError), result.err());
}

// // // Todo only for debugging. (Make sure to enable stdout)
// use crate::connections::SwimConnectionProducer;
// use std::{thread, time};
//
// #[tokio::test(core_threads = 2)]
// async fn test_with_remote() {
//     let (router_tx, _router_rx) = mpsc::channel(5);
//
//     let mut connection_pool = ConnectionPool::new(5, router_tx, SwimConnectionProducer {});
//
//     connection_pool
//         .send_message(
//             "ws://127.0.0.1:9001",
//             "@sync(node:\"/unit/foo\", lane:\"info\")",
//         )
//         .unwrap();
//
//     thread::sleep(time::Duration::from_secs(2));
// }

struct TestConnectionProducer {
    write_stream: TestWriteStream,
    read_stream: TestReadStream,
}

#[async_trait]
impl ConnectionProducer for TestConnectionProducer {
    type T = TestConnection;

    async fn create_connection(
        &self,
        _host: &str,
        buffer_size: usize,
        pool_tx: mpsc::Sender<Result<Message, ConnectionError>>,
    ) -> Result<Self::T, ConnectionError> {
        TestConnection::new(
            buffer_size,
            pool_tx,
            self.write_stream.clone(),
            self.read_stream.clone(),
        )
    }
}

struct TestConnection {
    tx: mpsc::Sender<Message>,
    send_handler: JoinHandle<Result<(), ConnectionError>>,
    receive_handler: JoinHandle<Result<(), ConnectionError>>,
}

impl TestConnection {
    fn new(
        buffer_size: usize,
        pool_tx: mpsc::Sender<Result<Message, ConnectionError>>,
        write_stream: TestWriteStream,
        read_stream: TestReadStream,
    ) -> Result<TestConnection, ConnectionError> {
        let (tx, rx) = mpsc::channel(buffer_size);

        let receive = receive_messages(pool_tx, read_stream);
        let send = TestConnection::send_messages(write_stream, rx);

        let send_handler = tokio::spawn(send);
        let receive_handler = tokio::spawn(receive);

        Ok(TestConnection {
            tx,
            send_handler,
            receive_handler,
        })
    }
}

#[async_trait]
impl Connection for TestConnection {
    //noinspection ALL
    async fn send_message(&mut self, message: &str) -> Result<(), ConnectionError> {
        self.tx.send(Message::text(message)).await?;
        Ok(())
    }
}

struct TestWebsocketProducer {
    write_stream: TestWriteStream,
    read_stream: TestReadStream,
}

#[async_trait]
impl WebsocketProducer for TestWebsocketProducer {
    type T = TestReadWriteStream;

    async fn connect(self, _url: url::Url) -> Result<Self::T, ConnectionError> {
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
        Poll::Ready(Err(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.error {
            Poll::Ready(Err(()))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
